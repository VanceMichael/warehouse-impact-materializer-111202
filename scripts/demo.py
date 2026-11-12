import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from warehouse.catalog import connect, load_bundle
from warehouse.engine import compare, full_results, run
from warehouse.plan import ensure_tables, load_plan, shift_day
from warehouse.store import ObjectStore


def execute(*arguments):
    command = [sys.executable, '-m', 'warehouse', *arguments]
    print('$ ' + ' '.join(command), flush=True)
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=True)
    print(result.stdout.strip(), flush=True)
    return json.loads(result.stdout)


def main():
    with tempfile.TemporaryDirectory() as temporary:
        database = str(Path(temporary) / 'warehouse.sqlite3')
        objects = str(Path(temporary) / 'objects')
        imported = execute('import', '--db', database, '--input', str(ROOT / 'examples/source.json'), '--batch', 'daily')
        if not imported['changed']:
            raise RuntimeError('首次导入没有写入')
        repeated = execute('import', '--db', database, '--input', str(ROOT / 'examples/source.json'), '--batch', 'daily')
        if repeated['changed']:
            raise RuntimeError('重复导入发生额外写入')
        calculated = execute('inspect', '--db', database)
        exported = execute('export', '--db', database, '--objects', objects)
        restored = ObjectStore(objects).read(exported['object'])
        expected = calculated if isinstance(calculated, list) else [calculated]
        if restored != expected:
            raise RuntimeError('导出与查询结果不符')

        plan_path = str(ROOT / 'examples/plans/demand.toml')
        explained = execute('plan', '--db', database, '--plan', plan_path)
        if [node['node'] for node in explained['nodes']] != ['daily', 'rolling_7d', 'weekly']:
            raise RuntimeError('计划解释缺少节点')
        previewed = execute('preview', '--db', database, '--plan', plan_path)
        if previewed['applied']:
            raise RuntimeError('试算不应写入')
        applied = execute('apply', '--db', database, '--plan', plan_path)
        if not applied['applied']:
            raise RuntimeError('首次物化没有发布')

        connection = connect(database)
        try:
            plan = load_plan(plan_path)
            ensure_tables(plan, connection)
            _scenarios(connection, plan)
        finally:
            connection.close()
        print('对象校验通过，重复导入未改变业务结果；增量结果等于全量结果', flush=True)


def _scenarios(connection, plan):
    # 跨周变化：补一天记录，只应影响它自己、七天滚动窗口与所属周。
    load_bundle(connection, {'demand': [
        {'record': 'd3', 'day': '2026-10-03', 'item': 's1', 'units': 5},
    ]}, 'extra')
    preview = run(plan, connection, apply=False)
    nodes = {node.node.name: node for node in preview.nodes}
    if set(nodes['daily'].dirty) != {'2026-10-03'}:
        raise RuntimeError('新增一天应只弄脏该日分区')
    needed = {
        shift_day(label, -step)
        for label in nodes['rolling_7d'].dirty
        for step in range(7)
    }
    if not set(nodes['rolling_7d'].scanned) <= needed:
        raise RuntimeError(f'扫到了无关分区：{sorted(set(nodes["rolling_7d"].scanned) - needed)}')
    run(plan, connection)
    _assert_matches_full(connection, plan, '补一天')

    # 删除：分区与下游一并移除。
    connection.execute("DELETE FROM demand WHERE record='d1'")
    preview = run(plan, connection, apply=False)
    nodes = {node.node.name: node for node in preview.nodes}
    if nodes['daily'].dirty['2026-10-01'].action != 'delete':
        raise RuntimeError('删除应产生 delete 动作')
    run(plan, connection)
    _assert_matches_full(connection, plan, '删除')

    # 规则改变：所有分区失效并向下游传递规则原因。
    text = (ROOT / 'examples/plans/demand.toml').read_text(encoding='utf-8').replace(
        'rule = { op = "sum", value = "units", output = "demand_units" }',
        'rule = { op = "max", value = "units", output = "demand_units" }',
    )
    temporary = Path(tempfile.mkdtemp()) / 'changed.toml'
    temporary.write_text(text, encoding='utf-8')
    changed = load_plan(temporary)
    preview = run(changed, connection, apply=False)
    nodes = {node.node.name: node for node in preview.nodes}
    if not nodes['daily'].rule_changed:
        raise RuntimeError('规则改变未被识别')
    if 'rule' not in nodes['weekly'].summary():
        raise RuntimeError('规则原因未传递到下游')
    run(changed, connection)
    _assert_matches_full(connection, changed, '规则改变')

    # 中断：发布前失败，正式表保持旧版本；恢复后只复用仍匹配的暂存。
    connection.execute("UPDATE demand SET units=11 WHERE record='d2'")
    before = [dict(row) for row in connection.execute('SELECT * FROM rolling_7d ORDER BY day')]
    def interrupt(_):
        raise RuntimeError('模拟提交前中断')
    try:
        run(changed, connection, publish_hook=interrupt)
    except RuntimeError:
        pass
    after = [dict(row) for row in connection.execute('SELECT * FROM rolling_7d ORDER BY day')]
    if before != after:
        raise RuntimeError('中断后正式表被部分更新')
    recovered = run(changed, connection, recover=True)
    if recovered.nodes[0].reuse_staged < 1:
        raise RuntimeError('恢复没有复用仍匹配的暂存')
    _assert_matches_full(connection, changed, '中断恢复')


def _assert_matches_full(connection, plan, label):
    verdict = compare(plan, connection, full_results(plan, connection))
    if not verdict['equal']:
        raise RuntimeError(f'{label}后增量结果与全量不一致：{verdict}')


if __name__ == '__main__':
    main()
