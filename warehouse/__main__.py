import argparse
import json
import sqlite3

from .catalog import connect, load_file
from .domain import overview
from .engine import compare, full_results, run
from .plan import PlanError, ensure_tables, load_plan, topological_order, verify
from .store import ObjectStore


def emit(value):
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False))


def main():
    parser = argparse.ArgumentParser(description='数据集合装载、查询与声明式增量物化')
    parser.add_argument('command', choices=['import', 'inspect', 'export', 'plan', 'preview', 'apply', 'recover', 'verify'])
    parser.add_argument('--db', required=True)
    parser.add_argument('--input')
    parser.add_argument('--batch')
    parser.add_argument('--objects')
    parser.add_argument('--plan')
    args = parser.parse_args()
    if args.command == 'import' and (not args.input or not args.batch):
        parser.error('导入需要 --input 和 --batch')
    if args.command == 'export' and not args.objects:
        parser.error('导出需要 --objects')
    if args.command in ('plan', 'preview', 'apply', 'recover', 'verify') and not args.plan:
        parser.error(f'{args.command} 需要 --plan')
    from pathlib import Path
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    connection = connect(args.db)
    try:
        if args.command == 'import':
            emit({'changed': load_file(connection, args.input, args.batch)})
            return
        if args.command in ('inspect', 'export'):
            result = overview(connection)
            if args.command == 'export':
                records = result if isinstance(result, list) else [result]
                emit({'object': ObjectStore(args.objects).put(records), 'rows': len(records)})
            else:
                emit(result)
            return
        plan = load_plan(args.plan)
        if args.command == 'plan':
            emit(explain(plan, connection))
            return
        if args.command == 'recover':
            emit(run(plan, connection, apply=True, recover=True).to_dict(preview=True))
            return
        if args.command == 'apply':
            emit(run(plan, connection, apply=True).to_dict(preview=True))
            return
        if args.command == 'preview':
            emit(run(plan, connection, apply=False).to_dict(preview=True))
            return
        full = full_results(plan, connection)
        emit(compare(plan, connection, full))
    finally:
        connection.close()


def explain(plan, connection):
    """只读解释：核验声明与现有实际表，但不创建任何表。"""
    ensure_tables(plan, connection, create=False)
    verified = verify(plan, connection, strict=False)
    nodes = []
    for node in topological_order(plan):
        info = verified[node.name]
        nodes.append({
            'node': node.name,
            'table': node.table,
            'source': node.source,
            'depends_on': list(node.depends_on),
            'partition_by': node.partition_by,
            'unique_key': list(node.unique_key),
            'mapping': node.mapping.to_dict(),
            'rule': node.rule.to_dict(),
            'rule_digest': node.rule_digest(),
            'input_table': info['input_table'],
            'output_columns': sorted(info['output_columns']),
        })
    return {'version': plan.version, 'nodes': nodes}


if __name__ == '__main__':
    try:
        main()
    except (PlanError, ValueError) as error:
        raise SystemExit(f'计划错误：{error}')
    except sqlite3.Error as error:
        raise SystemExit(f'数据库错误：{error}')
