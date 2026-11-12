"""增量物化引擎：脏分区传播、固定输入快照、分区级暂存、恢复与原子发布。

状态模型：

- ``node_partitions``：已发布分区及其输入摘要、规则摘要、输出摘要和行数。
- ``source_digests``：上一次成功发布时根节点来源分区的内容摘要，用于判断
  内容变化、删除与维度回溯。
- ``stage_partitions``：已暂存但未发布的分区；暂存行位于 ``stage_<table>``。
  中断后重跑会逐分区核验暂存的输入摘要与规则摘要，未变的直接复用。
- ``node_state``：节点当前规则指纹。规则变化使该节点全部分区重算，并沿
  依赖边传递。

发布在单个 SQLite 事务内换入全部分区的暂存结果并更新状态，查询方要么读到
完整的旧版本，要么读到完整的新版本，不会看到半新半旧的滚动窗口。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from .catalog import transaction
from .plan import (
    EMPTY_DIGEST,
    ConflictError,
    NodeSpec,
    Plan,
    PlanError,
    actual_columns,
    ensure_tables,
    fingerprint,
    forward_labels,
    input_labels,
    normalize_label,
    topological_order,
    verify,
)


# --------------------------------------------------------------------------- 原因与结果

@dataclass(frozen=True)
class Cause:
    """分区变脏的直接原因。

    - ``source_content``：根节点来源分区内容变化（新增、更新、删除、回溯）。
    - ``rule``：本节点规则指纹变化。
    - ``upstream_content``：直接上游输出分区内容变化。
    - ``upstream_rule``：直接上游规则指纹变化。
    - ``published_mismatch``：已发布记录与实际表内容校验不符，产物不能复用。

    ``root`` 把上述原因归并为维护者视角的三类：``source``（来源本身变了）、
    ``rule``（依赖规则变了）、``artifact``（已发布产物损坏）。
    """

    kind: str
    node: str
    partitions: tuple[str, ...] = ()

    def key(self) -> tuple:
        return (self.kind, self.node, self.partitions)

    def to_dict(self) -> dict:
        value = {'kind': self.kind, 'node': self.node, 'root': self.root}
        if self.partitions:
            value['partitions'] = list(self.partitions)
        return value

    @property
    def root(self) -> str:
        """维护者视角的根因分类：来源变化、规则变化或产物损坏。"""
        if self.kind in ('source_content', 'upstream_content'):
            return 'source'
        if self.kind in ('rule', 'upstream_rule'):
            return 'rule'
        return 'artifact'

    def relevant(self, labels: set[str]) -> bool:
        if not self.partitions:
            return True
        return bool(set(self.partitions) & labels)


@dataclass
class Dirty:
    partition: str
    action: str
    causes: list[Cause] = field(default_factory=list)
    propagated: list[Cause] = field(default_factory=list)

    def merge_cause(self, cause: Cause) -> None:
        for index, existing in enumerate(self.causes):
            if existing.kind == cause.kind and existing.node == cause.node:
                merged = Cause(
                    existing.kind,
                    existing.node,
                    tuple(sorted(set(existing.partitions) | set(cause.partitions))),
                )
                self.causes[index] = merged
                return
        self.causes.append(cause)

    def merge_propagated(self, causes: Iterable[Cause]) -> None:
        for cause in causes:
            if all(existing.key() != cause.key() for existing in self.propagated):
                self.propagated.append(cause)

    def to_dict(self) -> dict:
        return {
            'partition': self.partition,
            'action': self.action,
            'causes': [cause.to_dict() for cause in sorted(self.causes, key=lambda item: item.key())],
            'propagated': [cause.to_dict() for cause in sorted(self.propagated, key=lambda item: item.key())],
        }


@dataclass(frozen=True)
class Entry:
    node: str
    partition: str
    input_digest: str
    rule_digest: str
    output_digest: str
    rows: int
    staged: bool


@dataclass
class Result:
    partition: str
    action: str
    rows: list[dict] | None
    digest: str
    causes: list[Cause]
    propagated: list[Cause]
    input_digest: str = EMPTY_DIGEST
    rule_digest: str = EMPTY_DIGEST

    def to_dict(self, preview: bool) -> dict:
        value = {
            'partition': self.partition,
            'action': self.action,
            'digest': self.digest,
        }
        if preview:
            value['rows'] = len(self.rows or [])
        return value


@dataclass
class NodeReport:
    node: NodeSpec
    rule_digest: str
    rule_changed: bool
    dirty: dict[str, Dirty] = field(default_factory=dict)
    results: dict[str, Result] = field(default_factory=dict)
    checked: set[str] = field(default_factory=set)
    scanned: set[str] = field(default_factory=set)
    verified: int = 0
    reuse_published: int = 0
    reuse_staged: int = 0
    recalculated: int = 0
    deleted: int = 0

    def summary(self) -> dict:
        reasons: dict[str, set[str]] = {}
        for dirty in self.dirty.values():
            for cause in dirty.causes:
                reasons.setdefault(cause.root, set()).add(cause.kind)
            for cause in dirty.propagated:
                reasons.setdefault('propagated', set()).add(cause.kind)
        return {root: sorted(kinds) for root, kinds in sorted(reasons.items())}

    def to_dict(self, preview: bool) -> dict:
        value = {
            'node': self.node.name,
            'table': self.node.table,
            'partition_by': self.node.partition_by,
            'rule_digest': self.rule_digest,
            'rule_changed': self.rule_changed,
            'partitions': [self.dirty[key].to_dict() for key in sorted(self.dirty)],
            'checked': sorted(self.checked),
            'scanned': sorted(self.scanned),
            'verified': self.verified,
            'reasons': self.summary(),
            'reuse': {'published': self.reuse_published, 'staged': self.reuse_staged},
            'actions': {'recalculated': self.recalculated, 'deleted': self.deleted},
        }
        if preview:
            value['results'] = [self.results[key].to_dict(True) for key in sorted(self.results)]
        return value


@dataclass
class Report:
    nodes: list[NodeReport] = field(default_factory=list)
    applied: bool = False
    snapshot: str = EMPTY_DIGEST
    snapshot_size: int = 0

    def by_name(self) -> dict[str, NodeReport]:
        return {report.node.name: report for report in self.nodes}

    def to_dict(self, preview: bool = False) -> dict:
        return {
            'applied': self.applied,
            'snapshot': self.snapshot,
            'inputs': self.snapshot_size,
            'nodes': [report.to_dict(preview) for report in self.nodes],
        }


# --------------------------------------------------------------------------- 状态表

def init_state(connection) -> None:
    connection.execute(
        'CREATE TABLE IF NOT EXISTS node_partitions (node TEXT NOT NULL, partition TEXT NOT NULL, '
        'input_digest TEXT NOT NULL, rule_digest TEXT NOT NULL, output_digest TEXT NOT NULL, '
        'rows INTEGER NOT NULL, PRIMARY KEY (node, partition))'
    )
    connection.execute(
        'CREATE TABLE IF NOT EXISTS source_digests (node TEXT NOT NULL, partition TEXT NOT NULL, '
        'digest TEXT NOT NULL, PRIMARY KEY (node, partition))'
    )
    connection.execute(
        'CREATE TABLE IF NOT EXISTS stage_partitions (node TEXT NOT NULL, partition TEXT NOT NULL, '
        'input_digest TEXT NOT NULL, rule_digest TEXT NOT NULL, output_digest TEXT NOT NULL, '
        'rows INTEGER NOT NULL, PRIMARY KEY (node, partition))'
    )
    connection.execute('CREATE TABLE IF NOT EXISTS node_state (node TEXT PRIMARY KEY, rule_digest TEXT NOT NULL)')
    connection.execute(
        'CREATE TABLE IF NOT EXISTS applied_snapshots (snapshot TEXT PRIMARY KEY, inputs INTEGER NOT NULL)'
    )


def staged_table(table: str) -> str:
    return 'stage_' + table


def stored_entries(connection, node: NodeSpec, staged: bool) -> dict[str, Entry]:
    table = 'stage_partitions' if staged else 'node_partitions'
    entries: dict[str, Entry] = {}
    for row in connection.execute(
        f'SELECT partition, input_digest, rule_digest, output_digest, rows FROM "{table}" WHERE node=?',
        (node.name,),
    ):
        entries[row['partition']] = Entry(
            node.name, row['partition'], row['input_digest'], row['rule_digest'], row['output_digest'], row['rows'], staged
        )
    return entries


def previous_source_digests(connection, node: NodeSpec) -> dict[str, str]:
    return {
        row['partition']: row['digest']
        for row in connection.execute('SELECT partition, digest FROM source_digests WHERE node=?', (node.name,))
    }


def row_digest(rows: Iterable[dict]) -> str:
    rows = list(rows)
    return fingerprint(rows) if rows else EMPTY_DIGEST


def select_rows(connection, table: str, partition_column: str, partition: str, columns: Iterable[str]) -> list[dict]:
    names = ', '.join(f'"{column}"' for column in sorted(columns))
    cursor = connection.execute(
        f'SELECT {names} FROM "{table}" WHERE "{partition_column}"=? ORDER BY rowid', (partition,)
    )
    return [{column: row[column] for column in sorted(columns)} for row in cursor]


def table_exists(connection, table: str) -> bool:
    return actual_columns(connection, table) is not None


def current_labels(connection, node: NodeSpec) -> set[str]:
    return {
        normalize_label(node.partition_by, row[0])
        for row in connection.execute(f'SELECT DISTINCT "{node.partition_column}" FROM "{node.table}"')
    }


def combine_digests(digests: dict[str, str]) -> str:
    return fingerprint({label: digests[label] for label in sorted(digests)})


# --------------------------------------------------------------------------- 快照

@dataclass
class Snapshot:
    """一次执行固定的来源视图：根节点每个输入分区的行与摘要。"""

    rows: dict[str, dict[str, list[dict]]]
    digests: dict[str, dict[str, str]]

    def digest(self) -> str:
        return fingerprint({name: sorted(parts.items()) for name, parts in self.digests.items()})

    def size(self) -> int:
        return sum(len(parts) for parts in self.digests.values())


def take_snapshot(connection, plan: Plan, verified: dict[str, dict]) -> Snapshot:
    """读取并固定根节点来源表的内容；只读取计划声明引用的列。

    未声明的表与列不会出现在快照里；声明与实际表不一致已在 ``verify`` 拒绝。
    """
    rows: dict[str, dict[str, list[dict]]] = {}
    digests: dict[str, dict[str, str]] = {}
    for node in plan.nodes:
        if node.source is None:
            continue
        info = verified[node.name]
        column = node.mapping.source_partition
        columns = node.mapping.source_columns()
        rows[node.name] = {}
        digests[node.name] = {}
        cursor = connection.execute(f'SELECT DISTINCT "{column}" FROM "{info["input_table"]}" ORDER BY "{column}"')
        for (label,) in cursor:
            normalized = normalize_label(node.partition_by, label)
            selected = select_rows(connection, info['input_table'], column, normalized, columns)
            rows[node.name][normalized] = selected
            digests[node.name][normalized] = row_digest(selected)
    return Snapshot(rows, digests)


def changed_source_labels(connection, node: NodeSpec, snapshot: Snapshot) -> set[str]:
    previous = previous_source_digests(connection, node)
    current = snapshot.digests.get(node.name, {})
    return {label for label in set(current) | set(previous) if current.get(label) != previous.get(label)}


# --------------------------------------------------------------------------- 计算

def compute(node: NodeSpec, inputs: dict[str, list[dict]], target: str) -> list[dict]:
    """按节点声明把输入分区的行映射为某个输出分区的行。"""
    mapping = node.mapping
    rule = node.rule
    if rule.op == 'project':
        rows = []
        for label in sorted(inputs):
            for input_row in inputs[label]:
                if input_row[mapping.source_partition] != label:
                    raise ConflictError(f'节点 {node.name} 的输入行不属于分区 {label}')
                row = {}
                for source, destination in zip(mapping.source, mapping.target):
                    row[destination.column] = input_row[source.column]
                row[mapping.target_partition] = target
                rows.append(row)
    else:
        source_keys = [column for column in mapping.source_keys() if column != rule.value]
        target_keys = list(mapping.target_keys())
        if len(source_keys) != len(target_keys):
            raise PlanError(f'节点 {node.name} 的聚合键列数不一致')
        totals: dict[tuple, Any] = {}
        for label in sorted(inputs):
            for input_row in inputs[label]:
                if input_row[mapping.source_partition] != label:
                    raise ConflictError(f'节点 {node.name} 的输入行不属于分区 {label}')
                key = tuple(input_row[column] for column in source_keys)
                value = input_row[rule.value]
                if value is None:
                    raise ConflictError(f'节点 {node.name} 的聚合列 {rule.value} 不应为空')
                if rule.op == 'count':
                    totals[key] = totals.get(key, 0) + 1
                elif rule.op == 'sum':
                    totals[key] = totals.get(key, 0) + value
                elif rule.op == 'min':
                    totals[key] = value if key not in totals else min(totals[key], value)
                else:
                    totals[key] = value if key not in totals else max(totals[key], value)
        rows = []
        for key, value in totals.items():
            row = {target_column: key[index] for index, target_column in enumerate(target_keys)}
            row[mapping.target_partition] = target
            row[rule.output] = value
            rows.append(row)
    expected = node.output_columns()
    for row in rows:
        if set(row) != expected:
            raise PlanError(f'节点 {node.name} 的计算结果列 {sorted(row)} 与目标列 {sorted(expected)} 不一致')
    rows.sort(key=lambda item: tuple(item[column] for column in node.unique_key))
    keys = [tuple(row[column] for column in node.unique_key) for row in rows]
    if len(set(keys)) != len(keys):
        raise ConflictError(f'节点 {node.name} 的分区 {target} 存在唯一键冲突')
    return rows


# --------------------------------------------------------------------------- 分析

@dataclass
class Analysis:
    report: Report
    snapshot: Snapshot
    results: dict[str, dict[str, Result]]


def _stage_compatible(connection, node: NodeSpec, expected: set[str]) -> bool:
    actual = actual_columns(connection, staged_table(node.table))
    return actual is not None and {name for name, _ in actual} == expected


def analyze(plan: Plan, connection, strict: bool = True) -> Analysis:
    """在固定输入快照上分析全部节点；只读，不写业务表与暂存。

    复用判定只认“输入摘要 + 规则摘要”都匹配、并且读取内容重新校验通过的
    产物；文件或行存在本身不构成复用理由。

    发布前的“当前可见内容”按以下顺序确定：本次分析新算出的结果优先，其次
    是已发布记录（且内容摘要校验通过），最后才是未发布的暂存分区。未发布的
    暂存只用于恢复时补齐，绝不会被误认为已经发布。
    """
    init_state(connection)
    connection.execute('BEGIN')
    try:
        verified = verify(plan, connection, strict)
        snapshot = take_snapshot(connection, plan, verified)
        by_name = plan.by_name()
        reports: dict[str, NodeReport] = {}
        results: dict[str, dict[str, Result]] = {}
        published: dict[str, dict[str, Entry]] = {}
        staged: dict[str, dict[str, Entry]] = {}
        rule_changed: dict[str, bool] = {}
        for node in plan.nodes:
            published[node.name] = stored_entries(connection, node, False)
            staged[node.name] = (
                stored_entries(connection, node, True)
                if _stage_compatible(connection, node, node.output_columns())
                else {}
            )
            row = connection.execute('SELECT rule_digest FROM node_state WHERE node=?', (node.name,)).fetchone()
            if row is None:
                rule_changed[node.name] = True
            else:
                rule_changed[node.name] = row['rule_digest'] != node.rule_digest()
            results[node.name] = {}

        def published_digest(name: str, label: str) -> str:
            entry = published[name].get(label)
            return entry.output_digest if entry else EMPTY_DIGEST

        def effective_digest(name: str, label: str) -> str:
            """发布后该分区应有的内容摘要：新结果 > 已发布记录 > 未发布暂存。"""
            result = results[name].get(label)
            if result is not None and result.action not in ('delete', 'noop'):
                return result.digest
            if result is not None and result.action in ('delete', 'noop'):
                return EMPTY_DIGEST
            entry = published[name].get(label)
            if entry is not None:
                return entry.output_digest
            entry = staged[name].get(label)
            return entry.output_digest if entry is not None else EMPTY_DIGEST

        def effective_labels(name: str) -> set[str]:
            labels = {label for label, result in results[name].items() if result.action not in ('delete', 'noop')}
            labels.update(published[name])
            labels.update(staged[name])
            return labels

        def published_rows(node: NodeSpec, entry: Entry) -> tuple[list[dict], bool]:
            """读取已发布分区并重新校验内容摘要；不以存在性代替校验。"""
            rows = select_rows(connection, node.table, node.partition_column, entry.partition, node.output_columns())
            return rows, row_digest(rows) == entry.output_digest

        def staged_rows(node: NodeSpec, entry: Entry) -> tuple[list[dict], bool]:
            rows = select_rows(
                connection, staged_table(node.table), node.partition_column, entry.partition, node.output_columns()
            )
            return rows, row_digest(rows) == entry.output_digest

        def read_upstream(node: NodeSpec, label: str, report: NodeReport) -> tuple[list[dict], bool]:
            """读取上游分区行，返回内容与是否来自本次分析之外（即被扫描）。"""
            upstream = by_name[node.input_node]
            result = results[upstream.name].get(label)
            if result is not None and result.rows is not None:
                source_rows = result.rows
                external = False
            else:
                entry = published[upstream.name].get(label)
                if entry is None:
                    entry = staged[upstream.name].get(label)
                if entry is None:
                    return [], True
                table = upstream.table if label in published[upstream.name] else staged_table(upstream.table)
                source_rows = select_rows(connection, table, upstream.partition_column, label, upstream.output_columns())
                external = True
                report.scanned.add(label)
            columns = node.mapping.source_columns()
            return [{column: row[column] for column in sorted(columns)} for row in source_rows], external

        for node in topological_order(plan):
            report = NodeReport(node, node.rule_digest(), rule_changed[node.name])
            reports[node.name] = report
            candidates: dict[str, Dirty] = {}
            upstream = by_name[node.input_node] if node.source is None else None

            if node.source is not None:
                changed = changed_source_labels(connection, node, snapshot)
                report.checked.update(snapshot.digests.get(node.name, {}))
                for target in sorted(forward_labels(node, changed)):
                    needed = set(input_labels(node, target))
                    dirty = candidates.setdefault(target, Dirty(target, 'recalculate'))
                    overlap = tuple(sorted(needed & changed))
                    if overlap:
                        dirty.merge_cause(Cause('source_content', node.name, overlap))
                if rule_changed[node.name]:
                    present = set(snapshot.digests.get(node.name, {}))
                    for target in sorted(set(published[node.name]) | forward_labels(node, present)):
                        dirty = candidates.setdefault(target, Dirty(target, 'recalculate'))
                        dirty.merge_cause(Cause('rule', node.name))
            else:
                upstream_labels = set(published[upstream.name]) | set(staged[upstream.name]) | set(results[upstream.name])
                changed = {
                    label
                    for label in upstream_labels
                    if effective_digest(upstream.name, label) != published_digest(upstream.name, label)
                }
                # 上游规则变了，即使产物碰巧相同，也要把受影响下游列为候选并解释
                # 原因；是否真正重算仍由输入摘要与规则摘要决定。
                candidates_from = set(changed) | (upstream_labels if rule_changed[upstream.name] else set())
                for target in sorted(forward_labels(node, candidates_from)):
                    needed = set(input_labels(node, target))
                    dirty = candidates.setdefault(target, Dirty(target, 'recalculate'))
                    overlap = tuple(sorted(needed & changed))
                    if overlap:
                        dirty.merge_cause(Cause('upstream_content', upstream.name, overlap))
                        for label in overlap:
                            upstream_dirty = reports[upstream.name].dirty.get(label)
                            if upstream_dirty is not None:
                                dirty.merge_propagated(
                                    cause
                                    for cause in (*upstream_dirty.causes, *upstream_dirty.propagated)
                                    if cause.relevant({label})
                                )
                if rule_changed[node.name]:
                    for target in sorted(set(published[node.name]) | forward_labels(node, effective_labels(upstream.name))):
                        dirty = candidates.setdefault(target, Dirty(target, 'recalculate'))
                        dirty.merge_cause(Cause('rule', node.name))
                if rule_changed[upstream.name]:
                    for label in sorted({
                        target
                        for target in candidates
                        if set(input_labels(node, target)) & upstream_labels
                    }):
                        candidates[label].merge_cause(Cause('upstream_rule', upstream.name))

            # 已发布分区逐个校验内容摘要：文件/行存在不代表产物仍匹配，不能跳过。
            for label in sorted(published[node.name]):
                if label in candidates:
                    continue
                entry = published[node.name][label]
                rows, valid = published_rows(node, entry)
                report.verified += 1
                if not valid:
                    dirty = candidates.setdefault(label, Dirty(label, 'recalculate'))
                    dirty.merge_cause(Cause('published_mismatch', node.name, (label,)))

            for target in sorted(candidates):
                dirty = candidates[target]
                needed = input_labels(node, target)
                rule_digest = node.rule_digest()
                if node.source is not None:
                    input_digests = {
                        label: snapshot.digests.get(node.name, {}).get(label, EMPTY_DIGEST) for label in needed
                    }
                else:
                    input_digests = {label: effective_digest(upstream.name, label) for label in needed}
                input_digest = combine_digests(input_digests)
                entry_staged = staged[node.name].get(target)
                entry_published = published[node.name].get(target)

                if (
                    entry_staged is not None
                    and entry_staged.input_digest == input_digest
                    and entry_staged.rule_digest == rule_digest
                ):
                    rows, valid = staged_rows(node, entry_staged)
                    if valid:
                        report.reuse_staged += 1
                        dirty.action = 'reuse_staged'
                        results[node.name][target] = Result(
                            target, 'reuse_staged', rows, entry_staged.output_digest,
                            dirty.causes, dirty.propagated, input_digest, rule_digest,
                        )
                        report.dirty[target] = dirty
                        continue
                elif (
                    entry_published is not None
                    and entry_published.input_digest == input_digest
                    and entry_published.rule_digest == rule_digest
                ):
                    rows, valid = published_rows(node, entry_published)
                    if valid:
                        report.reuse_published += 1
                        dirty.action = 'reuse_published'
                        results[node.name][target] = Result(
                            target, 'reuse_published', rows, entry_published.output_digest,
                            dirty.causes, dirty.propagated, input_digest, rule_digest,
                        )
                        report.dirty[target] = dirty
                        continue

                inputs: dict[str, list[dict]] = {}
                if node.source is not None:
                    for label in needed:
                        inputs[label] = list(snapshot.rows.get(node.name, {}).get(label, []))
                else:
                    for label in needed:
                        rows, external = read_upstream(node, label, report)
                        inputs[label] = rows
                        if external:
                            report.scanned.add(label)
                rows = compute(node, inputs, target)
                digest = row_digest(rows)
                if not rows:
                    action = 'delete' if entry_published is not None else 'noop'
                    if action == 'delete':
                        report.deleted += 1
                    result = Result(target, action, [], EMPTY_DIGEST, dirty.causes, dirty.propagated, input_digest, rule_digest)
                else:
                    report.recalculated += 1
                    result = Result(target, 'recalculate', rows, digest, dirty.causes, dirty.propagated, input_digest, rule_digest)
                dirty.action = result.action
                results[node.name][target] = result
                report.dirty[target] = dirty

            report.results = results[node.name]
        connection.execute('COMMIT')
    except BaseException:
        connection.execute('ROLLBACK')
        raise
    report = Report([reports[node.name] for node in topological_order(plan)], False, snapshot.digest(), snapshot.size())
    return Analysis(report, snapshot, results)


# --------------------------------------------------------------------------- 暂存与发布

def _write_rows(connection, table: str, column: str, partition: str, rows: list[dict], columns: list[str]) -> None:
    connection.execute(f'DELETE FROM "{table}" WHERE "{column}"=?', (partition,))
    if not rows:
        return
    names = ', '.join(f'"{name}"' for name in columns)
    placeholders = ', '.join('?' for _ in columns)
    connection.executemany(
        f'INSERT INTO "{table}" ({names}) VALUES ({placeholders})',
        [[row[name] for name in columns] for row in rows],
    )


def stage(plan: Plan, connection, analysis: Analysis, hook: Callable[[str], None] | None = None) -> None:
    """逐节点写入暂存与分区状态；每节点单独提交，中断后已完成的节点可恢复。

    暂存只是候选产物：发布前不会被任何查询读到（暂存表名隔离）。恢复时只有
    输入摘要与规则摘要都未变、且暂存内容重新校验通过的分区才会被复用。
    """
    for node in topological_order(plan):
        results = analysis.results[node.name]
        compatible = _stage_compatible(connection, node, node.output_columns())
        with transaction(connection):
            if not compatible:
                if table_exists(connection, staged_table(node.table)):
                    connection.execute(f'DROP TABLE "{staged_table(node.table)}"')
                connection.execute(
                    f'CREATE TABLE "{staged_table(node.table)}" ({", ".join(plan.tables[node.table].definitions)})'
                )
            columns = sorted(node.output_columns())
            for result in results.values():
                if result.action == 'recalculate':
                    _write_rows(
                        connection, staged_table(node.table), node.partition_column, result.partition,
                        result.rows or [], columns,
                    )
                    connection.execute(
                        'INSERT OR REPLACE INTO stage_partitions VALUES (?, ?, ?, ?, ?, ?)',
                        (node.name, result.partition, result.input_digest, result.rule_digest, result.digest, len(result.rows or [])),
                    )
                elif result.action == 'delete':
                    connection.execute('DELETE FROM stage_partitions WHERE node=? AND partition=?', (node.name, result.partition))
                    connection.execute(
                        f'DELETE FROM "{staged_table(node.table)}" WHERE "{node.partition_column}"=?', (result.partition,)
                    )
        if hook is not None:
            hook(node.name)


def publish(plan: Plan, connection, analysis: Analysis, hook: Callable[[str], None] | None = None) -> None:
    """单个事务内把全部节点结果换入正式表。

    提交前查询方读到完整旧版本，提交后读到完整新版本；不存在半新半旧的滚动
    窗口。发布同时记录每个成功分区的输入摘要、规则摘要与输出摘要。
    """
    with transaction(connection):
        for node in topological_order(plan):
            results = analysis.results[node.name]
            columns = sorted(node.output_columns())
            for partition in sorted(results):
                result = results[partition]
                if result.action in ('recalculate', 'reuse_staged'):
                    _write_rows(connection, node.table, node.partition_column, partition, result.rows or [], columns)
                    connection.execute(
                        'INSERT OR REPLACE INTO node_partitions VALUES (?, ?, ?, ?, ?, ?)',
                        (node.name, partition, result.input_digest, result.rule_digest, result.digest, len(result.rows or [])),
                    )
                elif result.action == 'reuse_published':
                    connection.execute(
                        'INSERT OR REPLACE INTO node_partitions VALUES (?, ?, ?, ?, ?, ?)',
                        (node.name, partition, result.input_digest, result.rule_digest, result.digest, len(result.rows or [])),
                    )
                elif result.action == 'delete':
                    connection.execute(f'DELETE FROM "{node.table}" WHERE "{node.partition_column}"=?', (partition,))
                    connection.execute('DELETE FROM node_partitions WHERE node=? AND partition=?', (node.name, partition))
                elif result.action == 'noop':
                    connection.execute('DELETE FROM node_partitions WHERE node=? AND partition=?', (node.name, partition))
            connection.execute('INSERT OR REPLACE INTO node_state VALUES (?, ?)', (node.name, node.rule_digest()))
            if node.source is not None:
                connection.execute('DELETE FROM source_digests WHERE node=?', (node.name,))
                connection.executemany(
                    'INSERT INTO source_digests VALUES (?, ?, ?)',
                    [(node.name, label, digest) for label, digest in sorted(analysis.snapshot.digests.get(node.name, {}).items())],
                )
            connection.execute('DELETE FROM stage_partitions WHERE node=?', (node.name,))
            if table_exists(connection, staged_table(node.table)):
                connection.execute(f'DELETE FROM "{staged_table(node.table)}"')
        connection.execute(
            'INSERT OR REPLACE INTO applied_snapshots VALUES (?, ?)',
            (analysis.snapshot.digest(), analysis.snapshot.size()),
        )
        if hook is not None:
            hook('before_commit')


def run(
    plan: Plan,
    connection,
    apply: bool = True,
    recover: bool = False,
    stage_hook: Callable[[str], None] | None = None,
    publish_hook: Callable[[str], None] | None = None,
) -> Report:
    """解释/试算/执行统一入口。

    ``apply=False`` 只做内存试算，不写正式表与暂存。
    """
    if apply:
        ensure_tables(plan, connection)
    else:
        ensure_tables(plan, connection, create=False)
    if recover:
        recover_staging(plan, connection)
    analysis = analyze(plan, connection, strict=apply)
    if apply:
        stage(plan, connection, analysis, stage_hook)
        publish(plan, connection, analysis, publish_hook)
        analysis.report.applied = True
    return analysis.report


# --------------------------------------------------------------------------- 全量核对

def full_results(plan: Plan, connection) -> dict[str, dict[str, list[dict]]]:
    """不复用任何缓存与暂存，从固定输入快照重新计算全部节点分区。

    目标分区集合 = 现有已发布分区 ∪ 由当前输入可证明影响到的分区，因此整体
    删除来源分区时全量路径也会给出空集，能够与增量结果逐分区比较。
    """
    init_state(connection)
    connection.execute('BEGIN')
    try:
        verified = verify(plan, connection)
        snapshot = take_snapshot(connection, plan, verified)
        by_name = plan.by_name()
        published_labels = {
            node.name: set(stored_entries(connection, node, False)) for node in plan.nodes
        }
        computed: dict[str, dict[str, list[dict]]] = {node.name: {} for node in plan.nodes}
        for node in topological_order(plan):
            if node.source is not None:
                labels = set(snapshot.rows.get(node.name, {})) | published_labels[node.name]
                targets = sorted(set(forward_labels(node, labels)) | published_labels[node.name])
                for target in targets:
                    inputs = {
                        label: list(snapshot.rows.get(node.name, {}).get(label, []))
                        for label in input_labels(node, target)
                    }
                    rows = compute(node, inputs, target)
                    if rows:
                        computed[node.name][target] = rows
            else:
                upstream = by_name[node.input_node]
                labels = set(computed[upstream.name]) | published_labels[upstream.name]
                targets = sorted(set(forward_labels(node, labels)) | published_labels[node.name])
                for target in targets:
                    columns = node.mapping.source_columns()
                    inputs = {
                        label: [
                            {column: row[column] for column in sorted(columns)}
                            for row in computed[upstream.name].get(label, [])
                        ]
                        for label in input_labels(node, target)
                    }
                    rows = compute(node, inputs, target)
                    if rows:
                        computed[node.name][target] = rows
        connection.execute('COMMIT')
    except BaseException:
        connection.execute('ROLLBACK')
        raise
    return computed


def compare(plan: Plan, connection, full: dict[str, dict[str, list[dict]]]) -> dict:
    """逐分区比较正式表与全量计算结果，包含分区集合差异。"""
    nodes = {}
    equal = True
    for node in plan.nodes:
        expected = full.get(node.name, {})
        actual_labels = current_labels(connection, node)
        mismatches = []
        for label in sorted(set(expected) | actual_labels):
            actual_rows = (
                select_rows(connection, node.table, node.partition_column, label, node.output_columns())
                if label in actual_labels
                else []
            )
            expected_rows = expected.get(label, [])
            if row_digest(actual_rows) != row_digest(expected_rows):
                mismatches.append(
                    {'partition': label, 'expected_rows': len(expected_rows), 'actual_rows': len(actual_rows)}
                )
        nodes[node.name] = {'partitions': len(expected), 'mismatches': mismatches}
        equal = equal and not mismatches
    return {'equal': equal, 'nodes': nodes}


def rebuild_full(plan: Plan, connection) -> dict:
    """全量重建路径：清空输出并以全量结果原子发布，供核对与修复。"""
    full = full_results(plan, connection)
    with transaction(connection):
        for node in topological_order(plan):
            columns = sorted(node.output_columns())
            connection.execute(f'DELETE FROM "{node.table}"')
            connection.execute('DELETE FROM node_partitions WHERE node=?', (node.name,))
            connection.execute('DELETE FROM stage_partitions WHERE node=?', (node.name,))
            if table_exists(connection, staged_table(node.table)):
                connection.execute(f'DELETE FROM "{staged_table(node.table)}"')
            rows = [row for label in sorted(full[node.name]) for row in full[node.name][label]]
            if rows:
                names = ', '.join(f'"{name}"' for name in columns)
                connection.executemany(
                    f'INSERT INTO "{node.table}" ({names}) VALUES ({", ".join("?" for _ in columns)})',
                    [[row[name] for name in columns] for row in rows],
                )
            connection.execute('INSERT OR REPLACE INTO node_state VALUES (?, ?)', (node.name, node.rule_digest()))
        # 元数据在同一个事务内按全量结果重算。
        _repopulate_metadata(plan, connection, full)
    return {'rebuilt': True, 'partitions': {name: len(parts) for name, parts in full.items()}}


def _repopulate_metadata(plan: Plan, connection, full: dict[str, dict[str, list[dict]]]) -> None:
    by_name = plan.by_name()
    verified = verify(plan, connection)
    for node in topological_order(plan):
        if node.source is not None:
            info = verified[node.name]
            column = node.mapping.source_partition
            digests = {}
            for (label,) in connection.execute(f'SELECT DISTINCT "{column}" FROM "{info["input_table"]}"'):
                normalized = normalize_label(node.partition_by, label)
                rows = select_rows(connection, info['input_table'], column, normalized, node.mapping.source_columns())
                digests[normalized] = row_digest(rows)
            connection.execute('DELETE FROM source_digests WHERE node=?', (node.name,))
            connection.executemany(
                'INSERT INTO source_digests VALUES (?, ?, ?)',
                [(node.name, label, digest) for label, digest in sorted(digests.items())],
            )
            input_digest_for = lambda target: combine_digests(  # noqa: E731
                {label: digests.get(label, EMPTY_DIGEST) for label in input_labels(node, target)}
            )
        else:
            upstream = by_name[node.input_node]
            digests = {label: row_digest(rows) for label, rows in full[upstream.name].items()}
            input_digest_for = lambda target: combine_digests(  # noqa: E731
                {label: digests.get(label, EMPTY_DIGEST) for label in input_labels(node, target)}
            )
        entries = [
            (
                node.name,
                label,
                input_digest_for(label),
                node.rule_digest(),
                row_digest(rows),
                len(rows),
            )
            for label, rows in sorted(full[node.name].items())
        ]
        connection.executemany('INSERT INTO node_partitions VALUES (?, ?, ?, ?, ?, ?)', entries)


def recover_staging(plan: Plan, connection) -> dict:
    """发布中断恢复入口。

    暂存分区只有在输入摘要与规则摘要都匹配、并且暂存内容重新校验通过时才会
    保留；其余暂存行与记录一律清除。真正的复用判定仍在 ``analyze`` 中完成。
    """
    init_state(connection)
    dropped = {}
    for node in topological_order(plan):
        compatible = _stage_compatible(connection, node, node.output_columns())
        entries = stored_entries(connection, node, True) if compatible else {}
        keep = []
        for entry in entries.values():
            rows = select_rows(
                connection, staged_table(node.table), node.partition_column, entry.partition, node.output_columns()
            )
            if row_digest(rows) == entry.output_digest:
                keep.append(entry.partition)
        dropped[node.name] = sorted(set(entries) - set(keep))
        with transaction(connection):
            if not compatible:
                if table_exists(connection, staged_table(node.table)):
                    connection.execute(f'DROP TABLE "{staged_table(node.table)}"')
            else:
                for partition in dropped[node.name]:
                    connection.execute(
                        f'DELETE FROM "{staged_table(node.table)}" WHERE "{node.partition_column}"=?', (partition,)
                    )
            connection.execute('DELETE FROM stage_partitions WHERE node=?', (node.name,))
            connection.executemany(
                'INSERT INTO stage_partitions VALUES (?, ?, ?, ?, ?, ?)',
                [
                    (node.name, entry.partition, entry.input_digest, entry.rule_digest, entry.output_digest, entry.rows)
                    for entry in entries.values()
                    if entry.partition in keep
                ],
            )
    return {'dropped': dropped}
