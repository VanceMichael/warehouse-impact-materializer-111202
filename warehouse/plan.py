"""声明式增量物化计划：节点声明、表结构核验与分区映射。

这一层与执行状态无关，只回答四个问题：

1. 节点声明了哪些唯一键、分区粒度、依赖/来源和显式列映射？
2. 映射类型是否属于首期支持的同日、向后固定天数窗口、日到自然周？
3. 声明的表结构与数据库里的实际表是否一致，引用的列是否都声明过？
4. 给定一个变更的输入分区可能影响哪些输出分区；产出某个输出分区又必须
   读取哪些输入分区？

映射范围必须可证明：窗口映射要求固定的 ``window_days``；日到自然周由 ISO
周历法决定。这里不解析、也不猜测任意 SQL。所有日期偏移都通过
:class:`datetime.date` 与 :class:`datetime.timedelta` 完成，不做字符串加减。
"""

from __future__ import annotations

import hashlib
import json
import re
import tomllib
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable

GRANULARITIES = ('day', 'week')
MAP_KINDS = ('identity', 'day_offset', 'day_to_week')
RULE_OPS = ('project', 'sum', 'count', 'min', 'max')
AGGREGATES = ('sum', 'count', 'min', 'max')
NAME_PATTERN = re.compile(r'^[a-z][a-z0-9_-]*$')
WEEK_PATTERN = re.compile(r'^(\d{4})-W(\d{2})$')
MAX_WINDOW = 3660
EMPTY_DIGEST = hashlib.sha256(b'[]').hexdigest()


class PlanError(ValueError):
    """计划声明或映射核验不通过。"""


class ConflictError(ValueError):
    """待物化结果违反唯一键或映射约束。"""


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


# --------------------------------------------------------------------------- 日期

def parse_day(value: Any) -> date:
    if not isinstance(value, str):
        raise PlanError(f'日期必须是 ISO 文本：{value!r}')
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise PlanError(f'非法日期 {value!r}：{error}') from None


def shift_day(day: str, days: int) -> str:
    """唯一的日期偏移方式：解析为 date 后再按日历天偏移。"""
    if not isinstance(days, int) or isinstance(days, bool):
        raise PlanError(f'日期偏移必须是整数：{days!r}')
    return (parse_day(day) + timedelta(days=days)).isoformat()


def week_label(day: str) -> str:
    calendar = parse_day(day).isocalendar()
    return f'{calendar.year:04d}-W{calendar.week:02d}'


def week_days(label: str) -> list[str]:
    match = WEEK_PATTERN.match(label) if isinstance(label, str) else None
    if not match:
        raise PlanError(f'非法 ISO 周标签 {label!r}')
    year, week = int(match.group(1)), int(match.group(2))
    try:
        monday = date.fromisocalendar(year, week, 1)
    except ValueError as error:
        raise PlanError(f'非法 ISO 周标签 {label!r}：{error}') from None
    return [shift_day(monday.isoformat(), offset) for offset in range(7)]


def normalize_label(granularity: str, value: Any) -> str:
    """规范化分区值；非规范文本直接拒绝，避免日期边界上的字符串歧义。"""
    if granularity == 'day':
        label = parse_day(value).isoformat()
    elif granularity == 'week':
        match = WEEK_PATTERN.match(value) if isinstance(value, str) else None
        if not match:
            raise PlanError(f'非法 ISO 周标签 {value!r}')
        year, week = int(match.group(1)), int(match.group(2))
        try:
            date.fromisocalendar(year, week, 1)
        except ValueError as error:
            raise PlanError(f'非法 ISO 周标签 {value!r}：{error}') from None
        label = f'{year:04d}-W{week:02d}'
    else:
        raise PlanError(f'未知分区粒度 {granularity!r}')
    if value != label:
        raise PlanError(f'分区值必须是规范形式 {label!r}，实际为 {value!r}')
    return label


def forward_labels(node: 'NodeSpec', labels: Iterable[str]) -> set[str]:
    """输入分区标签变化可能影响到的输出分区标签。

    窗口语义：输出日期 ``t`` 读取来源 ``t - step``，``step`` 覆盖
    ``0..window_days``。因此来源日期 ``s`` 影响 ``s .. s + window_days``，
    每个来源日期最多影响 ``window_days + 1`` 个输出分区，范围可证明。
    """
    mapping = node.mapping
    if mapping.kind == 'identity':
        return {normalize_label(node.partition_by, label) for label in labels}
    if mapping.kind == 'day_offset':
        result = set()
        for label in labels:
            parse_day(label)
            for step in range(mapping.window_days + 1):
                result.add(shift_day(label, step))
        return result
    if mapping.kind == 'day_to_week':
        return {week_label(label) for label in labels}
    raise PlanError(f'未知映射类型 {mapping.kind!r} 在节点 {node.name}')


def input_labels(node: 'NodeSpec', target: str) -> list[str]:
    """产出某个输出分区必须读取的输入分区，顺序固定。"""
    mapping = node.mapping
    if mapping.kind == 'identity':
        return [normalize_label(node.partition_by, target)]
    if mapping.kind == 'day_offset':
        parse_day(target)
        return [shift_day(target, -step) for step in range(mapping.window_days + 1)]
    if mapping.kind == 'day_to_week':
        return week_days(target)
    raise PlanError(f'未知映射类型 {mapping.kind!r} 在节点 {node.name}')


# --------------------------------------------------------------------------- 声明结构

@dataclass(frozen=True)
class ColumnRef:
    column: str
    partition: bool = False


@dataclass(frozen=True)
class Mapping:
    kind: str
    source: tuple[ColumnRef, ...]
    target: tuple[ColumnRef, ...]
    window_days: int = 0

    def source_columns(self) -> set[str]:
        return {ref.column for ref in self.source}

    def target_columns(self) -> set[str]:
        return {ref.column for ref in self.target}

    @property
    def source_partition(self) -> str:
        return [ref.column for ref in self.source if ref.partition][0]

    @property
    def target_partition(self) -> str:
        return [ref.column for ref in self.target if ref.partition][0]

    def source_keys(self) -> tuple[str, ...]:
        return tuple(ref.column for ref in self.source if not ref.partition)

    def target_keys(self) -> tuple[str, ...]:
        return tuple(ref.column for ref in self.target if not ref.partition)

    def to_dict(self) -> dict:
        value = {
            'kind': self.kind,
            'source': [{'column': ref.column, 'partition': ref.partition} for ref in self.source],
            'target': [{'column': ref.column, 'partition': ref.partition} for ref in self.target],
        }
        if self.window_days:
            value['window_days'] = self.window_days
        return value


@dataclass(frozen=True)
class Rule:
    op: str
    value: str | None = None
    output: str | None = None

    def to_dict(self) -> dict:
        value = {'op': self.op}
        if self.value is not None:
            value['value'] = self.value
        if self.output is not None:
            value['output'] = self.output
        return value


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[tuple[str, str], ...]
    key: tuple[str, ...]
    definitions: tuple[str, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.columns)

    def to_dict(self) -> dict:
        return {'columns': list(self.definitions), 'key': list(self.key)}


@dataclass(frozen=True)
class NodeSpec:
    name: str
    table: str
    unique_key: tuple[str, ...]
    partition_by: str
    mapping: Mapping
    rule: Rule
    source: str | None = None
    depends_on: tuple[str, ...] = ()

    @property
    def partition_column(self) -> str:
        return self.mapping.target_partition

    @property
    def input_node(self) -> str | None:
        return self.depends_on[0] if self.depends_on else None

    def output_columns(self) -> set[str]:
        target = self.mapping.target_columns()
        if self.rule.op == 'project':
            return target
        return target | {self.rule.output}

    def rule_digest(self) -> str:
        return fingerprint(self.to_dict())

    def to_dict(self) -> dict:
        value = {
            'name': self.name,
            'table': self.table,
            'unique_key': list(self.unique_key),
            'partition_by': self.partition_by,
            'mapping': self.mapping.to_dict(),
            'rule': self.rule.to_dict(),
        }
        if self.source is not None:
            value['source'] = self.source
        if self.depends_on:
            value['depends_on'] = list(self.depends_on)
        return value


@dataclass(frozen=True)
class Plan:
    nodes: tuple[NodeSpec, ...]
    tables: dict[str, TableSpec]
    version: str | None = None

    def by_name(self) -> dict[str, NodeSpec]:
        return {node.name: node for node in self.nodes}

    def dependencies(self, name: str) -> tuple[str, ...]:
        node = self.by_name()[name]
        return () if node.source is not None else node.depends_on

    def rule_digest(self, name: str) -> str:
        return self.by_name()[name].rule_digest()

    def to_dict(self) -> dict:
        return {
            'version': self.version,
            'tables': {name: table.to_dict() for name, table in self.tables.items()},
            'nodes': [node.to_dict() for node in self.nodes],
        }


# --------------------------------------------------------------------------- 解析与声明校验

def _refs(value: Any, where: str) -> tuple[ColumnRef, ...]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list) or not value:
        raise PlanError(f'{where} 必须是非空数组或列名')
    refs = []
    for item in value:
        if isinstance(item, str):
            refs.append(ColumnRef(item))
        elif isinstance(item, dict):
            unknown = set(item) - {'column', 'partition'}
            if unknown:
                raise PlanError(f'{where} 存在未声明的列属性 {sorted(unknown)}')
            if not isinstance(item.get('column'), str) or not item['column']:
                raise PlanError(f'{where} 的 column 必须是非空字符串')
            partition = item.get('partition', False)
            if not isinstance(partition, bool):
                raise PlanError(f'{where} 的 partition 必须是布尔值')
            refs.append(ColumnRef(item['column'], partition))
        else:
            raise PlanError(f'{where} 的列引用格式错误；只接受列名或带 column/partition 的对象')
    if len({ref.column for ref in refs}) != len(refs):
        raise PlanError(f'{where} 存在重复列')
    return tuple(refs)


def _columns(value: Any, where: str) -> tuple[tuple[ColumnRef, ...], tuple[ColumnRef, ...]]:
    if not isinstance(value, dict) or set(value) != {'source', 'target'}:
        raise PlanError(f'{where} 必须只声明 source 与 target 列')
    return _refs(value['source'], f'{where}.source'), _refs(value['target'], f'{where}.target')


def _mapping(value: Any, where: str) -> Mapping:
    if not isinstance(value, dict):
        raise PlanError(f'{where} 必须声明映射')
    unknown = set(value) - {'kind', 'window_days', 'columns'}
    if unknown:
        raise PlanError(f'{where} 存在未声明的映射属性 {sorted(unknown)}')
    kind = value.get('kind')
    if kind not in MAP_KINDS:
        raise PlanError(f'{where} 的映射类型 {kind!r} 不在首期支持范围 {list(MAP_KINDS)}')
    source, target = _columns(value.get('columns'), f'{where}.columns')
    if sum(ref.partition for ref in source) != 1:
        raise PlanError(f'{where} 必须恰好声明一个来源分区列')
    if sum(ref.partition for ref in target) != 1:
        raise PlanError(f'{where} 必须恰好声明一个目标分区列')
    if kind == 'day_offset':
        window = value.get('window_days')
        if not isinstance(window, int) or isinstance(window, bool) or not 0 <= window <= MAX_WINDOW:
            raise PlanError(f'{where} 的 window_days 必须是 [0, {MAX_WINDOW}] 内的固定整数，范围才可证明')
    else:
        if 'window_days' in value and value['window_days'] != 0:
            raise PlanError(f'{where} 的 {kind} 映射不能声明窗口，范围不可证明')
        window = 0
    return Mapping(kind, source, target, window)


def _rule(value: Any, where: str) -> Rule:
    if not isinstance(value, dict) or 'op' not in value:
        raise PlanError(f'{where} 必须是含 op 的对象')
    unknown = set(value) - {'op', 'value', 'output'}
    if unknown:
        raise PlanError(f'{where} 存在未声明的规则属性 {sorted(unknown)}')
    op = value['op']
    if op not in RULE_OPS:
        raise PlanError(f'{where} 的 op {op!r} 不在 {list(RULE_OPS)}')
    column = value.get('value')
    output = value.get('output')
    if output is not None and (not isinstance(output, str) or not output):
        raise PlanError(f'{where} 的 output 必须是非空列名')
    if op == 'project':
        if column is not None or output is not None:
            raise PlanError(f'{where} 的 project 规则不能声明 value 或 output')
        return Rule(op)
    if not isinstance(column, str) or not column:
        raise PlanError(f'{where} 的 value 必须是非空列名')
    if not isinstance(output, str) or not output:
        raise PlanError(f'{where} 的聚合规则必须声明 output 列')
    return Rule(op, column, output)


def _names(value: Any, where: str, required: bool = True) -> tuple[str, ...]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list) or (required and not value):
        raise PlanError(f'{where} 必须是非空字符串数组')
    if not all(isinstance(item, str) and item for item in value):
        raise PlanError(f'{where} 的元素必须是非空字符串')
    if len(set(value)) != len(value):
        raise PlanError(f'{where} 存在重复项')
    return tuple(value)


def _definition(text: Any, table: str) -> tuple[str, str, str]:
    if not isinstance(text, str) or not text.strip():
        raise PlanError(f'表 {table} 的列声明必须是非空文本')
    text = text.strip()
    if text[0] in ('"', "'"):
        quote = text[0]
        end = text.find(quote, 1)
        if end < 0:
            raise PlanError(f'表 {table} 的列名引号未闭合：{text!r}')
        name = text[1:end]
        rest = text[end + 1:]
    else:
        parts = text.split(None, 1)
        name = parts[0]
        rest = parts[1] if len(parts) > 1 else ''
    rest = rest.strip()
    if not name or not rest:
        raise PlanError(f'表 {table} 的列声明必须包含列名与类型：{text!r}')
    kind = rest.split()[0].upper()
    if not kind:
        raise PlanError(f'表 {table} 的列声明缺少类型：{text!r}')
    return name, kind, text


def _table(value: Any, name: str) -> TableSpec:
    if not isinstance(value, dict) or set(value) - {'columns', 'key'}:
        raise PlanError(f'表 {name} 的声明只能包含 columns 与 key')
    raw_columns = value.get('columns')
    if not isinstance(raw_columns, list) or not raw_columns:
        raise PlanError(f'表 {name} 的 columns 必须是非空数组')
    columns: list[tuple[str, str]] = []
    definitions: list[str] = []
    for raw in raw_columns:
        column, kind, definition = _definition(raw, name)
        if column in {existing for existing, _ in columns}:
            raise PlanError(f'表 {name} 的列 {column!r} 重复')
        columns.append((column, kind))
        definitions.append(definition)
    key = _names(value.get('key'), f'表 {name} 的 key')
    column_names = {column for column, _ in columns}
    if not set(key) <= column_names:
        raise PlanError(f'表 {name} 的 key 引用了未声明列 {sorted(set(key) - column_names)}')
    return TableSpec(name, tuple(columns), key, tuple(definitions))


def _node(raw: Any) -> NodeSpec:
    if not isinstance(raw, dict):
        raise PlanError('节点声明必须是对象')
    unknown = set(raw) - {'name', 'table', 'unique_key', 'partition_by', 'depends_on', 'source', 'mapping', 'rule'}
    if unknown:
        raise PlanError(f'节点存在未声明属性 {sorted(unknown)}')
    name = raw.get('name')
    if not isinstance(name, str) or not NAME_PATTERN.match(name):
        raise PlanError(f'节点名 {name!r} 必须以小写字母开头，只含小写字母、数字、下划线或短横线')
    table = raw.get('table', name)
    if not isinstance(table, str) or not table:
        raise PlanError(f'节点 {name} 的 table 必须是非空字符串')
    partition_by = raw.get('partition_by')
    if partition_by not in GRANULARITIES:
        raise PlanError(f'节点 {name} 的分区粒度 {partition_by!r} 不在 {list(GRANULARITIES)}')
    unique_key = _names(raw.get('unique_key'), f'节点 {name} 的 unique_key')
    depends_on = _names(raw.get('depends_on'), f'节点 {name} 的 depends_on', required=False) if 'depends_on' in raw else ()
    source = raw.get('source')
    if source is not None and (not isinstance(source, str) or not source):
        raise PlanError(f'节点 {name} 的 source 必须是非空表名')
    if source is not None and depends_on:
        raise PlanError(f'节点 {name} 不能同时声明 source 与 depends_on')
    if source is None and not depends_on:
        raise PlanError(f'节点 {name} 必须声明 source 或 depends_on')
    if len(depends_on) > 1:
        raise PlanError(f'节点 {name} 首期只支持单一输入映射，depends_on 最多一个')
    mapping = _mapping(raw.get('mapping'), f'节点 {name} 的 mapping')
    rule = _rule(raw.get('rule'), f'节点 {name} 的 rule')
    source_partition = mapping.source_partition
    target_partition = mapping.target_partition
    if node_partition(kind=mapping.kind, partition_by=partition_by) is False:
        raise PlanError(
            f'节点 {name} 的 {mapping.kind} 映射与分区粒度 {partition_by} 不匹配'
        )
    if unique_key and target_partition not in unique_key:
        raise PlanError(f'节点 {name} 的目标分区列 {target_partition} 必须属于唯一键')
    if rule.op == 'project':
        if len(mapping.source) != len(mapping.target):
            raise PlanError(f'节点 {name} 的 project 映射要求 source 与 target 列数一致')
        if mapping.source_keys() != mapping.target_keys():
            raise PlanError(
                f'节点 {name} 的 project 映射键列 {list(mapping.source_keys())} 与 '
                f'{list(mapping.target_keys())} 不一致'
            )
    else:
        if len(mapping.source) != len(mapping.target) + 1:
            raise PlanError(f'节点 {name} 的聚合映射必须恰好多出一个来源值列')
        if rule.value not in mapping.source_keys():
            raise PlanError(f'节点 {name} 的 rule.value 列 {rule.value} 未被映射的 source 声明')
        if rule.value in mapping.target_columns():
            raise PlanError(f'节点 {name} 的 rule.value 列 {rule.value} 不能同时作为映射目标列')
        if rule.output in mapping.target_columns():
            raise PlanError(f'节点 {name} 的 rule.output 列 {rule.output} 不能同时作为映射目标列')
        source_keys = tuple(column for column in mapping.source_keys() if column != rule.value)
        if len(source_keys) != len(mapping.target_keys()):
            raise PlanError(f'节点 {name} 的聚合键列数不一致')
    return NodeSpec(name, table, unique_key, partition_by, mapping, rule, source, depends_on)


def node_partition(*, kind: str, partition_by: str) -> bool:
    if kind in ('identity', 'day_offset'):
        return partition_by == 'day'
    if kind == 'day_to_week':
        return partition_by == 'week'
    return False


def load_plan(path: str | Path) -> Plan:
    try:
        data = tomllib.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise PlanError(f'无法读取计划：{error}') from None
    if not isinstance(data, dict) or set(data) - {'version', 'nodes', 'tables'}:
        raise PlanError('计划文件只能包含 version、tables 与 nodes')
    raw_tables = data.get('tables', {})
    if not isinstance(raw_tables, dict):
        raise PlanError('tables 必须是表名到声明的映射')
    tables = {name: _table(value, name) for name, value in raw_tables.items()}
    raw_nodes = data.get('nodes')
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise PlanError('计划需要非空的 nodes 数组')
    nodes = tuple(_node(raw) for raw in raw_nodes)
    seen: dict[str, NodeSpec] = {}
    table_owner: dict[str, str] = {}
    for node in nodes:
        if node.name in seen:
            raise PlanError(f'节点名 {node.name} 重复')
        if node.table in table_owner:
            raise PlanError(f'表 {node.table} 被节点 {table_owner[node.table]} 与 {node.name} 重复声明')
        if node.table not in tables:
            raise PlanError(f'节点 {node.name} 的目标表 {node.table} 缺少 tables 结构声明')
        seen[node.name] = node
        table_owner[node.table] = node.name
    for node in nodes:
        table = tables[node.table]
        if set(node.unique_key) != set(table.key):
            raise PlanError(f'节点 {node.name} 的唯一键 {list(node.unique_key)} 与表 {node.table} 的 key {list(table.key)} 不一致')
        if node.output_columns() != set(table.column_names):
            raise PlanError(
                f'节点 {node.name} 的输出列 {sorted(node.output_columns())} '
                f'与表 {node.table} 声明列 {sorted(table.column_names)} 不一致'
            )
        for dependency in node.depends_on:
            if dependency == node.name:
                raise PlanError(f'节点 {node.name} 不能依赖自身')
            if dependency not in seen:
                raise PlanError(f'节点 {node.name} 依赖未声明的节点 {dependency}')
        if node.source is not None:
            if node.source not in tables:
                raise PlanError(f'节点 {node.name} 的来源表 {node.source} 未在 tables 声明')
            if node.source in table_owner:
                raise PlanError(f'节点 {node.name} 以节点表 {node.source} 为普通来源，应改用 depends_on')
            undeclared = node.mapping.source_columns() - set(tables[node.source].column_names)
            if undeclared:
                raise PlanError(
                    f'节点 {node.name} 的映射引用了来源表 {node.source} 未声明列 {sorted(undeclared)}'
                )

    version = data.get('version')
    if version is not None and not isinstance(version, str):
        raise PlanError('version 必须是字符串')
    return Plan(nodes, tables, version)


def topological_order(plan: Plan) -> list[NodeSpec]:
    """拓扑序；循环依赖直接拒绝。"""
    remaining = {node.name: set(plan.dependencies(node.name)) for node in plan.nodes}
    by_name = plan.by_name()
    result: list[NodeSpec] = []
    while remaining:
        ready = sorted(name for name, dependencies in remaining.items() if not dependencies)
        if not ready:
            raise PlanError(f'依赖存在循环：{sorted(remaining)}')
        for name in ready:
            result.append(by_name[name])
            del remaining[name]
        for dependencies in remaining.values():
            dependencies.difference_update(ready)
    return result


# --------------------------------------------------------------------------- 与实际表核验

def actual_columns(connection, table: str) -> list[tuple[str, str]] | None:
    exists = connection.execute('SELECT 1 FROM sqlite_master WHERE type=? AND name=?', ('table', table)).fetchone()
    if not exists:
        return None
    return [(row[1], (row[2] or '').upper()) for row in connection.execute(f'PRAGMA table_info("{table}")')]


def actual_key(connection, table: str) -> tuple[str, ...]:
    """实际主键；没有主键时返回唯一索引覆盖的列（按索引定义顺序）。"""
    key = tuple(row[1] for row in connection.execute(f'PRAGMA table_info("{table}")') if row[5])
    if key:
        return key
    for index in connection.execute(f'PRAGMA index_list("{table}")'):
        if not index[2]:
            continue
        columns = tuple(row[2] for row in connection.execute(f'PRAGMA index_info("{index[1]}")'))
        if columns:
            return columns
    return ()


def ensure_tables(plan: Plan, connection, create: bool = True) -> None:
    """创建输出表与唯一键约束；来源表必须已存在，列与声明完全一致。

    ``create=False`` 只做核验（解释与只读试算不得创建或修改表结构）。
    """
    output_tables = {node.table for node in plan.nodes}
    for name, table in plan.tables.items():
        actual = actual_columns(connection, name)
        if actual is None:
            if name not in output_tables:
                raise PlanError(f'来源表 {name} 不存在；请先导入')
            if not create:
                continue
            connection.execute(f'CREATE TABLE "{name}" ({", ".join(table.definitions)})')
            actual = actual_columns(connection, name)
        if tuple(actual) != tuple(table.columns):
            raise PlanError(f'表 {name} 的实际列 {actual} 与计划声明 {list(table.columns)} 不一致')
        if name in output_tables:
            if actual_key(connection, name) != tuple(table.key):
                declared_primary = tuple(
                    row[1] for row in connection.execute(f'PRAGMA table_info("{name}")') if row[5]
                )
                if declared_primary:
                    raise PlanError(
                        f'表 {name} 的主键 {declared_primary} 与计划声明 key {list(table.key)} 不一致'
                    )
                if not create:
                    raise PlanError(f'表 {name} 缺少与声明一致的唯一键 {list(table.key)}')
                connection.execute(
                    f'CREATE UNIQUE INDEX IF NOT EXISTS "ux_{name}" ON "{name}" ({", ".join(table.key)})'
                )
                if actual_key(connection, name) != tuple(table.key):
                    raise PlanError(f'表 {name} 无法建立与声明一致的唯一键 {list(table.key)}')


def verify(plan: Plan, connection, strict: bool = True) -> dict[str, dict]:
    """核验声明与实际输入表、输出表一致；返回每个节点的输入表与列集合。

    ``strict=False`` 允许输出表尚未创建，此时按计划声明核对列集合。
    """
    by_name = plan.by_name()
    verified: dict[str, dict] = {}
    for node in topological_order(plan):
        if node.source is not None:
            actual = actual_columns(connection, node.source)
            if actual is None:
                raise PlanError(f'节点 {node.name} 声明的来源表 {node.source} 不存在')
            input_table = node.source
            input_columns = {name for name, _ in actual}
        else:
            dependency = by_name[node.input_node]
            input_table = dependency.table
            input_columns = verified[dependency.name]['output_columns']
            if node.mapping.kind in ('identity', 'day_offset') and dependency.partition_by != 'day':
                raise PlanError(f'节点 {node.name} 的 {node.mapping.kind} 映射要求输入为日粒度')
            if node.mapping.kind == 'identity' and node.partition_by != dependency.partition_by:
                raise PlanError(f'节点 {node.name} 的 identity 映射要求输入与输出粒度一致')
            if node.mapping.kind == 'day_to_week' and dependency.partition_by != 'day':
                raise PlanError(f'节点 {node.name} 的 day_to_week 映射要求输入为日粒度')
        unknown_source = node.mapping.source_columns() - input_columns
        if unknown_source:
            raise PlanError(
                f'节点 {node.name} 的映射引用了实际输入表 {input_table} 不存在的列 {sorted(unknown_source)}'
            )
        actual = actual_columns(connection, node.table)
        definition = plan.tables[node.table]
        if actual is None:
            if strict:
                raise PlanError(f'节点 {node.name} 的目标表 {node.table} 不存在；请先 apply 或检查声明')
            # 目标表尚未建立时按声明列核对；只读试算不写任何表结构。
            output_columns = set(definition.column_names)
        else:
            if tuple(actual) != tuple(definition.columns):
                raise PlanError(f'节点 {node.name} 的目标表 {node.table} 实际列 {actual} 与计划声明不一致')
            output_columns = {name for name, _ in actual}
        if node.output_columns() != output_columns:
            raise PlanError(f'节点 {node.name} 的输出列 {sorted(node.output_columns())} 与表 {node.table} 不一致')
        if not set(node.unique_key) <= output_columns:
            raise PlanError(f'节点 {node.name} 的唯一键列不在输出列中')
        actual_key_columns = actual_key(connection, node.table)
        if actual_key_columns and actual_key_columns != tuple(node.unique_key):
            raise PlanError(
                f'节点 {node.name} 的唯一键 {list(node.unique_key)} 与实际表 {node.table} '
                f'的键 {list(actual_key_columns)} 不一致'
            )
        verified[node.name] = {
            'input_table': input_table,
            'input_columns': input_columns,
            'output_columns': output_columns,
        }
    return verified
