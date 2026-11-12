# impact-materializer

将每日需求按声明式计划物化为日、七日滚动与 ISO 自然周层，并支持分区级增量
重算。跨年周使用 ISO 所属年，日期边界一律按日历计算。

## 数据组织

- `demand`：`record TEXT PRIMARY KEY, day TEXT NOT NULL, item TEXT NOT NULL, units INTEGER NOT NULL`
- `daily_units`：`day, item, demand_units`，唯一键 `(day, item)`
- `rolling_7d`：`day, item, window_units`，唯一键 `(day, item)`，覆盖 `day` 及之前 6 天
- `weekly_units`：`week, item, week_units`，唯一键 `(week, item)`，`week` 为 `YYYY-Www`

`examples/source.json` 为本地合成输入，`examples/expected.json` 是原始周汇总查询的
公开样例结果。`schema.json` 描述输入结构，运行时定义在 `warehouse/definition.py`。
业务计算位于 `warehouse/domain.py`，事务装载位于 `warehouse/catalog.py`，内容寻址
JSONL 对象位于 `warehouse/store.py`。计划声明与核验在 `warehouse/plan.py`，增量
执行、恢复与全量核对在 `warehouse/engine.py`。

输入批次以内容 SHA-256 固定；相同批次重投不重复写入，相同批次内容不同则报错。
装载以完整输入包为事务，主键冲突不留下半批记录。关系数据保存在 SQLite，精确
数量用十进制文本；导出对象按内容哈希命名，读取会重新校验，来源不在运行时修改。

## 声明式计划

`examples/plans/demand.toml` 声明三层节点。每个节点声明唯一键、分区粒度
（`day`/`week`）、来源表或依赖、显式列映射与规则：

```toml
[[nodes]]
name = "rolling_7d"
table = "rolling_7d"
depends_on = ["daily"]
partition_by = "day"
unique_key = ["day", "item"]
rule = { op = "sum", value = "demand_units", output = "window_units" }
mapping = { kind = "day_offset", window_days = 6, columns = { source = [{ column = "day", partition = true }, "item", "demand_units"], target = [{ column = "day", partition = true }, "item"] } }
```

首期支持的映射：

| kind | 语义 | 可证明的扇出 |
| --- | --- | --- |
| `identity` | 同日一对一投影 | 1 个输出分区 |
| `day_offset` | 输出日期读取 `day, day-1, ..., day-window_days` | 每个输入日期最多 `window_days + 1` 个输出分区 |
| `day_to_week` | 日分区按 ISO 周标签归并 | 每个输入日期恰好 1 个周分区 |

计划在装载时核验：未声明的节点属性、映射属性、表与列、未声明引用、缺失窗口、
无法证明范围的映射、循环依赖、唯一键与表 `key` 不一致、声明列与实际表列不一致
都会被拒绝。依赖只来自声明，绝不从 SQL 猜测。

## 增量语义

- 每次执行先固定输入快照（只含计划声明引用的列），再在快照上分析。
- 脏分区来源：来源内容变化（新增、更新、删除、维度回溯）、已发布产物校验不符、
  节点规则指纹变化，以及沿依赖边传递的上游变化。
- 传递原因分直接原因与传播原因；每条原因带 `root` 分类，维护者能区分
  `source`（来源本身变了）、`rule`（依赖规则变了）与 `artifact`（已发布产物损坏）。
- 日期边界全部用 `datetime.date` / `timedelta` / `isocalendar`，不做字符串加减。
- 每个成功分区记录输入摘要、规则摘要、输出摘要和行数；恢复只复用三者匹配且
  重新读取校验通过的分区，不因文件或行存在就跳过。
- 执行先逐节点写暂存（`stage_<table>`），完成后在单个 SQLite 事务内发布；
  提交前查询方只看到完整旧版本，提交后只看到完整新版本，不会读到半新半旧的
  滚动窗口。
- 全量路径保留：`full_results` 从同一快照不复用缓存重算，`compare` 逐分区比较，
  `rebuild_full` 原子重建。

## 环境与验证

Python 3.11 以上，仅使用标准库，无第三方安装。`runtime.txt` 记录验证解释器版本。
数据库和对象目录均由入口参数指定。源码和合成数据可以直接复制到 Linux 环境运行。

```bash
python3 -m compileall -q warehouse tests scripts
python3 -m unittest discover -s tests -v
```

## 运行

```bash
python3 scripts/demo.py
```

分步调用：

```bash
python3 -m warehouse import --db /tmp/w.sqlite3 --input examples/source.json --batch first
python3 -m warehouse plan   --db /tmp/w.sqlite3 --plan examples/plans/demand.toml
python3 -m warehouse preview --db /tmp/w.sqlite3 --plan examples/plans/demand.toml
python3 -m warehouse apply   --db /tmp/w.sqlite3 --plan examples/plans/demand.toml
python3 -m warehouse recover --db /tmp/w.sqlite3 --plan examples/plans/demand.toml
python3 -m warehouse verify  --db /tmp/w.sqlite3 --plan examples/plans/demand.toml
python3 -m warehouse inspect --db /tmp/w.sqlite3
python3 -m warehouse export  --db /tmp/w.sqlite3 --objects /tmp/w-objects
```

- `plan` 输出核验后的节点声明与规则指纹。
- `preview` 在执行前给出每个节点将重算/复用/删除的分区、原因与扫描范围；
  不创建输出表、不写暂存，只读取业务与元数据。
- `apply` 执行增量物化。
- `recover` 先清理不可复用的暂存，再执行并复用仍然匹配的分区。
- `verify` 比较增量结果与全量重算，逐分区报告差异。

`scripts/demo.py` 打印实际命令和完整输出，验证首次写入、重复导入、查询、导出
往返，以及跨周变化、删除、规则改变、中断恢复、原子发布和增量等于全量。
小规模样例在数秒内结束，失败返回非零退出码，不回放预制日志。
