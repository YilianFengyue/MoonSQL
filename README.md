
```

```

下面给你一份**“渐进式全步骤路线图”**，严格对齐任务书要求（编译器四阶段 → 页式存储与缓存 → 执行器五算子 → 系统目录与持久化 → 交互与展示；最后是可选加分）。每一步都有**目标/产出/验收点/接口稳定点**，保证可以从最小闭环一路替换升级，直到覆盖全部评分点。参考依据：任务书对各模块与步骤的明确要求与示例用例，以及你给的最终方案文档中的目录与里程碑安排。 

------

# 渐进式 · 全步骤路线图（先不管人数）

## Phase 0｜项目骨架与基线闭环（D0）

**目标**：拉起最小可运行链路，后续只“替换实现”，不改接口。
 **产出**：项目目录、最小 CLI、占位 Planner/Executor/Storage 接口。
 **接口稳定点**：

- `Planner.plan(sql) -> Plan(JSON)`（错误用统一结构抛出/返回）。
- `StorageEngine.create_table / insert_row / seq_scan / delete_where`。
- `Executor.run(plan)`、`Executor.cursor(plan).fetchmany(n)`。 

------

## Phase A｜SQL 编译器（A1–A5）

### A1｜词法分析（Lexer）

- **目标**：输出四元式 `[TokenType, Lexeme, line, col]`，含非法字符提示。
- **产出**：`sql/lexer.py`，CLI `--show=token`。
- **验收**：对任务书示例 SQL 全量过，非法样例能定位行列。

### A2｜语法分析（Parser）

- **目标**：支持子集语法（`CREATE/INSERT/SELECT/DELETE`），生成 AST；出现错误要给出“期望符号”。
- **产出**：`sql/parser.py`，CLI `--show=ast`。
- **验收**：四类语句 + 缺分号/未闭合字符串等错误用例。

### A3｜语义分析（Semantic）

- **目标**：表/列存在性、类型一致、列数/列序校验；维护 Catalog（内存态）。
- **产出**：`sql/semantic.py`，CLI `--show=sem`。
- **验收**：类型不匹配、列名拼写错误等能报 `[错误类型, 位置, 原因]`。

### A4｜计划生成（Planner）

- **目标**：AST → Plan(JSON)，包含 `CreateTable / Insert / SeqScan / Filter / Project / Delete`。
- **产出**：`sql/planner.py`，CLI `--show=plan`。
- **验收**：示例 `SELECT id,name FROM student WHERE age>18` 生成三段式 `Project(Filter(SeqScan))`。

### A5｜编译器联测与负样例集

- **目标**：把 Token→AST→Semantic→Plan 串起来，形成“**四视图**”截图物料；整理一套负样例脚本可回放。
- **产出**：`cli/minidb_cli.py` 支持 `--show=*`；`tests/test_sql.py` + `tests/bad_cases/`。
- **验收**：任务书给定正确/错误用例全过，四视图一键展示。

------

## Phase B｜页式存储与缓存（B1–B4）

### B1｜Slotted Page（4KB 槽式页）

- **目标**：定长页头 + 槽目录 + 变长记录；`insert/read/delete/to_bytes/from_bytes`。
- **产出**：`storage/page.py`。
- **验收**：页大小固定、页编号唯一，插入/读取/逻辑删除正确。

### B2｜文件与序列化

- **目标**：表文件 `.tbl`、索引文件 `.idx`；行编解码（NULL 位图 + 列偏移 + 变长数据）。
- **产出**：`storage/file_manager.py`、`storage/serdes.py`。
- **验收**：冷启动后可还原；多行混合变长字段读写正确。

### B3｜缓冲池（BufferPool）与替换策略

- **目标**：LRU/FIFO 可切换；命中率统计与**页淘汰日志**。
- **产出**：`storage/buffer.py`，`BufferPool.stats()->{hits,misses,hit_ratio,evictions,policy}`。
- **验收**：可构造命中/淘汰场景并导出日志截图。

### B4｜接口整合与持久化冒烟

- **目标**：统一存储访问接口，供上层读取与刷盘；重启后数据不丢。
- **产出**：最小 `StorageEngine` 原型与冒烟脚本。
- **验收**：建表→插入→重启→查询，行数一致、校验和一致。

------

## Phase C｜执行引擎与系统目录（C1–C4）

### C1｜StorageEngine：表=页集合

- **目标**：`create_table/insert_row/seq_scan/delete_where`；RID = `(page_id, slot_id)`。
- **产出**：`engine/storage_engine.py`。
- **验收**：顺扫（SeqScan）能遍历整表，Delete 支持逻辑删或物理删。

### C2｜执行器（Executor）

- **目标**：解释 Plan(JSON) 并执行：`CreateTable / Insert / SeqScan / Filter / Project / Delete`。
- **产出**：`engine/executor.py`。
- **验收**：任务书四类语句端到端跑通，查询结果准确。

### C3｜系统目录（System Catalog）

- **目标**：把元数据落库（如 `sys_tables/sys_columns`），自身也通过页式存储管理。
- **产出**：`engine/catalo g_mgr.py`；启动时初始化/加载。
- **验收**：重启后可根据目录恢复表结构并继续插入/查询。

### C4｜CLI 四视图 + 结果展示

- **目标**：CLI 输入 SQL，顺序输出 Token/AST/Semantic/Plan/Result 或 Error。
- **产出**：增强 `cli/minidb_cli.py`，统一错误结构与行列定位。
- **验收**：形成“可截图”的评分物料。

------

## Phase D｜演示接口与（可选）网络客户端（D1–D3）

### D1｜游标与分页（可选但实用）

- **目标**：`Executor.cursor(plan).fetchmany(n)`；支撑大结果集按页取数与导出。
- **产出**：游标对象与 CLI 的 `FETCH n`。
- **验收**：百万级数据逐页输出不卡顿；接口稳定。

### D2｜3306 风格轻量协议与 Python 驱动（可选）

- **目标**：`AUTH/QUERY/PREPARE/FETCH/OK/ERR/RESULT` 帧；本地 TCP 服务。
- **产出**：`wire/server.py`、`wire/protocol.py`、`wire/driver_py/minidb.py`。
- **验收**：驱动 `connect/execute/fetchall/fetchmany` 打通；便于后续 WinUI3 或别的前端连接。

### D3｜WinUI3 微客户端（可选演示）

- **目标**：连接、执行 SQL、表格展示、导出 CSV；或以 Python 驱动做 HTTP 网桥替代。
- **验收**：形成演示录屏，作为展示加分物料。

------

## Phase E｜加分与纵深（E1–E5，按性价比分层推进）

### E1｜索引 v1：有序块 + 溢出页

- **目标**：等值/范围查询明显快于 SeqScan；接口与 B+ 树兼容。
- **产出**：`engine/index_ordered.py`；`index_scan(table,col,op,key)`。
- **验收**：基准对比图（行数/耗时）。

### E2｜索引 v2：B+ 树（叶子链）

- **目标**：替换 v1 不改上层接口；支持 `=` 与范围。
- **产出**：`engine/index_bpt.py`。
- **验收**：大数据量下范围查询曲线优于 v1。

### E3｜触发器（最小子集）

- **目标**：`AFTER/BEFORE × INSERT/DELETE`；写路径调用触发器管理器。
- **产出**：`engine/trigger_mgr.py` + 触发日志。
- **验收**：插入/删除能触发记录副作用。

### E4｜存储过程（最小子集）

- **目标**：`CREATE PROCEDURE p(a INT) BEGIN INSERT...; DELETE...; END;`（顺序执行 + 参数替换，无流程控制）。
- **产出**：`engine/proc_mgr.py`。
- **验收**：过程可记名保存与调用，结果与日志正确。

### E5｜轻量优化（Explain/谓词下推/投影下推）

- **目标**：`EXPLAIN` 展示计划树；将 `Filter/Project` 前推以减少 I/O。
- **验收**：同一查询前后对比 Plan 与耗时。

------

## 里程碑与验收清单（可直接照此准备“评分物料”）

1. **四视图**：Token/AST/Semantic/Plan 一屏展示；含正确与错误样例各若干。
2. **页式与缓存**：4KB 页结构图、命中率统计表、替换日志截屏。
3. **五算子跑通**：`CreateTable/Insert/SeqScan/Filter/Project` 的端到端结果截图。
4. **持久化**：重启后表与数据仍可查询的对照截图。
5. **（可选）索引对比**：SeqScan vs IndexScan 的耗时/行数柱状图。
6. **（可选）游标分页/客户端**：分页抓取或 WinUI3 表格与导出 CSV 的录屏。

------

## 最小到完整的“递进顺序”总表（可按天或按阶段推进）

1. 骨架与接口冻结（Phase 0）
2. Lexer → Parser → Semantic → Planner（A1–A4）
3. 编译器联测与负样例集（A5）
4. SlottedPage → FileManager/SerDes → BufferPool（B1–B3）
5. 存储接口整合与持久化冒烟（B4）
6. StorageEngine → Executor 五算子（C1–C2）
7. 系统目录持久化（C3）
8. CLI 四视图与结果统一展示（C4）
9. （可选）游标与分页（D1）
10. （可选）3306 风格协议 + Python 驱动（D2）
11. （可选）WinUI3 微客户端（D3）
12. （可选）索引 v1 → v2（E1–E2）
13. （可选）触发器 / 存储过程（E3–E4）
14. （可选）Explain + 谓词/投影下推（E5）

> 以上步骤完全覆盖任务书“编译器四阶段、页式存储与缓存、执行器与系统目录、交互与持久化验证”的硬性考核点，并提供可选扩展项的**渐进式**实现路径；接口先行、实现可替换，确保从 D0 起即可演示、逐日增强直到完整。 

如果你愿意，我可以把 **Phase 0–A1** 的最小代码骨架（文件清单 + 函数签名 + 假实现）直接给成“可运行”版本，后续你只需要替换具体实现即可。

```
MoonSQL
├─ data
├─ README.md
├─ src
│  ├─ cli
│  │  ├─ data
│  │  ├─ minidb_cli.py
│  │  └─ __init__.py
│  ├─ client
│  │  └─ __init__.py
│  ├─ engine
│  │  ├─ catalog_mgr.py
│  │  ├─ executor.py
│  │  ├─ interfaces.py
│  │  ├─ test_catalog_data
│  │  │  ├─ sys_columns.tbl
│  │  │  ├─ sys_indexes.tbl
│  │  │  ├─ sys_tables.tbl
│  │  │  └─ tables_metadata.json
│  │  ├─ test_catalog_persist
│  │  │  ├─ sys_columns.tbl
│  │  │  ├─ sys_indexes.tbl
│  │  │  ├─ sys_tables.tbl
│  │  │  └─ tables_metadata.json
│  │  ├─ test_complex_data
│  │  │  ├─ employees.tbl
│  │  │  └─ tables_metadata.json
│  │  ├─ test_executor_data
│  │  │  ├─ tables_metadata.json
│  │  │  └─ test_exec.tbl
│  │  └─ __init__.py
│  ├─ main.py
│  ├─ sql
│  │  ├─ lexer.py
│  │  ├─ parser.py
│  │  ├─ planner.py
│  │  ├─ semantic.py
│  │  └─ __init__.py
│  ├─ storage
│  │  ├─ buffer.py
│  │  ├─ file_manager.py
│  │  ├─ page.py
│  │  ├─ serdes.py
│  │  ├─ storage_engine.py
│  │  ├─ test_buffer_data
│  │  ├─ test_data
│  │  ├─ test_dirty_data
│  │  ├─ test_persistence_data
│  │  │  ├─ tables_metadata.json
│  │  │  └─ test_persistence.tbl
│  │  ├─ test_policy_data
│  │  ├─ test_storage_data
│  │  │  ├─ courses.tbl
│  │  │  ├─ students.tbl
│  │  │  └─ tables_metadata.json
│  │  └─ __init__.py
│  ├─ tests
│  │  ├─ bad_cases.py
│  │  ├─ test_sql.py
│  │  └─ __init__.py
│  ├─ wire
│  │  └─ __init__.py
│  └─ 任务计划书.md
└─ test_a1.sql

```