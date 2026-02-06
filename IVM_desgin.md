# 目标
## 总目标
实现对内表 OlapTable 的增量物化视图（IVM, Incremental Materialized View）的支持。

支持对基表的 DELETE、INSERT、UPDATE 操作的增量维护。

## 第一阶段目标

实现对基表的 INSERT only 操作的增量维护。

# 前置需求
## OlapTable 读取多版本

### 目标
给定一个 OlapTable，维护 base_version 和 head_version。
- FE metadata 可以读取 `[base_version, head_version]` 每个版本的元数据，用于查询。
- BE 上，>=base_version 的版本数据都保留在 BE 上，供查询使用，不能被 compaction 掉。

为了测试方便，增加几个命令：
- ALTER TABLE <table_name> SET BASE_VERSION = x
- SQL 中 `FROM table_name VERSION(x)` 来指定版本读取。
- SHOW VERSIONS FROM table_name 来显示所有版本信息。

### 已实现的 base_version 相关能力
目前已落地的命令/能力（按当前实现范围）：

1. **设置 base_version**  
   `ALTER TABLE <table_name> SET ("base_version" = "<x>")`

2. **指定读取版本**  
   `SELECT ... FROM <table_name> VERSION(x)`  
   FE 做范围校验并下传到 BE。

3. **查看版本信息**  
   `SHOW VERSIONS FROM <table_name>`  
   输出 `DbName / TableName / PartitionName / PhysicalPartitionId / VisibleVersion / BaseVersion`。

同时，BE 侧的版本保留策略已覆盖：
- **Primary/Unique Key（updatable）表**：`TabletUpdates::remove_expired_versions` 按 base_version 保留。
- **Duplicate Key 表**：`Tablet::delete_expired_stale_rowset` 加入 base_version 保留逻辑。
- **Lake（存算分离）表**：FE 在 vacuum 请求的 `retain_versions` 中加入 base_version，避免 vacuum 删除。


### 简易支持
**如果只是“走通流程”的 query-only IVM**，最小化可行条件是：

1. **禁用迁移/重平衡**  
   让 tablet 不变动位置，避免“新位置只有最新版本”的问题。

2. **禁用旧版本清理**  
   让 `>= base_version` 的版本一直保留在 BE。

3. FE 只做：  
   - `base_version` 存储/设置  
   - `VERSION(x)` 语法 + 下传版本号  
   - 基本范围校验（`x <= visibleVersion`、`x >= base_version`）

如果你确认这是一个**受控测试环境**，这条路线是合理的。

你要我帮你确认具体哪些 BE 配置可以关掉迁移/清理吗？

### 关闭负载均衡的配置（建议组合）
如需在测试环境中**尽可能关闭负载均衡/迁移**，建议同时设置：

1. `tablet_sched_disable_balance=true`  
   关闭 FE TabletScheduler 的 balance 调度，同时同步到 StarMgr 关闭后台 shard 负载均衡检查。

2. `tablet_sched_disable_colocate_balance=true`  
   关闭 colocate 表的自动平衡。

3. `lake_enable_balance_tablets_between_workers=false`  
   关闭存算分离（Lake）场景下的 worker 间 tablet 平衡。

说明：
- 上述配置主要关闭“均衡/迁移”类行为，**不等同于**关闭异常修复类调度（如副本丢失修复）。
- 如果你还希望禁用异常修复调度，需要再明确范围与目标行为。


## 读取基表 Changes

### 目标

给定两个 version v1 和 v2，能够读取 OlapTable 在这两个版本之间的变化（changes）。

返回格式中，增加一列 `action` 类型为 SMALLINT，值为 +1 和 -1，分别表示 INSERT 和 DELETE。UPDATE 拆分为 DELETE + INSERT。

为了测试方便，增加一个命令：
- `SELECT ... FROM table_name CHANGES BETWEEN v1 AND v2` 来指定版本范围读取变化。

支持范围
- Duplicate Key 表：只支持 INSERT。
- Primary Key 表：支持 INSERT、DELETE、UPDATE（拆分为 DELETE + INSERT）。
