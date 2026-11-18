# 海外站点访问明细仓

这是用于本地数据接收、事实核对和列式交换的仓库服务。数据使用 DuckDB 持久化，导出为 Parquet；不需要远程数据库或运行期网络。示例数据仅为本地合成的业务记录。

## 现有业务

站点具有 IANA 时区，事件标识与匿名访客标识均在站点内有效。明细读取按事件时刻排序，每日访问数按站点当地日期计算。事件同时保存发生与收到时间，保留跨日期明细；原有 `events.timeline` 与明细查询保持不变。

`daily.refresh_days` 刷新按当地日期保存的日集市，写入所用事件摘要；`daily.read_days` 读取当前日集市。

## 会话事实

同一访客（匿名标识仅在所属站点内有效）在一次访问中的连续事件构成会话：相邻事件间隔不超过 30 分钟属于同一会话。判定只使用事件时刻，不按自然日切分，因此跨午夜的一次访问仍是同一会话；同一时刻的多条事件只增加事件数，不增加会话时长。会话入口页取该会话中最早的有效事件。

`warehouse.sessions` 在保留 `events` 明细的基础上生产并查询会话事实：

- `sessions`：每个会话一行，`session_id` 由会话最早事件派生（`s-<event_id>`），按会话开始当地日期保存 `start_day`，并保存时长、事件数与入口页；
- `site_days`：日集市同时保存按会话开始当地日期统计的 `sessions`，以及按事件发生当地日期统计的访问数 `hits`/`visits` 与访客数，单次刷新保证明细与集市一致；
- `event_dispositions`：每条原始事件的去向，包括超出补传窗口（发生时刻早于收到时刻 48 小时）而被排除的事件及原因；
- `fact_restatements`：已发布日期被重述时的前后值与触发来源。

会话标识稳定且可重定位。补传记录把原先两个会话连成一个时，较早会话的标识保留，较晚标识通过事件归属解析到当前会话；`resolve_session`、`read_session` 与 `session_members` 按站点接受新旧任一引用，因此此前暴露过的引用仍能找到当前归属，原先多记的开始日会话数也随刷新消除。

补传窗口按每条事件自身的发生与收到时刻判定，不随最新到达时间漂移，因此从原始记录重建与增量处理得到完全一致的结果。超过窗口的事件不会被静默丢弃：原始明细仍可查询，去向表给出状态与原因，也不会改变已冻结日期的集市；本地日在其结束后满 48 小时即视为冻结，若确有更正需要，可显式重述并留下审计记录。

```python
from warehouse import sessions
sessions.refresh_facts(connection, 'paris')          # 增量刷新一个站点
sessions.rebuild_facts(connection)                   # 从原始记录重建
sessions.read_sessions(connection, 'paris', visitor='anon-1')
sessions.read_session(connection, 'paris', 's-e1')   # 旧引用同样可用
sessions.read_day_facts(connection, 'paris')
sessions.read_dispositions(connection, 'paris', 'excluded')
sessions.reconcile(connection, 'paris')              # 明细、去向、集市是否平衡
```

`reconcile` 会核对原始事件是否恰好被"纳入 + 排除及原因"覆盖，并核对集市访问数与会话成员是否一致。

## 使用

运行时支持 Python 3.11.x，独立容器使用摘要固定的 Python 3.11 镜像。`requirements.txt` 精确锁定依赖版本，`requirements.lock` 保存相同版本的 Linux CPython 3.11 wheel 哈希，容器构建时校验哈希。推荐使用下列容器方式；镜像构建时准备依赖，之后运行不联网。镜像只包含本仓库应用及依赖，不启动额外服务。

```bash
docker build -t sessions-warehouse .
docker run --rm --network none sessions-warehouse
```

已经准备依赖的环境中也可以直接执行：

```bash
python -m unittest discover -s tests -v
python -m compileall -q warehouse tests
python -m warehouse.cli --database /tmp/sessions.duckdb load samples/delivery.json
python -m warehouse.cli --database /tmp/sessions.duckdb overview
python -m warehouse.cli --database /tmp/sessions.duckdb sessions paris
python -m warehouse.cli --database /tmp/sessions.duckdb days paris
python -m warehouse.cli --database /tmp/sessions.duckdb reconcile paris
python -m warehouse.cli --database /tmp/sessions.duckdb rebuild
python -m warehouse.cli --database /tmp/sessions.duckdb export /tmp/sessions-exports release1
```

接收事务内同时刷新受影响站点的会话事实；`sessions`、`session`、`days`、`dispositions`、`restatements` 与 `reconcile` 用于查询，`rebuild` 从原始记录重建全部站点的事实。

上面的导出版本不可覆盖；再次核对应使用新的数据库路径和新的导出目录，或直接运行使用临时目录的测试。测试同时验证接收事务、业务主键冲突、批号重送、持久化及列式文件回读。完整测试和样本运行通常数秒结束，没有等待外部服务的步骤。

## 数据与接口

构建所需工具和传递依赖同样精确锁定，并包含在容器依赖中。执行 `python -m build --wheel --no-isolation` 可在不下载额外构建依赖的情况下生成 wheel；SQL 表定义随包分发。

接收包包含 `batch_id` 和 `tables`。合法字段由各表契约与领域校验共同约束；同批号同内容重送不重复写入，同业务键异内容拒绝整个接收事务。已有原始行和接收回执保留，业务值更正须采用领域中允许的独立记录，不能覆写原行。

`warehouse.storage.open_store` 打开仓库，`warehouse.ingest.ingest` 接收业务包，`warehouse.queries` 提供现有事实查询。`warehouse.export.export_tables` 固定一次读取视图并生成版本目录，`read_export` 校验成员文件摘要后读取。`samples/delivery.json` 展示各表的当前字段和合法数据。

领域校验、事实计算与查询分别存在于同一应用的不同职责中。SQL 定义保存当前存储契约；批号回执与原始行登记属于接收事务，不是业务汇总来源。文件导出只交换现有业务表，接收回执不会充当事实行。
