# Issue #1：Paimon Pipeline Connector 兼容性研究

核查日期：2026-09-17。范围为官方发布源码和官方版本文档；未编译适配产物、启动 Flink 或访问 TOS。本文件只记录证据及实施建议，不代表兼容性验收通过。

## 结论

**用户已确定沿用 Yak 现有 Flink CDC 3.6.0 YAML Pipeline，目标运行基线为 Flink 1.20.5 / Paimon 2.0.0，使用 filesystem Catalog 和标准 S3 接口接入 TOS。** 本 Issue 按该路线实施，具体产品行为和任务拆分以 [实现与验收规格](../architecture/PAIMON_REALTIME_SYNC_SPEC.md) 为准。

CDC 3.6.0 官方 Paimon Pipeline Connector 实际依赖 Paimon 1.3.1，并将 Paimon 打包进自身 JAR；原版产物不能直接认定为 Paimon 2.0.0 组合。[S1] 将依赖升级、实际 API 差异修复、定制产物构建和运行验证列为实施任务，遇到问题依据证据解决。不能通过旁挂 2.0.0 JAR 宣布升级成功。下文 Action 内容保留为官网同步机制的研究背景，不作为本 Issue 的候选执行路线。

Flink 1.20 属于 CDC 3.6 的目标运行分支；CDC release-3.6.0 默认编译基线是 Flink 1.20.3，并使用 `paimon.flink.major.version=1.20`。因此 1.20.5 是同 minor 的目标补丁版本，但本次没有进行该精确版本的运行验证。[S2] Paimon 2.0 官方列出 Flink 1.20 对应产物 `paimon-flink-1.20-2.0.0.jar`，这只能证明 Paimon 自身提供该 Flink 适配，不能替代 CDC Connector 的适配证明。[S3]

## 依赖、打包与可执行路径

| 证据 | 含义 |
| --- | --- |
| Connector POM 的 `paimon.version` 为 `1.3.1` | 原版 CDC 3.6.0 不满足目标 Paimon 2.0.0 |
| 依赖 `paimon-flink-${paimon.flink.major.version}` | Flink 1.20 路线使用 Paimon Flink 1.20 API |
| shade 的 include 为 `org.apache.paimon:*` | Paimon 实现进入连接器产物 |
| relocation 配置只移动 Kafka 包，不移动 Paimon 包 | 再放入 Paimon 2.0 JAR 可能产生同名类重复与加载顺序问题 |

以上来自固定标签 POM。[S1][S2] “重复类风险”是基于打包配置的工程推断，本次没有下载发布二进制进行逐类清单核对。

实施时先定位 Yak 当前使用的 CDC 源码/产物归属，固定其 commit 并保留既有定制；CDC `release-3.6.0` 的固定 commit 用于上游对照。在 Flink 1.20 构建配置下升级 Paimon 依赖至 2.0.0，处理实际 API 差异，运行连接器单测、集成测试，再在 Flink 1.20.5 上做单表初始快照、增删改、DDL 和故障恢复验证。产物使用明确的定制标识，保留补丁、构建命令、依赖树、SHA-256 与运行时版本证据。不能仅改变 Maven 属性便认定兼容。这些工作属于已选 Pipeline 路线的实现和验收，不再作为重新选型的前置条件。

Paimon Action 与 CDC YAML Pipeline 是不同入口。本 Issue 延续 Yak Ops 当前 Pipeline 接入方式，以 Pipeline Connector 发布 POM 和实现为依据。**Paimon Action 文档示例中的 MySQL CDC JAR 版本或最低版本，不是 Pipeline Connector 兼容矩阵**。[S1][S4]

## Paimon 2.0 官网的数据同步路线（研究背景）

官网提供 `paimon-flink-action-2.0.0.jar` 中的 `mysql_sync_table` 和 `mysql_sync_database`，可直接 `flink run` 或通过 DataStream API 的 `MySqlSyncTableAction` / `MySqlSyncDatabaseAction` 使用。这条路线不需要 CDC Paimon Pipeline Connector，因而绕开其内置 Paimon 1.3.1 的问题。[S7]

| 方面 | Paimon 原生 Action | CDC Pipeline Sink |
| --- | --- | --- |
| MySQL 读取 | CDC source connector | CDC Pipeline MySQL connector |
| Paimon 写入 | Paimon 2.0 Action / bundled 实现 | CDC 3.6 connector 原包内置 Paimon 1.3.1 |
| 单表与整库 | mysql_sync_table / mysql_sync_database | YAML source/sink/route |
| Yak Ops 接入 | 需要新增 engine adapter / 提交参数编译 | 较接近现有编译链，需适配 Paimon 2.0 |
| 兼容性关注点 | Action + source CDC 3.6.0 的精确兼容性与产品策略映射 | 定制 connector 的构建及运行兼容性 |

Paimon 2.0 官网页面展示 CDC 3.5.0 的 MySQL SQL bundled JAR、MySQL JDBC driver，并要求 CDC 3.5.0 及以上。固定 `release-2.0.0` 的 `paimon-flink-cdc/pom.xml` 更具体：Flink 编译基线为 1.20.4，CDC 为 3.5.0；`flink-connector-mysql-cdc` 是 provided，另依赖 CDC common/runtime。[S7][S8] 因此 CDC 3.6.0 满足文档最低版本条件，但仍不是该固定 tag 的编译基线；本次未证明替换为 source connector 3.6.0 后所有 API / classpath 均兼容。

官网说明 Action 在目标表不存在时自动建表，已有时比较 schema；整库模式只同步有主键表，还提供过滤、表名前后缀与分片合并选项。不能据此推断它与 Pipeline 的任意 route、DDL FAIL / EVOLVE 策略完全等价，也不能推断已有表重跑不会残留历史行。[S7]

**本 Issue 已选定 CDC 3.6.0 YAML Pipeline，不新增 Action engine adapter，也不安排 Action 路线验证任务。** Action 原始 CLI 示例将数据库密码放入参数，其示例不能直接作为 Yak Ops 的安全提交方案；Pipeline 实现仍需保护 argv、进程列表、日志和任务快照中的敏感信息。

Paimon 2.0 CDC Ingestion 官网明确：DROP COLUMN / RENAME TABLE 会被忽略，RENAME COLUMN 按新增列处理，支持新增列及部分类型扩宽。它还说明已有表可能依据 `table_conf` 修改可变属性。[S9] 这些与“禁止 DDL 必须停止”的产品策略不等价，不能据 Action 文档承诺 Pipeline 的 FAIL 行为。此处依据版本官网，尚未完成固定 tag 的 DDL 逐方法实现核验和实测。后文 Schema / at-least-once 结论仅适用于 Pipeline，不能自动套到 Action。

## 主键、交付语义和建表

固定 release-3.6.0 文档明确只支持主键表，并将交付语义描述为 at-least-once 加主键表幂等写入，不能对用户承诺 exactly-once。[S4]

固定标签 `PaimonMetadataApplier.applyCreateTable` 会按目标 schema 名创建数据库，并调用 `createTable(..., true)`；已有表忽略创建不等于已有表结构通过校验。该方法还把不在主键内的分区列加入主键集合。[S5]

对 Yak Ops 的建议属于产品约束：源表必须有完整物理主键；分区键限定为源主键子集；新任务默认拒绝已有目标表；独占目标表、单写入者、使用去重合并行为。自动故障恢复访问本作业已经创建的表，与平台发起新 Execution 是两回事。仅凭上述上游建表实现，不能保证从头全量重跑会清除目标残留数据，也不能保证平台 Restart 已接通 checkpoint/savepoint。

## Schema Evolution 边界

固定标签 `getSupportedSchemaEvolutionTypes()` 列出 CREATE_TABLE、ADD_COLUMN、DROP_COLUMN、RENAME_COLUMN、ALTER_COLUMN_TYPE。实现虽也包含 drop-table、truncate-table、alter-comment 方法，但这些未列入该支持集合，不能仅据方法存在便宣传完整支持。[S5]

源码还显示：新增已存在列会警告后跳过；删除不存在列/表可能警告后跳过。由此可见，笼统启用上游 evolve 并不等于“所有异常都失败”。类型变化调用 Paimon 的 schema API，列入事件类型不等于任意类型转换都受支持。[S5]

实施规格确定首个单表切片默认 FAIL，后续任务 06 完成有限 EVOLVE：可空、无默认值、非主键新增列和非主键 INT → BIGINT。运行期拒绝其他 DDL 必须通过真实事件链路测试，包括拒绝发生在目标变更之前。发布前校验和勾选允许事件类型都不能单独证明该约束。事件链路存在缺口时由实现任务补齐，保持任务未完成，不能以 FAIL-only 替代整个 Issue 的有限 EVOLVE 验收。此处是产品范围，不是对上游能力的完整描述。

## 标准 S3 与火山云 TOS

Paimon 2.0 官方 S3 文档支持 `s3://`、自定义 endpoint、AK/SK 和可选 path-style；推荐的插件为 `paimon-s3-2.0.0.jar`，在 Flink 场景放入 `lib`。[S6] 本 Issue 首期使用该插件验证标准 S3 接入。官网还提供复用 Flink S3 文件系统的配置方式，但首期不混合多套文件系统，以便核对实际加载的实现。

Pipeline 透传 Catalog 参数使用 `catalog.properties.*`，表属性使用 `table.properties.*`。[S4] 因而用户配置应映射为以下非敏感部分：

```yaml
sink:
  type: paimon
  catalog.properties.metastore: filesystem
  catalog.properties.warehouse: s3://emr-olap-starrocks-20241125160240/paimon-2.0
  catalog.properties.s3.endpoint: https://tos-s3-cn-beijing.ivolces.com
```

AK/SK 由既有安全凭据机制在提交边界注入，不能写入版本化任务定义。`work_temp` 是目标数据库路由，不是 warehouse 或 Catalog 名。SQL Client 的 `batch`、`tableau` 不进入实时同步配置。

TOS 通过 S3 兼容接口接入符合这条实现路线；但本次未查证该账户具体权限、region/签名要求、寻址方式或执行节点网络，因此只能认定设计方向成立，不能认定该 endpoint 已读写成功。path-style 保持可配置，不强制设为 true。`paimon-s3` 与最终打包的 Paimon 版本保持一致，提交客户端、JobManager、TaskManager 均需要正确依赖和访问能力。[S6]

## Pipeline 实现的验收门槛

1. 基于 CDC 3.6.0 适配 Paimon 2.0.0 的定制连接器编译及相关测试通过，并在 Flink 1.20.5 上验证；记录 Paimon/Flink/CDC 版本和完整 classpath，排除旧版重复类。
2. 在独立测试库表验证全量追平、INSERT/UPDATE/DELETE、复合主键及主键值更新；对账以目标已提交快照的主键集合和字段值为准。
3. 验证 checkpoint 前后故障与重放，包含删除不复活、无 checkpoint 时不静默全量覆盖已有表；不以 RUNNING、计数相等或 checkpoint 成功代替数据正确性。
4. 验证声明支持及禁止的 DDL，确认限制在实际运行中生效。
5. 验证 TOS 读写、权限失败、网络失败和依赖缺失；只读连接测试不能证明 Flink 集群写权限。

以上均为后续验收项。本次没有真实 AK/SK，也未执行任何建表、对象写入、恢复或部署。

## 官方来源

- [S1：CDC release-3.6.0 Paimon Connector POM](https://github.com/apache/flink-cdc/blob/release-3.6.0/flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-paimon/pom.xml)
- [S2：CDC release-3.6.0 根 POM](https://github.com/apache/flink-cdc/blob/release-3.6.0/pom.xml)
- [S3：Paimon 2.0 Flink Quick Start](https://paimon.apache.org/docs/2.0/flink/quick-start/)
- [S4：CDC release-3.6.0 Paimon Pipeline 文档源码](https://github.com/apache/flink-cdc/blob/release-3.6.0/docs/content/docs/connectors/pipeline-connectors/paimon.md)
- [S5：CDC release-3.6.0 PaimonMetadataApplier](https://github.com/apache/flink-cdc/blob/release-3.6.0/flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-paimon/src/main/java/org/apache/flink/cdc/connectors/paimon/sink/PaimonMetadataApplier.java)
- [S6：Paimon 2.0 Filesystems / S3](https://paimon.apache.org/docs/2.0/maintenance/filesystems/#s3)
- [S7：Paimon 2.0 MySQL CDC / Action](https://paimon.apache.org/docs/2.0/cdc-ingestion/mysql-cdc/)
- [S8：Paimon release-2.0.0 Flink CDC 模块 POM](https://github.com/apache/paimon/blob/release-2.0.0/paimon-flink/paimon-flink-cdc/pom.xml)
- [S9：Paimon 2.0 CDC Ingestion / Schema Change Evolution](https://paimon.apache.org/docs/2.0/cdc-ingestion/)

CDC 证据固定发布标签；Paimon 使用 2.0 版本官方文档（版本分支文档仍可能补充更新），并明确区分文件系统与 Pipeline 两条证据链。
