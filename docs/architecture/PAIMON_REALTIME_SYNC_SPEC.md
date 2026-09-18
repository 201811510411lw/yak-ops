# Issue #1：Paimon 实时同步实现规格

状态：实施基线；2026-09-17。用户已确定 Yak Flink CDC 3.6.0 YAML Pipeline 路线。任务已拆分，尚未实现、构建连接器或完成 TOS 联调。

需求来源：[Issue #1](https://github.com/201811510411lw/yak-ops/issues/1)。版本、存储和执行路线已经用户确认；本文定义首期实施范围，并非现有产品能力清单。执行入口见[任务索引](../paimon-realtime/README.md)。

## 问题与解决方案

用户需要在 Yak Ops 配置、发布和运行 MySQL → Paimon 入湖任务，并在 TOS 上核对全量及增量结果。现有实时目标端以 JDBC 为前提，不能只增加下拉选项实现该功能。扩展非 JDBC 数据源与同步模型，继续使用现有 CDC Pipeline 提交、不可变版本和执行生命周期，版本差异作为实现问题处理。

## 用户故事

1. 作为数据工程师，我希望配置标准 S3 endpoint 和 warehouse，以便连接 TOS 或其他兼容存储。
2. 作为数据源管理员，我希望安全保存和编辑 AK/SK，以便轮换凭据而不泄露密钥。
3. 作为操作者，我希望只读测试连接并浏览库表，以便检查元数据访问权限。
4. 作为任务作者，我希望通过向导或 Yak YAML 配置同一任务，以便按习惯编辑且结果一致。
5. 作为任务作者，我希望把有主键的 MySQL 表映射到新的 Paimon 表，以便保留更新和删除语义。
6. 作为任务作者，我希望发布不可变版本，以便修改草稿时不影响运行实例。
7. 作为操作者，我希望启动任务后自动建表并持续同步，以便核对全量及增量数据。
8. 作为任务作者，我希望配置多表一对一路由、分区和 bucket，以便组织独立的目标表。
9. 作为运维人员，我希望 LOCAL 和 SSH 环境都能提交并查看状态，以便复用现有运行环境。
10. 作为任务作者，我希望明确选择遇到 DDL 失败或有限自动演进，以便控制表结构变化。
11. 作为运维人员，我希望依赖缺失、权限问题和配置错误有明确提示，以便修复后重试。
12. 作为操作者，我希望故障恢复和提交超时不会造成重复任务或目标数据错误，以便可靠处理异常。
13. 作为操作者，我希望不支持的 Restart/Apply 在停止健康实例前被拒绝，以便避免误操作。
14. 作为验收人员，我希望按主键和字段核对目标快照，以便确认正确性而非仅看到 RUNNING。

## 1. 已确定的基线与前置条件

| 项目 | 约定 |
| --- | --- |
| Flink | 1.20.5 |
| Flink CDC | 3.6.0，使用 Flink 1.20 构建产物 |
| Paimon | 2.0.0 |
| 接入方式 | MySQL Source → Yak 使用的 Flink CDC 3.6.0 YAML Pipeline → Paimon Sink |
| Catalog | filesystem；不依赖 Hive Metastore |
| 存储 | 标准 S3 兼容接口，首个验收环境为火山云 TOS |
| Warehouse | `s3://emr-olap-starrocks-20241125160240/paimon-2.0`，连接级可配置，不写死 |
| Endpoint | `https://tos-s3-cn-beijing.ivolces.com`，连接级可配置，不写死 |
| 目标库 | 用户示例为 `work_temp`；任务路由可配置 |

**需要实现的版本适配：CDC release-3.6.0 的官方 Paimon Pipeline Connector 依赖并打包 Paimon 1.3.1。** 因此，上表是目标组合，不是已验证可用的官方现成组合。不能把额外挂载 `paimon-flink-1.20-2.0.0.jar` 当作连接器升级。

沿用 Pipeline：基于 CDC `release-3.6.0` 的 Flink 1.20 构建分支/配置，重编译适配 Paimon 2.0.0 的连接器；记录源码 commit、补丁、构建命令、依赖树和产物 SHA-256。API 变化必须用源码适配和测试解决，不能只修改版本号后宣布兼容。S3 实现采用匹配 Paimon 2.0.0 的依赖，检查完整运行 classpath 无旧版 Paimon 重复类。

这个连接器产物是 Yak Ops 的外部运行时依赖；适配和可复现构建属于任务 01，控制面仍不自动构建、安装或管理连接器。实现时先定位 Yak 当前使用的 CDC 源码/产物归属，在其 3.6.0 基线上保留既有定制并适配；上游 release 标签是证据基准，不是覆盖 Yak 定制的理由。编译、API 或运行问题在该任务内根据证据修复，不因此反复重开选型；未解决的失败记录为未完成，不降级 Paimon 版本。详见[兼容性研究](../research/issue-1-paimon-compatibility.md)。

### 执行路线决定

使用现有 `flink-cdc.sh` Pipeline 提交链路，Paimon Action 仅保留在研究文档作为对照，不新增 Action runner。下文所有 Schema、路由、提交和恢复要求均按 Pipeline 实施。

## 2. 首期范围

- 支持 Paimon 数据源配置保存、编辑、脱敏回显、连接测试及库表元数据读取。
- 支持 MySQL 有主键表的全量加增量同步，启动模式首期限定 `initial`。
- 支持一张或多张源表映射到各自独立的 Paimon 主键表；目标使用明确的 `database.table`。
- 复用任务定义、发布、LOCAL/SSH 提交、执行状态、Reconcile、日志和 Metrics。
- 首期选择明确表名；Paimon 路由拒绝正则动态发现、多对一合表以及同任务内重复目标表。现有 JDBC 路由能力保持原状。
- 不提供 Paimon Source、离线同步、通用 Paimon SQL 编辑器、Hive Catalog、TOS 专用 SDK、集群管理或自动安装连接器。
- SQL Client 的 `tableau` 和 `batch` 设置不是任务配置；实时任务使用 streaming 语义。

## 3. 数据源、凭据和连接测试

连接模型拥有 `catalogType=filesystem`、`warehouse`、`endpoint`、可选 region、可选 path-style、默认数据库以及 AK/SK。参数名称在适配器中映射为所选 Paimon 版本的真实 S3 参数；不强制所有 S3 服务启用 path-style。

- Warehouse 必须是含 bucket 的 `s3://` URI；endpoint 必须是合法 HTTP(S) URL，禁止内嵌 userinfo/凭据。TOS 示例使用 HTTPS。
- 不提供任意连接 JSON 透传入口；可扩展参数需要声明类型、敏感性和允许范围。
- AK/SK 均按敏感字段管理，复用 datasource Descriptor-aware secret codec；编辑回显掩码、未修改凭据复用、持久化加密行为均需测试。
- SyncDefinition 只引用数据源身份及非敏感同步语义；DefinitionVersion、执行快照、摘要、日志不得保存 AK/SK 或完整连接 JSON。
- 凭据仅在连接测试、元数据读取、提交边界按需解析；提交后释放。LOCAL 临时 YAML 和 SSH stdin/临时文件使用既有安全生命周期，失败路径同样清理。

连接测试为只读 Catalog/元数据访问，不创建库表或对象。结果必须注明检查来自 Yak Ops 节点，以及检查到的是读权限；warehouse 为空不应仅因无数据库判定失败。写权限和 Flink 节点访问能力由独立联调验证，不把连接测试成功表述为完整运行就绪。

保存失败/连通失败的状态转换遵循 datasource DOMAIN：配置校验失败不覆盖已有状态；未保存连接测试不持久化状态。错误区分参数、认证、权限、网络、Catalog 元数据和依赖缺失，并统一脱敏。

## 4. 表映射、建表和数据行为

| 场景 | 首期约定 |
| --- | --- |
| 源表没有物理主键 | 发布/启动前拒绝；不通过手填 ReplayKey 宣称具备更新删除能力 |
| 源表主键 | 保留完整复合主键；ReplayKey 必须与之匹配 |
| 目标库表不存在 | 在实际任务接收到建表事件时由 connector 创建；保存、预览和连接测试不创建 |
| 目标表已存在 | 首期拒绝将新任务挂接到已有表；不覆盖、不清空、不声称能与外部写入安全共存 |
| Flink 同一作业故障恢复 | 允许访问其已创建目标表，并按 checkpoint 状态恢复 |
| 新 Execution 再次启动/平台 Restart/Apply | 没有已验证的状态恢复路径时按已有目标表处理并拒绝，不能误走全量覆盖 |
| 多个源表映射到同一目标 | 首期拒绝，避免合表主键冲突及 schema 合并语义扩张 |
| 增量 INSERT / UPDATE / DELETE | 核对目标行内容及删除结果，包含非主键更新、主键更新和复合主键 |

连接器初始化行为必须在 G0 验证中确认能遵守上述要求。启动前检查目标是否存在，并说明检查与外部建表之间不能形成跨系统原子锁；验收库要求独占目标表名和单写入任务，不将平台检查声称为跨应用排他锁。

首期表属性采用窄范围：固定正整数 bucket 数、可选分区键（必须包含在主键中），由向导和 Yak YAML 表达相同含义。merge engine 固定为主键去重所需行为；不暴露 sum/aggregation 等会改变重放语义的模式。分区/主键/bucket 在运行后不通过 Apply 自动变更。其他 Paimon 表属性只有在连接器及验收覆盖后才开放；未知或不适用字段明确拒绝，不静默忽略。

上述“仅新目标”和路由限制是保守的首期范围。若实际业务必须接已有 `work_temp` 表或做分表合并，需要扩大本节及恢复验收后再实现，不能隐式放开。

## 5. Schema Evolution

首期默认 FAIL（编译为 CDC `exception`）；自动创建初始表不算运行中的 DDL 变更。EVOLVE 在任务 06 证明具体事件链路正确后开放；Paimon 首期拒绝 IGNORE，以免把 schema 未应用当作成功。

| 事件 | FAIL | EVOLVE 首期目标 |
| --- | --- | --- |
| 初始建表 | 允许 | 允许 |
| 增加可空、无默认值、非主键列 | 停止并显示失败 | 必须支持；验证旧行、新行及恢复后的值 |
| 非主键列 `INT → BIGINT` | 停止并显示失败 | 必须支持；边界值和恢复后继续写入必须正确 |
| 删除列、列改名、其他类型变化 | 停止并显示失败 | 首期拒绝，不忽略 |
| 主键/分区键变更、删表、TRUNCATE | 停止并显示失败 | 首期拒绝，不传播破坏操作 |

这里的 EVOLVE 是产品允许范围，不等于上游全部能力。对运行中新发生的禁止事件，必须证明实际执行路径会失败且不会先在目标执行 DDL；只做发布前校验不足以实现该约定。若 connector 没有合适事件能力过滤/失败机制，任务 06 负责补适配并验证。单表首个切片只开放 FAIL；完整 Issue 交付要求任务 06 的有限 EVOLVE 通过验收。实现不了时记录具体阻塞，不以静默缩减为 FAIL-only 关闭 Issue；不能把不符合此表的 upstream `evolve` 直接暴露为本产品能力。

DDL 失败后 UI 显示真实任务异常；不自动删除目标数据或从头初始化。失败修复需要明确恢复位置和表结构对齐方案。

## 6. 交付语义与恢复

承诺边界为 **at-least-once + Paimon 主键表幂等写入**，不宣称 exactly-once。幂等结果受主键、去重合并方式、事件顺序和单写入者约束；Checkpoint 成功不能替代目标数据核对。

- Flink 同一作业的 TaskManager/进程故障恢复：使用可用 checkpoint 重放，并验证目标最终状态无缺失、无多余主键、更新和删除不丢失。
- Yak Ops 提交响应丢失：保留现有 runtime identity 和 UNKNOWN/CONFLICT Reconcile，禁止重复提交第二个作业。
- 平台 Restart/Apply：沿用“新 Execution”定义，但不等同于从 checkpoint/savepoint 恢复。当前 runner 未提供已验证的 savepoint 恢复链路，首期不可对此作承诺。
- 初次任务无 checkpoint 且已部分写入目标时，不自动从头写已有表；返回明确的人工恢复要求。
- 验收环境必须实际开启 checkpoint 和 Flink 故障恢复策略，并记录有效运行配置；平台接受的策略必须真正生效或明确拒绝，不能顺手假装修复已知 ExecutionPolicy gap。
- source binlog 已过期、checkpoint 不可用、权限撤销等情形均明确失败，不降级为无提示全量重建。

## 7. 模型与工程边界

Domain Impact Analysis：

- Aggregate：DataSourceDefinition/ConnectionProfile、SyncDefinition、DefinitionVersion；SyncExecution 保持现有生命周期。
- Domain Gap：现有 SinkWritePolicy 和连接解析含 JDBC 假设，无法直接表达非 JDBC Sink；需明确区分通用写入语义与目标专属配置。
- Core Domain 不引入 Paimon SDK、S3 凭据、Flink YAML 或 raw SPI；库表映射、主键与允许的目标写入策略属于领域值对象。
- 不新增第二份可编辑 Paimon Task/Spec。Wizard、Yak YAML、DTO、持久化兼容映射都映射至同一 SyncDefinition；digest 必须包含新增影响行为的字段。

Architecture / Dependency Impact Analysis：

- datasource plugin：新增非 JDBC Paimon 连接和 Catalog 能力，不伪造 JDBC URL/driver；未提供的 SQL 执行能力不得声明。
- datasource gateway/adapter：负责 SDK/SPI 和业务模型转换、安全凭据管理；必要 SPI 扩展保留 JDBC 插件兼容，不能藏进未声明 Map key。
- realtime engine：扩展 ResolvedCdcPipeline 的目标类型、RealtimeDataSourceResolver、PipelineYamlCompiler、CredentialBindings 及能力检查。Paimon 分支生成 `type: paimon`、`catalog.properties.*`、允许的 `table.properties.*` 和 route；不混入 JDBC batch/statement cache 参数。
- environment：区分声明能力与探测证据；检查 MySQL/Paimon connector、Paimon/S3 版本及 LOCAL/SSH 提交节点的产物，记录可核查版本/校验和。提交节点检查不代表所有 Flink TaskManager classpath 已验证。
- frontend：数据源表单和同步向导按目标能力展示；YAML/向导双向编辑保持一致，明确显示数据源测试和运行环境验证范围。
- runtime truth owner 仍为 SyncExecution；不从 Controller/Observability 修改状态。
- 不预先扩大 dependency whitelist。若确需新增 corridor，实施 PR 同时修改模块 REQUIREMENTS/DOMAIN/ARCHITECTURE/DEPENDENCIES 与对应 guard。

本规格是本功能的实施基线。任务 02/03 分别将相应需求和模型约定同步进 datasource/realtime 的模块合同，与代码和护栏同批评审；本文不改变现有运行实例或数据库。

## 8. 验收矩阵与证据

| 编号 | 场景 | 必须提供的证据 |
| --- | --- | --- |
| G0 | 定制 connector + Paimon 2.0.0 + S3 | 固定源码及补丁、依赖树、构建/测试结果、JAR 摘要；Flink 1.20.5 运行时版本证据；单表全量和 I/U/D 最小验证；DDL/恢复分别归 G6/G7 |
| G1 | 连接保存/编辑/测试 | AK/SK 加密与脱敏；掩码编辑不丢 secret；非法参数/认证/只读权限错误可区分；空 warehouse 测试 |
| G2 | 非 JDBC 模型 | Paimon 不要求 JDBC URL/driver；JDBC 历史配置可反序列化；digest 和不可变版本测试 |
| G3 | 编译与 UI | 向导/YAML round-trip；数据库路由、复合键、bucket/分区；错误选项明确拒绝；预览和持久化均不含明文凭据；提交期 YAML 仅在受限临时边界注入 |
| G4 | 提交及运行环境 | LOCAL/SSH 各自验证；缺失/冲突连接器明确失败；提交超时无重复作业；临时凭据文件失败路径清理 |
| G5 | 数据正确性 | 独立测试库表：初始全量、并发增量、I/U/D、主键/分区键值更新、decimal/null/时间/中文；源目标按主键逐行核对 |
| G6 | Schema | 上述每类允许/禁止事件；禁止事件无目标破坏；新增列/扩类型后故障恢复仍正确 |
| G7 | 故障恢复 | checkpoint 前后注入失败、重复事件；源删除行不复活；目标最终主键集合及字段一致；无 checkpoint 时明确失败边界 |
| G8 | 生命周期 | Save/Publish 不改变旧运行实例；Restart/Apply 提前拒绝不支持路径且不先停止健康任务；UNKNOWN/CONFLICT 不重复启动 |
| G9 | 回归 | MySQL/PostgreSQL 原有行为、架构护栏、后端编译测试与前端测试构建通过 |

真实 TOS 验收使用独立且明确指定的测试目标，不对现有业务表执行清空、删表或覆盖。需要真实凭据和可执行的 Flink 环境；`xx` 是占位值。本次未请求/保存真实密钥，也未执行任何存储写入。

G5/G7 对账应在源写入暂停并追平到明确 binlog 位点后读取目标已提交快照，保存 source 位点、checkpoint、Paimon snapshot、行级差异和时间。计数相等、RUNNING、Checkpoint 成功各自只能作为辅助证据。

## 9. 实施顺序与验证命令

1. 任务 01 完成版本适配及最小数据验证，任务 02 完成连接管理；两者没有相互依赖，可以并行。01 可使用隔离的 S3 兼容测试服务，真实 TOS 属于任务 08。
2. 01/02 完成后，任务 03 打通“配置 → 单表发布 → LOCAL 提交 → 全量和 I/U/D”完整链路。
3. 03 完成后，任务 04（多表）、05（SSH）、06（Schema）、07（恢复）可分别推进；共享代码的修改由集成分支协调。
4. 04/05/06/07 完成后，任务 08 在独立 TOS 目标做组合验收及全量回归；所有门槛通过才完成 Issue。

实施后的后端基线：`bash ./mvnw -B -ntp -pl yak-ops-boot -am clean verify`；如修改分发/插件装配，再验证根 reactor 的 `package` 分发产物。不得用 CI 的 `-DskipTests` 替代功能测试。

前端使用仓库 Yarn Classic lockfile：在 `yak-ops-ui` 执行 `yarn test --runInBand`、`yarn tsc`、`yarn build`，新增/修改测试覆盖 Paimon 配置及向导。既有或环境失败单独记录，不越界修复。

## 10. 测试边界与完成判定

主要行为测试通过现有数据源管理入口和实时任务 Application Facade 驱动，验证保存、发布、启动、失败和恢复的对外结果。优先复用既有组件替身和生命周期测试，不新增第二套测试专用业务入口。编译器、脱敏器和连接器外部进程属于少数必要适配边界，可分别验证结构化输出、安全清理及真实数据结果；避免对私有方法或调用次数做无业务意义的断言。

参考既有 PipelineYamlCompilerTest、RealtimeDefinitionLifecycleTest、FlinkCdcEngineGatewaySshTest、RealtimeExecutionCommandSafetyBaselineTest、RealtimeReplacementRecoveryTest；架构验证保留 RealtimeArchitectureTest、RealtimeSyncDependencyBoundaryTest、DataSourceDependencyBoundaryTest。

每项 ticket 提供可独立演示的行为、阻塞依赖、验收项和证据。`ready-for-agent` 仅表示任务定义完整，不表示依赖已完成；缺少真实环境、凭据或权限时记录未验收，不用 mock 结果替代 TOS 数据验收。

本次是规格与任务更新：验证文档链接、依赖图、G0–G9 覆盖及一致性；没有执行代码编译、运行任务或线上变更。
