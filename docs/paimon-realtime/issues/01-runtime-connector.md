# 01: build: 适配 CDC 3.6 的 Paimon 2.0 运行产物

**Parent:** [Issue #1：支持 Paimon 作为实时同步目标端](https://github.com/201811510411lw/yak-ops/issues/1)

**Spec:** [Paimon 实时同步实现规格](../../architecture/PAIMON_REALTIME_SYNC_SPEC.md)，重点为版本基线、G0、数据与凭据边界。

**What to build:** 提供基于 Flink CDC 3.6.0 YAML Pipeline、Flink 1.20.5、Paimon 2.0.0 的可重复构建和最小同步验证。执行者按文档准备运行产物后，能将一张 MySQL 有主键表的全量及增删改写入标准 S3 兼容存储上的新 Paimon 表；不依赖真实 TOS 账号即可复现。

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

## 实施约定

执行路线已确定为 Yak 现有 YAML Pipeline。先定位 Yak 当前使用的 CDC 源码、既有定制与运行产物归属，在该 3.6.0 基线上实施，保留原 JDBC 能力；上游 release 标签用作依赖核查基准。CDC 3.6.0 官方 Paimon Connector 的 Paimon 1.3.1 依赖需要适配到 2.0.0，不能通过再挂一个 Paimon 2.0 JAR 或仅修改版本号认定兼容。固定上游及 Yak 源码 commit、构建 profile、补丁及完整运行依赖；连接器构建与分发是外部运行时依赖管理，不让控制面在线构建或安装产物。

使用隔离 MySQL 与 S3 兼容测试环境、独立 warehouse/库表、单写入任务。匹配 Paimon 2.0.0 的 S3 实现必须进入实际运行 classpath。后续 06、07 分别扩展运行时 DDL 与恢复能力，本票不提前宣称这些场景已通过。

## 验收清单

- [ ] 构建入口、上游 commit、适配补丁、构建依赖及 SHA-256 可重现；记录 Flink 1.20 对应构建配置和 Paimon 2.0.0 依赖树。
- [ ] 验证实际加载的是 Paimon 2.0.0 及匹配 S3 实现，排除混入旧 Paimon 类/产物造成的假兼容；输出可供环境探测使用的版本、清单和摘要。
- [ ] 单表初次全量、持续 INSERT、非主键 UPDATE、DELETE 均在目标提交快照中逐行核对；覆盖空值、decimal、时间和中文数据。
- [ ] 明确源主键如何映射为 Paimon 主键、目标新库表何时创建、重复建表事件及已有表初始化的实际行为，为平台启动前保护提供依据。
- [ ] 验证使用显式 checkpoint 和 Flink 故障恢复配置能够运行，记录生效配置；不得以 RUNNING 或 checkpoint 成功代替目标数据核对。
- [ ] 密钥由受限运行输入提供；脚本、命令参数、构建输出、提交日志与证据中没有真实 AK/SK 或 MySQL 密码。
- [ ] API/依赖冲突必须修复并留下回归验证；如果版本适配仍失败，记录可复现错误并保持本票未完成，不静默降级 Paimon 或切到 Action。

## 验证与证据

保留可运行的复现入口、隔离环境说明、构建/测试结果、依赖树、运行产物摘要，以及 source binlog 位点、checkpoint、Paimon snapshot 和行级差异。对账时暂停测试写入并确认追平后读取已提交目标快照。产物构建执行其自身规定的编译与测试；若调整 Yak 分发装配，验证相应 reactor 的构建及分发内容。任何网络或依赖环境失败单独记录，不能写为数据验收成功。

## 非目标

不实现平台数据源/向导、SSH、完整 Schema Evolution 或恢复产品；不改用 Paimon Action、不安装生产运行产物、不使用真实 TOS 业务表。
