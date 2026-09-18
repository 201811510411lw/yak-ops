# 03: feat: 打通 Paimon 单表配置发布与 LOCAL 同步

**Parent:** [Issue #1：支持 Paimon 作为实时同步目标端](https://github.com/201811510411lw/yak-ops/issues/1)

**Spec:** [Paimon 实时同步实现规格](../../architecture/PAIMON_REALTIME_SYNC_SPEC.md)，重点为单表数据行为、默认 FAIL、模型、LOCAL 提交和生命周期保护。

**What to build:** 用户选择 MySQL 有主键源表和 Paimon 目标连接，在同步向导或 Yak YAML 中编辑同一任务定义，发布后经 LOCAL 环境启动，看到新目标表全量及持续增删改正确落地、状态和错误可追踪。此票完成第一条平台端到端可运行链路。

**Blocked by:** [01：运行产物](01-runtime-connector.md)、[02：Paimon 数据源](02-datasource.md)。

**Status:** ready-for-agent

## 实施约定

运行基线为 Flink 1.20.5 + CDC 3.6.0 YAML Pipeline + Paimon 2.0.0；首个切片限定 `initial`、单个显式源表、单列物理主键、单个未存在的非分区目标表与固定正整数 bucket。无物理主键不得用手填 ReplayKey 绕过；向导、YAML、DTO、版本持久化映射到唯一 SyncDefinition，新增行为字段进入 digest。

目标采用 filesystem Catalog 和 S3 配置，编译结果使用 Paimon sink/catalog/table/route 参数，不能混入 JDBC batch/statement-cache 配置。凭据只在提交边界解析；任务定义、版本、执行快照和可展示 YAML 不保存明文凭据。对运行中新 DDL 默认 FAIL，初始建表除外。

## 验收清单

- [ ] 向导与 Yak YAML 双向编辑和保存重载一致；发布产生不可变 DefinitionVersion，digest 覆盖 Paimon 目标行为；历史 JDBC 定义可反序列化且行为不变。
- [ ] 无主键、ReplayKey 不匹配、未知表属性、动态正则路由、多对一和当前不支持模式明确拒绝；单列主键、目标库表与 bucket 进入真实运行配置。
- [ ] Save、预览、Publish 和连接测试均不创建目标；Start 前检查目标不存在，只有实际建表事件才能创建新表。已有表明确拒绝，不覆盖或清空；说明独占目标名称与单写入者是运行前提。
- [ ] LOCAL 启动前探测 01 产物及匹配 S3 依赖，版本缺失或冲突可诊断；环境声明与探测证据分开，提交节点检查不冒充所有 TaskManager 的 classpath 检查。
- [ ] 已发布单表任务完成全量及 INSERT/UPDATE/DELETE、主键值变更；UI/API 能展示正确执行身份、状态、日志和 Metrics，源目标逐行一致。
- [ ] 默认 FAIL 编译并实际生效：初始建表允许，运行中可观测 DDL 导致任务失败且不在目标先执行变更；不能仅验证 YAML 字符串。
- [ ] LOCAL 临时凭据文件权限和成功/异常/提交超时清理经过测试；AK/SK 与源密码不进入 argv、日志、持久化快照或用户可见 YAML。
- [ ] Save/Publish 不改变旧运行实例；对已有目标发起新 Execution，以及尚无状态恢复路径的 Restart/Apply，执行前拒绝且不先停止健康任务。提交不确定时保留 runtime identity，不自动重提。
- [ ] Core Domain 不依赖 Paimon SDK/S3/Flink YAML；同步受影响模块的 REQUIREMENTS/DOMAIN/ARCHITECTURE/DEPENDENCIES 与 guard，不预先放宽全部依赖边界。

## 验证与证据

隔离 MySQL + S3 环境从 UI/API 配置直至目标快照核对，记录 definition/version/execution/job 标识、运行产物摘要、生效 checkpoint 配置和行级差异。失败用例包含已有目标、缺依赖、受限写权限、DDL、提交失败和凭据清理；保留明确的失败阶段。实现完成后 review 全量 diff，执行后端 `bash ./mvnw -B -ntp -pl yak-ops-boot -am clean verify`，涉及分发时验证根 reactor `package`；前端执行 `yarn test --runInBand`、`yarn tsc`、`yarn build`。真实 TOS 验收留给 08。

## 非目标

不做多表/分区/复合主键、SSH、EVOLVE 或新 savepoint 恢复入口；不允许当前不支持的表属性因 UI 未显示而从 YAML 绕过。
