# 05: feat: 支持 SSH 环境安全运行 Paimon 同步

**Parent:** [Issue #1：支持 Paimon 作为实时同步目标端](https://github.com/201811510411lw/yak-ops/issues/1)

**Spec:** [Paimon 实时同步实现规格](../../architecture/PAIMON_REALTIME_SYNC_SPEC.md)，重点为 SSH 能力探测、提交身份、凭据生命周期和执行观测。

**What to build:** 用户选择 SSH 计算环境后，能查看 Paimon 运行能力检查结果，发布并远程启动已有单表 Paimon 任务，从平台持续观察真实执行状态和失败原因；远端临时凭据在成功和失败路径均受到保护。

**Blocked by:** [03：单表 LOCAL 同步](03-single-table.md)。

**Status:** ready-for-agent

## 实施约定

复用唯一 SyncDefinition、DefinitionVersion 与 SyncExecution 生命周期，不建立 SSH 专属任务模型。远端使用 Flink 1.20.5 + CDC 3.6.0 + 01 适配的 Paimon 2.0.0/S3 产物，配置语义与 LOCAL 一致；SSH 节点检查的结论只覆盖提交节点，TaskManager 实际加载与 S3 访问需运行验证。

## 验收清单

- [ ] UI/API 能声明、探测和展示 SSH 环境能力；远端 Flink/CDC/connector/Paimon/S3 版本及校验和可核查，缺失或冲突在提交前明确失败。
- [ ] 发布的单表任务可通过 SSH 提交、关联唯一 runtime identity、查询状态/日志/Metrics 并完成全量及增删改，平台观测最终指向同一真实 Flink 作业。
- [ ] 传输采用既有安全 stdin/受限临时文件机制，source 密码及 AK/SK 不进入命令参数、日志、错误、永久 YAML 或执行快照；远端文件权限可验证。
- [ ] 成功、远端退出非零、连接断开、超时和清理失败均有明确处理；可达远端会清理凭据文件，远端暂不可达时有可追踪的延迟清理策略，不能误称已清理。
- [ ] 远端提交可能已成功但响应丢失时进入现有 UNKNOWN/CONFLICT Reconcile 路径并保留身份，不重提第二个作业；可在恢复连接后识别原作业。
- [ ] 停止、已有目标拒绝、禁止 Restart/Apply 的保护与 LOCAL 一致，不因 SSH 路径绕过目标数据保护。
- [ ] SSH 测试成功明确区分连接性、依赖检查与真正目标写入验证；LOCAL 和原有 JDBC SSH 路径通过回归。

## 验证与证据

使用隔离 SSH 提交主机与 S3 测试环境，记录探测输出、远端产物摘要、execution/job 身份对应、临时文件权限/生命周期及目标快照行级差异。故障注入覆盖提交前失败和提交后断连，证明后者没有第二个作业。完成后 review 完整 diff，执行后端 `bash ./mvnw -B -ntp -pl yak-ops-boot -am clean verify`；前端改动执行 `yarn test --runInBand`、`yarn tsc`、`yarn build`；涉及装配时补根 reactor `package`。

## 非目标

不自动分发/安装 connector、不管理 Flink 集群、不把 SSH 主机探测扩称为所有 TaskManager 验证，不引入新的恢复产品。
