# 07: fix: 验证 Paimon 故障恢复与重复提交保护

**Parent:** [Issue #1：支持 Paimon 作为实时同步目标端](https://github.com/201811510411lw/yak-ops/issues/1)

**Spec:** [Paimon 实时同步实现规格](../../architecture/PAIMON_REALTIME_SYNC_SPEC.md)，重点为交付语义、checkpoint 恢复、Reconcile、Restart/Apply 边界及 G7/G8。

**What to build:** 已运行的 Paimon 作业能在 Flink 同一作业故障恢复后继续得到正确目标结果；平台遇到提交结果不确定时识别原执行，面对不能恢复状态的 Restart/Apply 则在影响健康作业前拒绝。用户能看到实际恢复与失败边界。

**Blocked by:** [03：单表 LOCAL 同步](03-single-table.md)。

**Status:** ready-for-agent

## 实施约定

承诺为 at-least-once + Paimon 主键表幂等写入，约束为稳定主键、去重合并、事件顺序和单写入者；不宣称 exactly-once。同一 Flink 作业通过 checkpoint 恢复可以访问其自己创建的表。平台 Restart/Apply 是新 Execution，不能因目标存在便误判为可以无状态全量重跑。

复用 SyncExecution 的 runtime identity 及 UNKNOWN/CONFLICT Reconcile；当前没有已验证 restore 链路就提前拒绝相关操作，不为本票新增 savepoint 管理或手填 restore 功能。验收必须实际开启 checkpoint 与 Flink 重启策略，策略未生效时明确失败。

## 验收清单

- [ ] 记录并验证实际生效的 checkpoint/restart 配置；平台不能接受一个策略后静默不应用，必要修复限定在本功能运行要求内。
- [ ] 同一作业在 checkpoint 前后分别注入故障，恢复后持续 I/U/D 正确；重复重放不产生额外主键，更新不丢失，已删除记录不复活。
- [ ] 恢复能够访问同作业已创建目标表，同时首次新 Execution 指向已有表仍被拒绝；不以取消“仅新目标”限制解决恢复。
- [ ] 提交成功但响应丢失时保留唯一执行身份，经 Reconcile 找到原作业；UNKNOWN/CONFLICT 不触发第二次提交，UI/API 明确显示待核实状态。
- [ ] 不支持状态恢复的 Restart/Apply 在停止健康作业前拒绝；Save/Publish 不影响旧实例，失败尝试不改变旧运行状态或目标数据。
- [ ] 无 checkpoint 且已部分写入时不自动从头覆盖；checkpoint 不可用、binlog 过期、权限撤销均明确失败并提供可操作的原因，不无提示重建。
- [ ] 同步异常与恢复记录不泄密，不删除/清空业务数据；原 JDBC 生命周期和 Reconcile 行为通过回归。

## 验证与证据

在隔离环境按固定数据序列执行全量、并发增量及故障注入，保存 source binlog 位点、故障前后 checkpoint、job/runtime identity、Paimon snapshot 和逐行差异。证明无重复提交需同时核对平台 execution 与 Flink 作业列表，不能只看平台状态。完成后 review 完整 diff，执行后端 `bash ./mvnw -B -ntp -pl yak-ops-boot -am clean verify`；前端变更执行 `yarn test --runInBand`、`yarn tsc`、`yarn build`；产物/装配变更补其构建测试及根 reactor `package`。

## 非目标

不新增 savepoint 创建/选择/管理/恢复产品，不迁移已有作业、不接管外部已有表，不对 exactly-once 作承诺。
