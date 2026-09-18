# 08: test: 完成 TOS 端到端验收与 JDBC 回归

**Parent:** [Issue #1：支持 Paimon 作为实时同步目标端](https://github.com/201811510411lw/yak-ops/issues/1)

**Spec:** [Paimon 实时同步实现规格](../../architecture/PAIMON_REALTIME_SYNC_SPEC.md)，覆盖完整 G0–G9 验收矩阵。

**What to build:** 将前序能力组合为可交付的完整 Paimon 实时同步功能，在真实 TOS 的独立测试目标上验证 LOCAL/SSH、单表/多表、Schema Evolution、故障恢复及失败保护，同时确认原 JDBC 功能和所有规定构建测试通过。产出可复核的验收记录与运行指引。

**Blocked by:** [04：多表路由](04-multi-table.md)、[05：SSH](05-ssh.md)、[06：Schema Evolution](06-schema.md)、[07：恢复与执行保护](07-recovery.md)。

**Status:** ready-for-agent

## 实施约定

采用已确定的 Flink 1.20.5 + Flink CDC 3.6.0 YAML Pipeline + Paimon 2.0.0，以及 filesystem Catalog + 标准 S3 接口。用户提供的 warehouse 为 `s3://emr-olap-starrocks-20241125160240/paimon-2.0`，endpoint 为 `https://tos-s3-cn-beijing.ivolces.com`；它们必须作为配置输入，不写死到实现。

真实验收开始前需具备授权的隔离测试源、Flink/SSH 环境、真实凭据和明确指定的独立 TOS 测试库/表或前缀。示例 `xx` 不是可用凭据，`work_temp` 不自动视为可覆盖的测试库。不清空、删改或覆盖现有业务对象；需要的测试创建/写入在确认的隔离范围内执行。环境缺失只阻塞真实验收，不能凭 mock、编译成功或本地 S3 通过认定 TOS 已通过。

## 验收清单

- [ ] G0–G4：运行产物版本/摘要、数据源保存编辑脱敏与只读测试、非 JDBC 模型、向导/YAML round-trip、LOCAL/SSH 提交及能力检查均有可核查结果。
- [ ] G5：单表、多表独立路由、复合主键、分区/bucket、初始全量与并发 I/U/D，包含主键/分区键值更新、decimal/null/时间/中文，均按主键逐行核对。
- [ ] G6：FAIL/有限 EVOLVE 允许与禁止事件矩阵全部覆盖；禁止事件无目标破坏，DDL 失败可见且不静默忽略。
- [ ] G6/G7 组合：新增允许列或完成 `INT → BIGINT` 后再故障恢复，旧行、新行、边界值和恢复后的 I/U/D 正确；不能仅将两个独立成功记录拼作组合验收。
- [ ] G7：checkpoint 前后故障、重复重放、删除不复活、无 checkpoint/过期 binlog/权限撤销失败边界在授权测试范围内有实际证据。
- [ ] G8：Save/Publish 不改变旧实例；不支持的 Restart/Apply 提前拒绝且旧作业健康；提交响应丢失后 Reconcile 找回原作业并证明没有重复作业，覆盖 LOCAL/SSH。
- [ ] 凭据在配置、版本、执行快照、预览、错误、日志、argv 及 LOCAL/SSH 临时文件生命周期中均符合安全边界，证据和运行指引不包含真实密钥。
- [ ] G9：MySQL/PostgreSQL JDBC 数据源、同步/路由、发布、LOCAL/SSH 与生命周期回归通过；架构 guard、后端编译测试、前端测试类型检查和构建通过。
- [ ] 汇总所有验收缺口与实际修复；未验证或失败场景保留为未完成，不用 RUNNING、计数相等或 checkpoint 成功替代正确性证据。

## 验证与证据

对每次运行记录配置基线、镜像/产物摘要、源库表和独立目标范围、definition/version/execution/job、环境类型与时间。暂停测试写入并追平到明确 binlog 位点后，读取目标已提交 snapshot，对齐 source 位点、checkpoint 和 Paimon snapshot，保存行级差异、DDL/恢复前后结果及脱敏日志。

最终 review 完整功能 diff 和 G0–G9 覆盖关系，执行 `bash ./mvnw -B -ntp -pl yak-ops-boot -am clean verify`；分发/插件装配变化验证根 reactor `package`，外部适配 connector 执行其规定构建与测试。前端在 `yak-ops-ui` 执行 `yarn test --runInBand`、`yarn tsc`、`yarn build`。只修复本功能导致的问题，既有、环境或外部依赖失败保留证据并说明，不能越界修改以换取绿灯。

## 非目标

不自动部署生产、不迁移业务表、不清理未明确授权的测试或业务对象、不提交/推送 Git、不发布 GitHub 评论或关闭父 Issue；这些不属于本票验收本身。
