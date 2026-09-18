# 06: feat: 支持 Paimon 有限 Schema Evolution 并拒绝越界 DDL

**Parent:** [Issue #1：支持 Paimon 作为实时同步目标端](https://github.com/201811510411lw/yak-ops/issues/1)

**Spec:** [Paimon 实时同步实现规格](../../architecture/PAIMON_REALTIME_SYNC_SPEC.md)，重点为 Schema Evolution 事件矩阵、DDL 故障表现与 G6。

**What to build:** 用户在向导或 Yak YAML 选择默认 FAIL 或有限 EVOLVE；运行时只有已声明的安全 schema 变化可以应用，其他 DDL 导致明确失败且不会先破坏目标表，平台展示实际原因。完整覆盖配置、编译、connector 事件处理、目标结果及错误反馈。

**Blocked by:** [03：单表 LOCAL 同步](03-single-table.md)。

**Status:** ready-for-agent

## 实施约定

初始建表允许，不属于运行中的 schema 变化。FAIL 对应 CDC 的 `exception` 行为并以真实事件测试确认。EVOLVE 仅允许新增可空、无默认值、非主键列，以及非主键列 `INT → BIGINT`；首期不支持 IGNORE。产品允许范围不是直接透传 upstream `evolve` 的全部行为，必要时补齐 01 外部运行产物的适配并更新构建证据。

对删列、列改名、其他类型变化、主键/分区键变更、删表及 TRUNCATE，必须验证实际 Source—Pipeline—Sink 路径能发现并失败。若上游忽略某事件或无法在目标执行前拦截，需解决事件缺口；不能靠发布前校验或静默缩减验收代替。

## 验收清单

- [ ] UI/YAML/API 的 FAIL/EVOLVE 配置、保存重载、digest 和编译一致；IGNORE 及未完成适配的能力明确拒绝。
- [ ] FAIL 允许首次建表，对规格中的运行中 DDL 均显示失败；没有目标 schema 先变更、任务后报错的窗口。
- [ ] EVOLVE 新增允许列后，旧行呈现正确空值、新行及后续 UPDATE 正确携带新列；无需用户重新创建目标表。
- [ ] EVOLVE 的 `INT → BIGINT` 正确更新目标类型，超出 INT 范围的值可写入且不截断、不溢出，既有值保持正确。
- [ ] 逐项验证禁止事件：删列、列改名、非允许类型变更、主键/分区键变化、删表、TRUNCATE；任务明确失败，目标表及已提交数据未被破坏。
- [ ] UI/API 展示真实执行异常与失败事件类别且无凭据；失败后不自动删表、不全量重建、不悄悄忽略 DDL 并继续展示成功。
- [ ] Source 未暴露事件、类型映射不支持或拦截存在缺口时留下复现证据并保持本票阻塞，不将仅 FAIL 作为完整完成有限 EVOLVE 的替代。
- [ ] 变更后的 connector 产物可重复构建、版本/摘要同步，单表 I/U/D 与 JDBC Schema 行为回归通过。Schema 变化后故障恢复的组合验收交由 08 联合 07 执行。

## 验证与证据

隔离源表逐项触发真实 MySQL DDL，而非只向测试 mock 注入内部事件；保存 Source 可见事件/位点、任务失败原因、目标 schema/snapshot 前后差异、允许变更的行级核对。没有真实事件的场景不得标为通过。完成后 review 完整 diff，运行适配产物自身构建/测试和后端 `bash ./mvnw -B -ntp -pl yak-ops-boot -am clean verify`；前端执行 `yarn test --runInBand`、`yarn tsc`、`yarn build`，装配变化补根 reactor `package`。

## 非目标

不实现通用 schema 演化、自动修复 DDL 失败、破坏性目标 DDL、Schema IGNORE、改变主键/分区/bucket 或 Paimon Action 语义。
