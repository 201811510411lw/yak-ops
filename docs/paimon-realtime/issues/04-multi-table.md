# 04: feat: 支持 Paimon 多表路由与复合主键分区配置

**Parent:** [Issue #1：支持 Paimon 作为实时同步目标端](https://github.com/201811510411lw/yak-ops/issues/1)

**Spec:** [Paimon 实时同步实现规格](../../architecture/PAIMON_REALTIME_SYNC_SPEC.md)，重点为显式一对一路由、复合主键、bucket/分区与数据正确性。

**What to build:** 用户在同一同步任务中配置多个明确的 MySQL 源表，各自映射到独立新 Paimon 表；向导与 Yak YAML 能表达复合主键、固定 bucket、合法分区键并提交运行，目标数据与各自源表逐行一致。

**Blocked by:** [03：单表 LOCAL 同步](03-single-table.md)。

**Status:** ready-for-agent

## 实施约定

继承 03 的版本、安全、仅新目标、LOCAL、`initial` 和默认 FAIL 边界。路由按显式 `database.table` 一对一映射；所有源物理主键完整保留，ReplayKey 与其匹配。只支持固定正整数 bucket 和包含于主键的分区字段，固定主键去重写入语义，不开放 aggregation/sum 等合并模式。

## 验收清单

- [ ] 向导和 Yak YAML 可保存、编辑、预览、发布多个明确源表及独立目标映射，round-trip、digest 与不可变版本行为一致。
- [ ] 目标同名冲突、同任务重复目标、动态正则路由、多对一、无主键和缺失复合主键字段在提交前明确拒绝，不静默合并。
- [ ] bucket/分区按连接器实际可表达能力映射；若上游配置粒度无法满足逐表差异，补适配或明确拒绝该组合，不能展示已保存却不生效的设置。
- [ ] 分区字段不属于主键、bucket 非正整数、未知/不适用表属性明确拒绝；运行后的主键、分区、bucket 变更不通过 Apply 自动执行。
- [ ] 多个目标表首次创建结构正确，数据库/表名、完整复合主键、分区键与 bucket 值均有目标元数据证据。
- [ ] 每张表覆盖全量、增删改、复合主键碰撞边界、主键值/分区键值更新；旧主键或旧分区记录不残留，删除行不复活，跨表数据不串写。
- [ ] 一张目标已经存在时在启动前拒绝整个任务，不先创建其余目标；错误显示具体路由并保持凭据脱敏。
- [ ] 03 的单表路径和原有 JDBC 路由通过回归验证。

## 验证与证据

使用隔离源库与 S3 warehouse，至少两张不同结构源表，其中一张带复合主键及分区键。保留向导/YAML round-trip、实际生成配置、目标表元数据、各表源位点/目标 snapshot/行级差异。对账在暂停写入并追平后进行，计数仅为辅助。完成后 review 完整 diff，执行后端 `bash ./mvnw -B -ntp -pl yak-ops-boot -am clean verify` 与前端 `yarn test --runInBand`、`yarn tsc`、`yarn build`；分发改动补根 reactor `package`。

## 非目标

不支持动态发现新表、分表合并、多写入者、动态 bucket、自动改已有表主键/分区或离线重建。
