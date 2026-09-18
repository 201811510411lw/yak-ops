# 02: feat: 支持 Paimon 数据源配置与只读目录访问

**Parent:** [Issue #1：支持 Paimon 作为实时同步目标端](https://github.com/201811510411lw/yak-ops/issues/1)

**Spec:** [Paimon 实时同步实现规格](../../architecture/PAIMON_REALTIME_SYNC_SPEC.md)，重点为数据源、凭据、连接测试和非 JDBC 架构边界。

**What to build:** 用户能在数据源界面创建、保存、编辑一个 Paimon filesystem Catalog 连接，通过 API 与数据源插件完成只读连接测试和库表目录查看，并正确理解测试只证明 Yak Ops 节点的读取能力。完整贯通表单、API、安全存储、插件和错误反馈。

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

## 实施约定

固定 Paimon 2.0.0；连接配置包括 filesystem Catalog、`s3://` warehouse、自定义 HTTP(S) endpoint、可选 region、可选 path-style、默认库、Access Key 和 Secret Key。按标准 S3 兼容接口实现，TOS 不使用专用 SDK。参数使用显式类型与允许范围；不开放任意 JSON 透传。

Catalog 的只读访问可在隔离 S3 环境验证，不依赖 01 的 Flink Pipeline 产物。AK/SK 均使用现有 Descriptor-aware secret codec；Paimon 不是 JDBC 数据源，不伪造 URL/driver 或声明不具备的 SQL 执行能力。

## 验收清单

- [ ] UI/API 能创建、保存、编辑、重新载入配置并查看库表；warehouse、endpoint 等必填/格式错误在保存前明确反馈，拒绝 endpoint userinfo/内嵌凭据。
- [ ] region/path-style 为可配置项，不把 TOS 地址、bucket 或 path-style 值写死；未知或不适用参数明确拒绝。
- [ ] AK/SK 持久化加密、详情脱敏；掩码编辑复用原 secret，变更 secret 可正常保存；API 响应、日志、异常及审计摘要不泄漏凭据。
- [ ] 连接测试与目录读取不创建库、表或对象；空 warehouse 不因没有数据库而失败，UI 说明测试来自 Yak Ops 节点且不验证写权限或 Flink 节点连通性。
- [ ] 配置无效不覆盖已有状态；未保存连接的测试不持久化状态；参数、认证、权限、网络、Catalog 元数据和依赖缺失能区分且统一脱敏。
- [ ] Paimon SDK/S3 访问限制在 adapter/plugin，不进入核心 Domain；必要 SPI 变更保持现有 JDBC 插件兼容，并同步相关模块合同与架构 guard。
- [ ] 既有 JDBC 数据源配置、连接测试和凭据掩码行为通过回归测试。

## 验证与证据

提供 UI/API 的保存—编辑—测试—目录查看完整演示及集成结果。针对只读测试使用可观察对象写入的隔离环境或有请求审计的集成环境，证明测试未进行创建或写入；保留脱敏失败样例。实现完成后 review 完整 diff，执行 `bash ./mvnw -B -ntp -pl yak-ops-boot -am clean verify`；涉及插件分发时验证根 reactor `package` 产物。前端在 `yak-ops-ui` 执行 `yarn test --runInBand`、`yarn tsc`、`yarn build`。既有/环境失败与本次缺陷分别记录。

## 非目标

不提交同步任务、不验证 TOS 写权限、不支持 Hive Catalog、SQL 编辑器或 Paimon Source；不在连接测试中写探针对象。
