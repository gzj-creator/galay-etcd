# Release Note

按时间顺序追加版本记录，避免覆盖历史发布说明。

## v1.1.4 - 2026-04-21

- 版本级别：小版本（patch）
- Git 提交消息：`chore: 发布 v1.1.4`
- Git Tag：`v1.1.4`
- 自述摘要：
  - 锁定 `galay-kernel 3.4.4`、`galay-utils 1.0.3` 与 `GalayHttp 2.0.2` 的依赖版本，确保 `galay-etcd` 在最新基础库前缀下稳定解析依赖。
  - 同步更新安装导出的 `GalayEtcdConfig.cmake` 依赖声明，避免下游回落到旧版本基础库。
