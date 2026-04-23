# Release Note

按时间顺序追加版本记录，避免覆盖历史发布说明。

## v1.1.7 - 2026-04-23

- 版本级别：小版本（patch）
- Git 提交消息：`chore: 默认关闭测试构建`
- Git Tag：`v1.1.7`
- 自述摘要：
  - 将 `BUILD_TESTING` 固化为 `galay-etcd` 的测试主开关，并在未显式开启时默认强制关闭测试构建，避免配置阶段被 `CTest` 隐式带成开启状态。
  - 保留 `GALAY_ETCD_BUILD_TESTS` 与 `BUILD_TESTS` 两条兼容映射，已有脚本仍能通过旧参数显式恢复测试目标。
  - 将根 `CMakeLists.txt` 中的 `include(CTest)` 调整到测试选项钳制之后，确保默认配置和显式开启路径行为一致。

## v1.1.6 - 2026-04-23

- 版本级别：小版本（patch）
- Git 提交消息：`fix: 修正 galay-etcd 的 GalayHttp 最低依赖版本`
- Git Tag：`v1.1.6`
- 自述摘要：
  - 将源码仓库里的包配置模板统一为小写 kebab-case 命名，避免 `galay-etcd` 与其他 `galay-*` 项目在模板文件名上继续分叉。
  - 同步调整 `configure_package_config_file(...)` 的输入路径，同时保持安装后的 `GalayEtcdConfig.cmake` 与版本文件名不变，避免影响现有下游消费。
  - 将源码构建与安装导出配置中的 `GalayHttp` 最低依赖版本从 `2.0.2` 修正为 `2.1.0`，避免下游按旧版本约束解析后缺失 `HttpSession::sendSerializedRequest(...)`。

## v1.1.5 - 2026-04-22

- 版本级别：小版本（patch）
- Git 提交消息：`chore: 清理误提交的构建产物与日志文件`
- Git Tag：`v1.1.5`
- 自述摘要：
  - 删除误提交的 `build-docverify/` 构建输出与 `galay-http.log` 运行日志，移除不必要的仓库噪音。
  - 扩展 `.gitignore` 以忽略 `build-*` 目录和 `*.log` 文件，避免后续再次把生成产物带入发布。

## v1.1.4 - 2026-04-21

- 版本级别：小版本（patch）
- Git 提交消息：`chore: 发布 v1.1.4`
- Git Tag：`v1.1.4`
- 自述摘要：
  - 锁定 `galay-kernel 3.4.4`、`galay-utils 1.0.3` 与 `GalayHttp 2.0.2` 的依赖版本，确保 `galay-etcd` 在最新基础库前缀下稳定解析依赖。
  - 同步更新安装导出的 `GalayEtcdConfig.cmake` 依赖声明，避免下游回落到旧版本基础库。

## v1.1.8 - 2026-04-23

- 版本级别：小版本（patch）
- Git 提交消息：`chore: 发布 v1.1.8`
- Git Tag：`v1.1.8`
- 自述摘要：
  - 将主包配置的安装目录调整为 `lib/cmake/GalayEtcd`，修复下游 `find_package(GalayEtcd CONFIG REQUIRED)` 无法自动发现安装包的问题。
  - 补充 `galay-etcd` 兼容配置入口与对应版本文件，继续兼容文档和旧脚本使用的 `find_package(galay-etcd CONFIG REQUIRED)`。
  - 新增 `scripts/tests/test_cmake_packaging.sh`，回归验证两种包名入口都能在安装后正确解析。
