# CHANGELOG

维护说明：
- 未打 tag 的改动先写入 `## [Unreleased]`，发版时再归档为带版本号的节。
- 版本号遵循语义化规则：大改动升 major、新功能升 minor、修复/文档/维护升 patch。
- 推荐标题格式：`## [vX.Y.Z] - YYYY-MM-DD`，按时间倒序维护。
- 建议按 `Added` / `Changed` / `Fixed` / `Docs` / `Chore` 等小节归纳关键变化。

## [Unreleased]

## [v3.0.1] - 2026-04-29

### Docs
- 新增仓库级 `AGENTS.md` 约束文档，统一目录职责、构建产物边界、命名规范、接口约束与发版前验证清单。
- 明确 CMake/Bazel 双构建同步维护、`CHANGELOG.md` / `docs/release_note.md` / Tag 版本一致性与性能基准产出要求。

## [v3.0.0] - 2026-04-29

### Added
- 为 `AsyncEtcdClient` 新增单 key watch 能力，提供 `watch(key, task_handler)` 与 `watch(key, function_handler)` 两个公开重载。
- 新增 `EtcdWatchEventType`、`EtcdWatchEvent` 与 `EtcdWatchResponse`，补齐 watch 事件模型与解析辅助函数。
- 新增 `T9-AsyncEtcdTaskWatch`，实跑覆盖 `WatchTaskHandler` 路径。

### Changed
- 将同步与异步 etcd client 的公开结果契约统一改为“直接返回结构化 `std::expected<value, EtcdError>`”，不再依赖最近一次结果缓存。
- 调整 `EtcdClient` / `AsyncEtcdClient`、示例、测试与 benchmark 全部调用方式，统一从返回值读取 `get/del/grantLease/pipeline` 结果。
- 更新 README 与 API/使用/高级主题文档，改写为直接返回值与 async watch 的新语义。

### Removed
- 移除公开 `lastError()`、`lastBool()`、`lastLeaseId()`、`lastDeletedCount()`、`lastKeyValues()`、`lastPipelineResults()`、`lastStatusCode()` 与 `lastResponseBody()` 访问器。

### Release
- 按大版本发布要求提升版本到 `v3.0.0`。

## [v2.0.0] - 2026-04-29

### Changed
- 统一源码、头文件、测试、示例与 benchmark 文件命名为 `lower_snake_case`，编号前缀同步使用 `t<number>_`、`e<number>_` 与 `b<number>_` 风格。
- 同步更新构建脚本、模块入口、示例、测试、文档与脚本中的文件路径引用。
- 将项目内头文件包含调整为基于公开 include 根或模块根的非相对路径。

### Release
- 按大版本发布要求提升版本到 `v2.0.0`。

## [v1.1.8] - 2026-04-23

### Fixed
- 将主包配置安装目录从 `lib/cmake/galay-etcd` 调整为 `lib/cmake/GalayEtcd`，修复 `find_package(GalayEtcd CONFIG REQUIRED)` 无法自动命中安装包的问题。
- 补充 `galay-etcd` 兼容配置入口与版本文件，继续支持旧的 `find_package(galay-etcd CONFIG REQUIRED)` 消费方式。

### Added
- 新增 `scripts/tests/test_cmake_packaging.sh`，覆盖 `GalayEtcd` / `galay-etcd` 双入口的安装后包查找回归。

## [v1.1.7] - 2026-04-23

### Changed
- 将测试构建主入口统一到 `BUILD_TESTING`，并在用户未显式开启时默认强制关闭测试树。
- 保留 `GALAY_ETCD_BUILD_TESTS` 与 `BUILD_TESTS` 兼容映射，已有脚本仍可显式开启测试目标。

## [v1.1.6] - 2026-04-23

### Changed
- 将源码仓库中的包配置模板重命名为统一的小写 kebab-case `galay-etcd-config.cmake.in`，消除与其他 `galay-*` 项目的命名分歧。
- 同步更新 `configure_package_config_file(...)` 的模板路径，保持安装导出的 `GalayEtcdConfig.cmake` / `GalayEtcdConfigVersion.cmake` 兼容不变。

### Fixed
- 将源码构建与安装导出配置中的 `GalayHttp` 最低依赖版本从 `2.0.2` 修正为 `2.1.0`，与 `AsyncEtcdClient` 实际使用的 `HttpSession::sendSerializedRequest(...)` API 保持一致。

## [v1.1.5] - 2026-04-22

### Chore
- 删除误提交的 `build-docverify/` 构建目录及其 CMake 生成文件、二进制和测试输出，收紧仓库提交范围。
- 扩展忽略规则，新增 `build-*` 目录和 `*.log` 日志文件过滤，避免构建与运行产物再次进入版本库。

## [v1.1.4] - 2026-04-21

### Changed
- 锁定 `galay-kernel 3.4.4`、`galay-utils 1.0.3` 与 `GalayHttp 2.0.2` 的 CMake 依赖版本，避免误链接旧前缀中的基础库。
- 同步更新导出包配置中的 `find_dependency(...)` 版本约束，确保下游消费与源码构建使用同一组内部依赖版本。
