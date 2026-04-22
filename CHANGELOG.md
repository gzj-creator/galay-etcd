# CHANGELOG

维护说明：
- 未打 tag 的改动先写入 `## [Unreleased]`。
- 需要发版时，从 `Unreleased` 或“自上次 tag 以来”的累计变更整理出新的版本节。
- 版本号遵循 `major/minor/patch` 规则：大改动升主版本，新功能升次版本，修复与非破坏性维护升修订版本。
- 推荐标题格式为 `## [vX.Y.Z] - YYYY-MM-DD`，正文按 `Added` / `Changed` / `Fixed` / `Docs` / `Chore` 归纳。

## [Unreleased]

## [v1.1.6] - 2026-04-23

### Changed
- 将源码仓库中的包配置模板重命名为统一的小写 kebab-case `galay-etcd-config.cmake.in`，消除与其他 `galay-*` 项目的命名分歧。
- 同步更新 `configure_package_config_file(...)` 的模板路径，保持安装导出的 `GalayEtcdConfig.cmake` / `GalayEtcdConfigVersion.cmake` 兼容不变。

## [v1.1.5] - 2026-04-22

### Chore
- 删除误提交的 `build-docverify/` 构建目录及其 CMake 生成文件、二进制和测试输出，收紧仓库提交范围。
- 扩展忽略规则，新增 `build-*` 目录和 `*.log` 日志文件过滤，避免构建与运行产物再次进入版本库。

## [v1.1.4] - 2026-04-21

### Changed
- 锁定 `galay-kernel 3.4.4`、`galay-utils 1.0.3` 与 `GalayHttp 2.0.2` 的 CMake 依赖版本，避免误链接旧前缀中的基础库。
- 同步更新导出包配置中的 `find_dependency(...)` 版本约束，确保下游消费与源码构建使用同一组内部依赖版本。
