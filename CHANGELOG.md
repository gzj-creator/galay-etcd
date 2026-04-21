# CHANGELOG

维护说明：
- 未打 tag 的改动先写入 `## [Unreleased]`。
- 需要发版时，从 `Unreleased` 或“自上次 tag 以来”的累计变更整理出新的版本节。
- 版本号遵循 `major/minor/patch` 规则：大改动升主版本，新功能升次版本，修复与非破坏性维护升修订版本。
- 推荐标题格式为 `## [vX.Y.Z] - YYYY-MM-DD`，正文按 `Added` / `Changed` / `Fixed` / `Docs` / `Chore` 归纳。

## [Unreleased]

## [v1.1.4] - 2026-04-21

### Changed
- 锁定 `galay-kernel 3.4.4`、`galay-utils 1.0.3` 与 `GalayHttp 2.0.2` 的 CMake 依赖版本，避免误链接旧前缀中的基础库。
- 同步更新导出包配置中的 `find_dependency(...)` 版本约束，确保下游消费与源码构建使用同一组内部依赖版本。
