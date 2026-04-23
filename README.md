# galay-etcd

`galay-etcd` 是一个面向 etcd v3 HTTP API 的 C++ 客户端库，提供：

- 同步阻塞客户端：`galay::etcd::EtcdClient`
- 异步协程客户端：`galay::etcd::AsyncEtcdClient`
- 共享数据模型：`EtcdConfig`、`AsyncEtcdConfig`、`EtcdKeyValue`、`PipelineOp`
- CMake 包与可选 C++ module 接口：`galay-etcd::galay-etcd`、`galay-etcd::galay-etcd-modules` / `galay.etcd`

## 文档真相来源

当 Markdown 与源码冲突时，本仓库按以下顺序认定真相：

1. 公开头文件与导出 target
2. 实现行为
3. `examples/`
4. `test/`
5. `benchmark/`
6. Markdown 文档

## 当前支持范围

- KV：`put` / `get` / `del`
- 前缀操作：`get(key, true)` / `del(key, true)`
- 租约：`grantLease(ttl_seconds)` / `keepAliveOnce(lease_id)`
- Pipeline：单次 `POST /v3/kv/txn`，请求体固定为 `compare=[]`、`success=[ops]`、`failure=[]`
- 最近一次结果访问器：`lastKeyValues()`、`lastLeaseId()`、`lastDeletedCount()`、`lastPipelineResults()`

## 当前未实现或不建议误读的能力

- `https://` endpoint 语法可被解析，但同步/异步客户端都会拒绝 TLS 连接
- 认证、Watch、官方 Lock API、Cluster / Member 管理接口当前都没有公开 API
- Pipeline 不是 compare/failure 事务 DSL；当前只暴露“空 compare + success ops”批量请求
- `config.keepalive` 是传输层 keep-alive，不等于租约续约；租约续约必须显式调用 `keepAliveOnce()`
- 客户端实例未声明为线程安全；并发场景请使用“每线程 / 每协程一个客户端”或外部串行化

## 构建前提

| 项目 | 当前要求 | 来源 |
| --- | --- | --- |
| C++ 标准 | C++23 | `cmake/option.cmake` |
| CMake | `>= 3.20` | `CMakeLists.txt` |
| 内部依赖 | `galay-kernel`、`galay-utils`、`galay-http` | `galay-etcd/CMakeLists.txt` |
| 第三方依赖 | `spdlog`、`simdjson`（通过 `pkg-config simdjson`） | `galay-etcd/CMakeLists.txt` |
| 默认构建项 | tests / benchmarks / examples 默认开启 | `cmake/option.cmake` |
| module/import 编译 | 仅在受支持工具链下启用 | `cmake/option.cmake` |

module/import 编译路径的额外条件：

- `CMake >= 3.28`
- 生成器为 `Ninja` 或 `Visual Studio`
- `Linux + GCC >= 14`，或非 `AppleClang` 且可找到 `clang-scan-deps`

## 快速构建

以下命令会构建库、示例、测试与 benchmark：

```bash
cmake -S . -B build \
  -D GALAY_ETCD_BUILD_EXAMPLES=ON \
  -D BUILD_TESTING=ON \
  -D GALAY_ETCD_BUILD_BENCHMARKS=ON

cmake --build build -j
```

本地 etcd 联通检查建议在运行示例前执行：

```bash
curl http://127.0.0.1:2379/version
```

## 运行入口

本仓库所有公开示例 / 测试 / benchmark 都通过命令行参数接收 endpoint；不要求环境变量。

```bash
# examples/
./build/examples/E1-SyncBasic http://127.0.0.1:2379
./build/examples/E2-AsyncBasic http://127.0.0.1:2379
./build/examples/E1-SyncBasicImport http://127.0.0.1:2379
./build/examples/E2-AsyncBasicImport http://127.0.0.1:2379

# test/
./build/test/T1-EtcdSmoke http://127.0.0.1:2379
./build/test/T2-EtcdPrefixOps http://127.0.0.1:2379
./build/test/T3-EtcdPipeline http://127.0.0.1:2379
./build/test/T4-AsyncEtcdSmoke http://127.0.0.1:2379
./build/test/T5-AsyncEtcdPipeline http://127.0.0.1:2379
./build/test/T6-EtcdInternalHelpers
./build/test/T7-EtcdAwaitableSurface
./build/test/T8-AsyncBenchmarkSmoke

# benchmark/
./build/benchmark/B1-EtcdKvBenchmark http://127.0.0.1:2379 8 500 64 put
./build/benchmark/B1-EtcdKvBenchmark http://127.0.0.1:2379 8 500 64 mixed
```

说明：

- `T6-EtcdInternalHelpers` 是纯本地校验，不依赖 etcd 服务
- 公开 `examples/` / `test/` / `benchmark` 在省略 `argv[1]` 时会回落到 `http://127.0.0.1:2379`；企业环境请始终显式传入自己的 endpoint
- benchmark 结果页只接受“附命令、环境和日期”的数据；旧数字若未复算，一律按历史样本处理
- 对外发布 benchmark 结论前，必须补上同机、同构建类型、同 workload 的 Rust 基线；当前统一入口是 `scripts/S2-Bench-Rust-Compare.sh`
- `BUILD_TESTS` 和 `GALAY_ETCD_BUILD_TESTS` 仍可用，但仅作为 `BUILD_TESTING` 的兼容别名

## Rust 对照 benchmark

仓库内现已提供与 `B1/B2` 对齐的 Rust 基线：

- `benchmark/compare/rust/src/bin/rust_sync_etcd_bench.rs`
- `benchmark/compare/rust/src/bin/rust_async_etcd_bench.rs`
- `scripts/S2-Bench-Rust-Compare.sh`

Rust 选型是：

- 同步：`reqwest::blocking`
- 异步：`reqwest` + `tokio`

统一运行方式：

```bash
GALAY_ETCD_ENDPOINT=http://127.0.0.1:2379 \
GALAY_ETCD_BENCH_WORKERS=8 \
GALAY_ETCD_BENCH_OPS_PER_WORKER=125 \
GALAY_ETCD_BENCH_VALUE_SIZE=32 \
GALAY_ETCD_BENCH_MODE=put \
GALAY_ETCD_BENCH_IO_SCHEDULERS=2 \
./scripts/S2-Bench-Rust-Compare.sh ./build
```

如果某个 workload 还没有 Rust 基线，就只能按 `internal-only` / 历史样本解释，不能对外宣称为公开 benchmark 结论。

## 文档导航

- [文档索引](docs/README.md)
- [00-快速开始](docs/00-快速开始.md)
- [01-架构设计](docs/01-架构设计.md)
- [02-API参考](docs/02-API参考.md)
- [03-使用指南](docs/03-使用指南.md)
- [04-示例代码](docs/04-示例代码.md)
- [05-性能测试](docs/05-性能测试.md)
- [06-高级主题](docs/06-高级主题.md)
- [07-常见问题](docs/07-常见问题.md)

## 选择入口

- 想先跑起来：看 `docs/00-快速开始.md`
- 想对 API 对账：看 `docs/02-API参考.md`
- 想查 `EtcdLog` / `EtcdLoggerPtr` / `galay::etcd::internal` / `parseEndpoint()` / `import galay.etcd` 边界：看 `docs/02-API参考.md`
- 想找可运行文件与 target：看 `docs/04-示例代码.md`
- 想看 benchmark 真正怎么跑：看 `docs/05-性能测试.md`
- 想确认已知限制和生产边界：看 `docs/06-高级主题.md`
