# galay-etcd

基于 `galay-kernel + galay-http` 的 etcd v3 客户端库，提供协程异步 API（对齐 `galay-redis` 范式）。

## 功能

- KV：`put / get / delete`
- Pipeline：`pipeline(txn success ops)`
- 前缀操作：`prefix get / prefix delete`
- 租约：`lease grant / keepalive`
- 异步客户端接口：`AsyncEtcdClient`
- 协程客户端封装：`EtcdClient`（无 `promise/future` 阻塞桥接）
- 传输层范式：`TcpSocket + HttpSessionAwaitable`（不依赖 `HttpClient` 封装）
- 错误模型：`std::expected<T, EtcdError>`

## 目录

- `galay-etcd/`：库源码
- `test/`：功能验证程序
- `benchmark/`：压测程序
- `docs/`：文档

## 构建

### 依赖

- Galay 内部依赖（构建必需 + 联调推荐）：
  - `galay-kernel`（构建必需）
  - `galay-utils`（推荐）
  - `galay-http`（推荐）
- 第三方依赖：
  - C++23 编译器
  - CMake 3.20+
  - `simdjson`（本项目通过 `pkg-config simdjson` 查找）
  - `spdlog`

### 依赖安装（macOS / Homebrew）

```bash
brew install cmake spdlog simdjson pkg-config
```

### 依赖安装（Ubuntu / Debian）

```bash
sudo apt-get update
sudo apt-get install -y cmake g++ libspdlog-dev libsimdjson-dev pkg-config
```

### 拉取源码（统一联调推荐）

```bash
git clone https://github.com/gzj-creator/galay-kernel.git
git clone https://github.com/gzj-creator/galay-utils.git
git clone https://github.com/gzj-creator/galay-http.git
git clone https://github.com/gzj-creator/galay-etcd.git
```

仅单独构建 `galay-etcd` 时，最小内部依赖为 `galay-kernel` 和 `galay-http`。

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

常用选项：

- `-DGALAY_ETCD_BUILD_TESTS=ON/OFF`
- `-DGALAY_ETCD_BUILD_BENCHMARKS=ON/OFF`
- `-DGALAY_ETCD_BUILD_SHARED_LIBS=ON/OFF`
- `-DGALAY_ETCD_ENABLE_IMPORT_COMPILATION=ON/OFF`

## 功能测试

远端 etcd 示例地址（可覆盖）：`http://140.143.142.251:2379`

```bash
# 脚本化 smoke（curl 端到端）
./scripts/manual_smoke.sh

# 使用库执行 smoke
./build/test/T1-EtcdSmoke http://140.143.142.251:2379

# 前缀能力测试
./build/test/T2-EtcdPrefixOps http://140.143.142.251:2379

# pipeline 能力测试
./build/test/T3-EtcdPipeline http://140.143.142.251:2379
```

## 压测

```bash
# 参数：
# 1 endpoint, 2 threads, 3 ops_per_thread, 4 value_size, 5 mode(put|mixed)
./build/benchmark/B1-EtcdKvBenchmark http://140.143.142.251:2379 8 500 64 put
./build/benchmark/B1-EtcdKvBenchmark http://140.143.142.251:2379 8 300 128 mixed
```

输出包含：

- 总请求数、成功/失败数
- 总耗时、吞吐（ops/s）
- 延迟 `p50/p95/p99/max`（微秒）

## 验收记录（2026-02-16）

真实环境：`http://140.143.142.251:2379`

```bash
./build/test/T1-EtcdSmoke http://140.143.142.251:2379
./build/test/T2-EtcdPrefixOps http://140.143.142.251:2379
./build/benchmark/B1-EtcdKvBenchmark http://140.143.142.251:2379 8 200 64 put
./build/benchmark/B1-EtcdKvBenchmark http://140.143.142.251:2379 8 100 64 mixed
```

结果摘要：

- `T1-EtcdSmoke`：通过（put/get/delete + lease 生命周期通过）
- `T2-EtcdPrefixOps`：通过（prefix range/delete 通过）
- `put` 压测：
  - 优化前样本：`83.91 ops/s`，`p95=117344us`
  - 优化后样本：`85.61 ops/s`，`p95=115872us`
- `mixed` 压测：
  - 优化前样本：`43.95 ops/s`，`p95=200679us`
  - 优化后样本：`41.80 ~ 42.50 ops/s`，`p95=214396 ~ 218517us`（远端环境抖动较大）
- 去除 `promise/future` 阻塞桥接后（协程 worker）：
  - `put`: `211.276 ops/s`，`p95=38662us`
  - `mixed`: `115.01 ops/s`，`p95=72786us`
- 进一步优化（复用 `HttpSession` + `put` 快路径跳过无效 JSON parse）后：
  - `put`: `228.228 ~ 241.038 ops/s`，`p95=35408 ~ 34158us`
  - `mixed`: `111.685 ~ 113.746 ops/s`，`p95=72775 ~ 70858us`
- 传输层重构（去 `HttpClient`，改 `TcpSocket + HttpSessionAwaitable`）后：
  - 功能测试：`T1/T2` 均通过
  - `put`（8 workers）：`227.587 ~ 228.914 ops/s`，`p95=36111 ~ 35834us`
  - `mixed`（8 workers）：`116.151 ops/s`，`p95=68661us`
  - `put`（32 workers）：`608.457 ops/s`，`p95=39222us`

## 文档

- [快速开始](docs/01-快速开始.md)
- [API 参考](docs/02-API参考.md)
- [测试与压测](docs/03-测试与压测.md)
