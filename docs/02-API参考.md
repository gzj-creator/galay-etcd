# 02-API参考

本页只记录当前安装包 / 公开头文件 / module 接口中的可见接口，并补充必要语义说明。

## 1. 公开头文件与导出接口

当前安装包会安装这些头文件：

- `galay-etcd/base/EtcdInternal.h`
- `galay-etcd/base/EtcdNetworkConfig.h`
- `galay-etcd/base/EtcdConfig.h`
- `galay-etcd/base/EtcdError.h`
- `galay-etcd/base/EtcdLog.h`
- `galay-etcd/base/EtcdValue.h`
- `galay-etcd/base/EtcdTypes.h`
- `galay-etcd/async/AsyncEtcdConfig.h`
- `galay-etcd/async/AsyncEtcdClient.h`
- `galay-etcd/module/ModulePrelude.hpp`
- `galay-etcd/module/galay.etcd.cppm`
- `galay-etcd/sync/EtcdClient.h`

安装面补充说明：

- `galay-etcd/base/EtcdInternal.h` 暴露的是 `galay::etcd::internal` 命名空间中的内联 helper；它会直接包含 `<simdjson.h>`，因此安装包 target 也会导出 `PkgConfig::SIMDJSON`
- `galay-etcd/base/EtcdLog.h` 是可选日志辅助接口，提供 `EtcdLog` / `EtcdLoggerPtr`
- `galay-etcd/module/ModulePrelude.hpp` 是 `galay-etcd/module/galay.etcd.cppm` 的 global module fragment 支撑头，不是 header 模式下的首选入口
- `galay-etcd/module/galay.etcd.cppm` 是真实模块接口文件；它回答 module 模式下的公开导出边界

构建接口：

- 子目录 target：`galay-etcd`
- 安装后 imported target：`galay-etcd::galay-etcd`
- 安装后 `find_package` 名称：`galay-etcd`
- C++ module：`galay.etcd`

`galay.etcd` 的 module 导出边界，以 `galay-etcd/module/galay.etcd.cppm` 中的 `export { ... }` 块为准：

- 会被 `import galay.etcd;` 直接导出的头：`EtcdConfig.h`、`EtcdError.h`、`EtcdValue.h`、`EtcdTypes.h`、`EtcdNetworkConfig.h`、`AsyncEtcdConfig.h`、`AsyncEtcdClient.h`、`EtcdClient.h`
- 不在当前 module 导出边界内的安装头：`EtcdLog.h`、`EtcdInternal.h`
- `ModulePrelude.hpp` 虽然在 global module fragment 中 `#include` 了更多头，但它不是额外的导出清单

因此，如果你需要日志 helper 或 `galay::etcd::internal` helper，当前应继续直接 `#include` 对应头文件，而不是只依赖 `import galay.etcd;`。

## 2. 基础类型与可选日志辅助

### `EtcdNetworkConfig`

```cpp
struct EtcdNetworkConfig {
    std::chrono::milliseconds request_timeout = std::chrono::milliseconds(-1);
    size_t buffer_size = 16384;
    bool keepalive = true;

    bool isRequestTimeoutEnabled() const;
    static EtcdNetworkConfig withTimeout(std::chrono::milliseconds timeout);
};
```

语义：

- `request_timeout < 0` 表示禁用
- `keepalive` 控制传输层 keep-alive，不是租约续约

### `EtcdConfig`

```cpp
struct EtcdConfig : EtcdNetworkConfig {
    std::string endpoint = "http://127.0.0.1:2379";
    std::string api_prefix = "/v3";

    static EtcdConfig withTimeout(std::chrono::milliseconds timeout);
};
```

### `AsyncEtcdConfig`

```cpp
struct AsyncEtcdConfig : EtcdConfig {
    static AsyncEtcdConfig withTimeout(std::chrono::milliseconds timeout);
};
```

### `EtcdKeyValue`

```cpp
struct EtcdKeyValue {
    std::string key;
    std::string value;
    int64_t create_revision = 0;
    int64_t mod_revision = 0;
    int64_t version = 0;
    int64_t lease = 0;
};
```

### `PipelineOpType`

```cpp
enum class PipelineOpType {
    Put,
    Get,
    Delete,
};
```

### `PipelineOp`

```cpp
struct PipelineOp {
    PipelineOpType type = PipelineOpType::Put;
    std::string key;
    std::string value;
    bool prefix = false;
    std::optional<int64_t> limit = std::nullopt;
    std::optional<int64_t> lease_id = std::nullopt;

    static PipelineOp Put(std::string key,
                          std::string value,
                          std::optional<int64_t> lease_id = std::nullopt);

    static PipelineOp Get(std::string key,
                          bool prefix = false,
                          std::optional<int64_t> limit = std::nullopt);

    static PipelineOp Del(std::string key, bool prefix = false);
};
```

### `PipelineItemResult`

```cpp
struct PipelineItemResult {
    PipelineOpType type = PipelineOpType::Put;
    bool ok = false;
    int64_t deleted_count = 0;
    std::vector<EtcdKeyValue> kvs;
};
```

### `EtcdErrorType` 与 `EtcdError`

```cpp
enum class EtcdErrorType {
    Success = 0,
    InvalidEndpoint,
    InvalidParam,
    NotConnected,
    Connection,
    Timeout,
    Send,
    Recv,
    Http,
    Server,
    Parse,
    Internal,
};

class EtcdError {
public:
    EtcdError(EtcdErrorType type = EtcdErrorType::Success);
    EtcdError(EtcdErrorType type, std::string extra_msg);

    EtcdErrorType type() const;
    std::string message() const;
    bool isOk() const;
};
```

### `EtcdLoggerPtr`

```cpp
using EtcdLoggerPtr = std::shared_ptr<spdlog::logger>;
```

`EtcdLoggerPtr` 是 `spdlog::logger` 的共享指针别名；它只在 `galay-etcd/base/EtcdLog.h` 中声明，当前不属于 `galay.etcd` module 的直接导出内容。

### `EtcdLog`

```cpp
class EtcdLog {
public:
    static EtcdLog* getInstance();
    static void enable();
    static void console();
    static void console(const std::string& logger_name);
    static void file(const std::string& log_file_path = "galay-etcd.log",
                     const std::string& logger_name = "EtcdLogger",
                     bool truncate = false);
    static void disable();
    static void setLogger(EtcdLoggerPtr logger);
    EtcdLoggerPtr getLogger() const;
};
```

方法语义：

- `getInstance()` 返回进程内单例的裸指针；读取当前 logger 时需要调用 `EtcdLog::getInstance()->getLogger()`
- `enable()` 只是 `console()` 的别名
- `console()` 使用默认 logger 名 `EtcdLogger`
- `console(logger_name)` 会先尝试 `spdlog::get(logger_name)`，找不到时再创建彩色控制台 logger；成功拿到 logger 后会套用库内默认 pattern / level
- `file(log_file_path, logger_name, truncate)` 会创建一个新的文件 logger，并套用库内默认 pattern / level
- `disable()` 会先把当前 logger level 设为 `off`，再清空单例里保存的指针
- `setLogger(logger)` 只替换单例里保存的共享指针；它不会额外调用默认 pattern / level 配置
- `getLogger()` 在互斥锁保护下返回当前 `EtcdLoggerPtr` 的拷贝

使用边界：

- 这是**独立的可选 helper**；当前 `EtcdClient` / `AsyncEtcdClient` 的公开实现不会自动从这个单例中取 logger
- 如果你选择 module 方式接入主 API，日志 helper 仍需直接 `#include "galay-etcd/base/EtcdLog.h"`

## 3. 同步客户端

`galay-etcd/sync/EtcdClient.h` 中的结果类型：

```cpp
using EtcdVoidResult = std::expected<void, EtcdError>;
```

### `EtcdClientBuilder`

```cpp
class EtcdClientBuilder {
public:
    EtcdClientBuilder& endpoint(std::string endpoint);
    EtcdClientBuilder& apiPrefix(std::string prefix);
    EtcdClientBuilder& requestTimeout(std::chrono::milliseconds timeout);
    EtcdClientBuilder& bufferSize(size_t size);
    EtcdClientBuilder& keepAlive(bool enabled);
    EtcdClientBuilder& config(EtcdConfig config);
    EtcdClient build() const;
    EtcdConfig buildConfig() const;
};
```

### `EtcdClient`

```cpp
class EtcdClient {
public:
    using PipelineOpType = galay::etcd::PipelineOpType;
    using PipelineOp = galay::etcd::PipelineOp;
    using PipelineItemResult = galay::etcd::PipelineItemResult;

    explicit EtcdClient(EtcdConfig config = {});
    ~EtcdClient();

    EtcdClient(const EtcdClient&) = delete;
    EtcdClient& operator=(const EtcdClient&) = delete;
    EtcdClient(EtcdClient&&) = delete;
    EtcdClient& operator=(EtcdClient&&) = delete;

    EtcdVoidResult connect();
    EtcdVoidResult close();

    EtcdVoidResult put(const std::string& key,
                       const std::string& value,
                       std::optional<int64_t> lease_id = std::nullopt);

    EtcdVoidResult get(const std::string& key,
                       bool prefix = false,
                       std::optional<int64_t> limit = std::nullopt);

    EtcdVoidResult del(const std::string& key, bool prefix = false);
    EtcdVoidResult grantLease(int64_t ttl_seconds);
    EtcdVoidResult keepAliveOnce(int64_t lease_id);
    EtcdVoidResult pipeline(std::span<const PipelineOp> operations);
    EtcdVoidResult pipeline(std::vector<PipelineOp> operations);

    bool connected() const;
    EtcdError lastError() const;
    bool lastBool() const;
    int64_t lastLeaseId() const;
    int64_t lastDeletedCount() const;
    const std::vector<EtcdKeyValue>& lastKeyValues() const;
    const std::vector<PipelineItemResult>& lastPipelineResults() const;
    int lastStatusCode() const;
    const std::string& lastResponseBody() const;
};
```

生命周期与所有权：

- `EtcdClient` 是**非拷贝、非移动**的状态型对象；需要把它放在最终使用位置上，而不是指望后续按值搬运
- `EtcdClientBuilder::build()` 通过直接构造返回一个 prvalue；常见用法是立刻绑定到局部变量或类成员
- 析构函数会在 socket 仍然打开时执行关闭；生产代码里仍建议显式调用 `close()`，让关闭点更可控

语义补充：

- `connect()` 会校验 endpoint，并在配置了 `request_timeout` 时把它应用到同步建连与后续 socket 收发超时
- endpoint 解析接受 `https://...` 语法，但当前 `EtcdClient` 在构造期会把 secure endpoint 记为错误；`connect()` 时返回 `InvalidEndpoint`
- `connect()` 在“已经连接”时直接成功返回，不重复建连
- `close()` 在“已经关闭”时也会直接成功返回
- `get(..., true, limit)` 与 `del(..., true)` 使用 etcd 前缀 range 语义
- `keepAliveOnce()` 在未开启 `request_timeout` 时，会对这次续约请求使用固定 5 秒超时
- `pipeline()` 是固定格式的 txn 批量请求：`compare=[]`、`failure=[]`，只公开 success 分支

## 4. 异步客户端

`galay-etcd/async/AsyncEtcdClient.h` 中公开了这些结果类型：

```cpp
using EtcdVoidResult = std::expected<void, EtcdError>;
using EtcdGetResult = std::expected<std::vector<EtcdKeyValue>, EtcdError>;
using EtcdDeleteResult = std::expected<int64_t, EtcdError>;
using EtcdLeaseGrantResult = std::expected<int64_t, EtcdError>;
```

结果别名补充说明：

- `EtcdGetResult`、`EtcdDeleteResult`、`EtcdLeaseGrantResult` 描述的是结构化 payload 的形状
- 当前公开 awaitable 在 `co_await` 后仍统一产出 `EtcdVoidResult`
- 结构化结果通过 `lastKeyValues()`、`lastDeletedCount()`、`lastLeaseId()` 等最近结果访问器暴露

### `AsyncEtcdClientBuilder`

```cpp
class AsyncEtcdClientBuilder {
public:
    AsyncEtcdClientBuilder& scheduler(galay::kernel::IOScheduler* scheduler);
    AsyncEtcdClientBuilder& endpoint(std::string endpoint);
    AsyncEtcdClientBuilder& apiPrefix(std::string prefix);
    AsyncEtcdClientBuilder& requestTimeout(std::chrono::milliseconds timeout);
    AsyncEtcdClientBuilder& bufferSize(size_t size);
    AsyncEtcdClientBuilder& keepAlive(bool enabled);
    AsyncEtcdClientBuilder& config(AsyncEtcdConfig config);
    AsyncEtcdClient build() const;
    AsyncEtcdConfig buildConfig() const;
};
```

### `AsyncEtcdClient`

```cpp
class AsyncEtcdClient {
public:
    template <typename AwaitableType>
    class IoAwaitableBase;

    class ConnectAwaitable;
    class CloseAwaitable;
    class PostJsonAwaitable;
    class JsonOpAwaitableBase;
    class PutAwaitable;
    class GetAwaitable;
    class DeleteAwaitable;
    class GrantLeaseAwaitable;
    class KeepAliveAwaitable;
    class PipelineAwaitable;

    using PipelineOpType = galay::etcd::PipelineOpType;
    using PipelineOp = galay::etcd::PipelineOp;
    using PipelineItemResult = galay::etcd::PipelineItemResult;

    AsyncEtcdClient(galay::kernel::IOScheduler* scheduler,
                    AsyncEtcdConfig config = {});

    AsyncEtcdClient(const AsyncEtcdClient&) = delete;
    AsyncEtcdClient& operator=(const AsyncEtcdClient&) = delete;
    AsyncEtcdClient(AsyncEtcdClient&&) = delete;
    AsyncEtcdClient& operator=(AsyncEtcdClient&&) = delete;

    ConnectAwaitable connect();
    CloseAwaitable close();

    PutAwaitable put(const std::string& key,
                     const std::string& value,
                     std::optional<int64_t> lease_id = std::nullopt);

    GetAwaitable get(const std::string& key,
                     bool prefix = false,
                     std::optional<int64_t> limit = std::nullopt);

    DeleteAwaitable del(const std::string& key, bool prefix = false);
    GrantLeaseAwaitable grantLease(int64_t ttl_seconds);
    KeepAliveAwaitable keepAliveOnce(int64_t lease_id);
    PipelineAwaitable pipeline(std::span<const PipelineOp> operations);
    PipelineAwaitable pipeline(std::vector<PipelineOp> operations);

    bool connected() const;
    EtcdError lastError() const;
    bool lastBool() const;
    int64_t lastLeaseId() const;
    int64_t lastDeletedCount() const;
    const std::vector<EtcdKeyValue>& lastKeyValues() const;
    const std::vector<PipelineItemResult>& lastPipelineResults() const;
    int lastStatusCode() const;
    const std::string& lastResponseBody() const;
};
```

生命周期与所有权：

- `AsyncEtcdClient` 也是**非拷贝、非移动**的状态型对象
- 各个公开 awaitable 内部都持有 `AsyncEtcdClient*`；因此 client 实例必须活到对应 `co_await` 完成之后
- `AsyncEtcdClientBuilder::build()` 同样返回 prvalue，建议直接绑定到最终变量 / 成员

公开 nested base 的定位：

- `IoAwaitableBase<AwaitableType>` 是 `ConnectAwaitable` / `CloseAwaitable` 的共享基类
- `JsonOpAwaitableBase` 是 `Put/Get/Delete/GrantLease/KeepAlive/Pipeline` 的共享基类
- 它们都在 public 区域中声明，但常规调用入口仍是各个具体 awaitable

### `PostJsonAwaitable`

```cpp
class PostJsonAwaitable {
public:
    PostJsonAwaitable(AsyncEtcdClient& client,
                      std::string api_path,
                      std::string body,
                      std::optional<std::chrono::milliseconds> force_timeout);

    PostJsonAwaitable(const PostJsonAwaitable&) = delete;
    PostJsonAwaitable& operator=(const PostJsonAwaitable&) = delete;
    PostJsonAwaitable(PostJsonAwaitable&&) noexcept = default;
    PostJsonAwaitable& operator=(PostJsonAwaitable&&) noexcept = default;
    ~PostJsonAwaitable();

    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> handle);
    EtcdVoidResult await_resume();
};
```

语义补充：

- 这是一个**公开但偏底层**的 HTTP JSON POST awaitable；`AsyncEtcdClient` 的具体业务 awaitable 都通过它发起请求
- `api_path` 会与 client 当前的 `api_prefix` 拼接，形成最终请求路径
- 如果 client 尚未连接，构造函数会把 `lastError()` 设为 `NotConnected`，并让 awaitable 立即就绪
- `force_timeout` 有值时优先使用它；否则在启用了 `request_timeout` 时沿用配置超时
- `await_suspend()` 直接转发到底层 `HttpSessionAwaitable`
- `await_resume()` 会映射 HTTP / kernel 错误到 `EtcdError`，并在收到完整响应后写入 `lastStatusCode()` / `lastResponseBody()`
- HTTP 状态码不在 `2xx` 区间时，`await_resume()` 直接返回 `EtcdErrorType::Server`

### 公开 awaitable 的 `await_*` 语义

#### `ConnectAwaitable`

- 来源：`client.connect()`
- `await_ready()`：当 `IOScheduler*` 为空、endpoint 无效、或 client 已经连上并持有 socket / session 时立即返回 `true`
- `await_suspend()`：把协程挂到 `TcpSocket::connect(...)`
- `await_resume()`：
  - 若前面没有真正启动 I/O，则直接回放当前 `EtcdVoidResult`
  - I/O 成功后创建 `HttpSession`、置 `connected() = true`
  - I/O 失败或建 `HttpSession` 失败时，返回映射后的 `EtcdError`

#### `CloseAwaitable`

- 来源：`client.close()`
- `await_ready()`：当 client 当前没有 socket 时立即返回 `true`
- `await_suspend()`：把协程挂到 `TcpSocket::close()`
- `await_resume()`：
  - 如果底层 close 出错，返回映射后的连接错误
  - 无论成功还是失败，都会重置 `HttpSession` / socket，并把 `connected() = false`

#### `PutAwaitable` / `GetAwaitable` / `DeleteAwaitable` / `GrantLeaseAwaitable` / `KeepAliveAwaitable` / `PipelineAwaitable`

它们共享同一套 await 行为框架：

- 构造函数都会先清空最近结果缓存，再调用 request builder 生成 JSON body
- 如果参数校验或 request body 生成失败，构造阶段就会设置 `lastError()`，并让 awaitable 立即就绪
- `await_ready()` / `await_suspend()` 统一委托给内部 `PostJsonAwaitable`
- `await_resume()` 都会先完成 HTTP POST，再做各自的响应解析与 `last*()` 缓存回填

各个具体 awaitable 的差异如下：

- `PutAwaitable`
  - 路径：`/kv/put`
  - 解析：`parsePutResponse(...)`
  - 成功副作用：`lastBool() == true`
- `GetAwaitable`
  - 路径：`/kv/range`
  - 解析：`parseGetResponseKvs(...)`
  - 成功副作用：写入 `lastKeyValues()`，并把 `lastBool()` 设为“结果是否非空”
- `DeleteAwaitable`
  - 路径：`/kv/deleterange`
  - 解析：`parseDeleteResponseDeletedCount(...)`
  - 成功副作用：写入 `lastDeletedCount()`，并把 `lastBool()` 设为“删除数是否大于 0”
- `GrantLeaseAwaitable`
  - 路径：`/lease/grant`
  - 解析：`parseLeaseGrantResponseId(...)`
  - 成功副作用：写入 `lastLeaseId()`，并把 `lastBool()` 设为 `true`
- `KeepAliveAwaitable`
  - 路径：`/lease/keepalive`
  - 解析：`parseLeaseKeepAliveResponseId(..., expected_lease_id)`
  - 成功副作用：写入 `lastLeaseId()`，并把 `lastBool()` 设为 `true`
  - 超时补充：当 `request_timeout` 没有启用时，这个 awaitable 会对本次请求强制使用 5 秒超时
- `PipelineAwaitable`
  - 路径：`/kv/txn`
  - 解析：`parsePipelineTxnResponse(...)`
  - 成功副作用：写入 `lastPipelineResults()`，并把 `lastBool()` 设为 `true`
  - 额外状态：构造时会捕获一份 `PipelineOpType` 列表，用于在响应阶段按操作顺序解释每一项 txn 返回

## 5. `galay::etcd::internal` header-only surface

`galay-etcd/base/EtcdInternal.h` 是已安装的 header-only helper 集合，命名空间为 `galay::etcd::internal`。

使用边界：

- 当前它**不在** `galay.etcd` module 的导出边界里
- 所有函数都是 inline / header-only 形式；适合测试、适配层或自定义封装直接复用
- 同一头里还有一些更底层的数字 / 字符串 / base64 / simdjson 辅助函数；下面优先列出最适合作为外部调用入口的分组 API

### endpoint / prefix 相关 helper

```cpp
std::string normalizeApiPrefix(std::string prefix);

struct ParsedEndpoint {
    std::string host;
    uint16_t port = 0;
    bool secure = false;
    bool ipv6 = false;
};

std::expected<ParsedEndpoint, std::string> parseEndpoint(const std::string& endpoint);
std::string buildHostHeader(const std::string& host, uint16_t port, bool ipv6);
```

语义补充：

- `normalizeApiPrefix()` 会补齐前导 `/`，并移除尾部多余 `/`；空字符串会规范成 `/v3`
- `parseEndpoint()` 负责从 `http://...` / `https://...` endpoint 中解析 host、port、scheme 与 IPv6 标记，并在未显式给出端口时自动补 `80` / `443`
- `parseEndpoint()` 可以把 `https://...` 解析成 `secure = true`；但这不等于客户端真的支持 TLS，当前同步 / 异步客户端随后都会拒绝 secure endpoint
- `buildHostHeader()` 会按 IPv4 / IPv6 形式生成 HTTP `Host` 头值

### request body builder

```cpp
std::expected<std::string, EtcdError> buildPutRequestBody(
    std::string_view key,
    std::string_view value,
    std::optional<int64_t> lease_id = std::nullopt);

std::expected<std::string, EtcdError> buildGetRequestBody(
    std::string_view key,
    bool prefix = false,
    std::optional<int64_t> limit = std::nullopt);

std::expected<std::string, EtcdError> buildDeleteRequestBody(
    std::string_view key,
    bool prefix = false);

std::expected<std::string, EtcdError> buildLeaseGrantRequestBody(int64_t ttl_seconds);
std::expected<std::string, EtcdError> buildLeaseKeepAliveRequestBody(int64_t lease_id);
std::expected<std::string, EtcdError> buildTxnBody(std::span<const PipelineOp> operations);
std::expected<std::string, EtcdError> buildTxnBody(const std::vector<PipelineOp>& operations);
```

语义补充：

- 这些 builder 都返回**最终 JSON body 字符串**，不负责拼完整 HTTP request
- `buildPutRequestBody()` 要求 `key` 非空；`lease_id` 提供时必须为正数
- `buildGetRequestBody()` 要求 `key` 非空；`limit` 提供时必须为正数；`prefix=true` 时会自动生成 etcd range end
- `buildDeleteRequestBody()` 要求 `key` 非空；`prefix=true` 时同样生成 range end
- `buildLeaseGrantRequestBody()` 要求 `ttl_seconds > 0`
- `buildLeaseKeepAliveRequestBody()` 要求 `lease_id > 0`
- `buildTxnBody()` 要求操作列表非空，且每个 `PipelineOp` 的 `key` 非空；如果给了 `limit` / `lease_id`，也都必须为正数
- `buildTxnBody()` 当前固定生成 `{"compare":[],"success":[...],"failure":[]}` 结构；这正是当前 pipeline API 没有公开 compare / failure DSL 的原因

### response parser

```cpp
std::expected<simdjson::dom::object, EtcdError> parseEtcdSuccessObject(
    const std::string& body,
    const std::string& context);

std::expected<std::vector<EtcdKeyValue>, EtcdError> parseKvsFromObject(
    const simdjson::dom::object& object,
    const std::string& context);

std::expected<std::vector<PipelineItemResult>, EtcdError> parsePipelineResponses(
    const simdjson::dom::object& root,
    std::span<const PipelineOpType> operation_types);

std::expected<std::vector<PipelineItemResult>, EtcdError> parsePipelineResponses(
    const simdjson::dom::object& root,
    std::span<const PipelineOp> operations);

std::expected<void, EtcdError> parsePutResponse(const std::string& body);
std::expected<std::vector<EtcdKeyValue>, EtcdError> parseGetResponseKvs(const std::string& body);
std::expected<int64_t, EtcdError> parseDeleteResponseDeletedCount(const std::string& body);
std::expected<int64_t, EtcdError> parseLeaseGrantResponseId(const std::string& body);
std::expected<int64_t, EtcdError> parseLeaseKeepAliveResponseId(
    const std::string& body,
    int64_t expected_lease_id);

std::expected<std::vector<PipelineItemResult>, EtcdError> parsePipelineTxnResponse(
    const std::string& body,
    std::span<const PipelineOpType> operation_types);

std::expected<std::vector<PipelineItemResult>, EtcdError> parsePipelineTxnResponse(
    const std::string& body,
    std::span<const PipelineOp> operations);
```

语义补充：

- `parseEtcdSuccessObject()` 是通用入口：把 body 解析为 JSON object，并把 simdjson 错误映射成 `EtcdErrorType::Parse`
- `parseKvsFromObject()` 从 etcd 返回对象中解码 `kvs` 数组，生成 `std::vector<EtcdKeyValue>`
- `parsePutResponse()` 当前只在 body 看起来包含 etcd error 字段时进一步做对象解析；普通成功 body 会直接视为成功
- `parseGetResponseKvs()` / `parseDeleteResponseDeletedCount()` / `parseLeaseGrantResponseId()` / `parseLeaseKeepAliveResponseId()` 分别对应公开的 `get` / `del` / `grantLease` / `keepAliveOnce` 解析语义
- `parseLeaseGrantResponseId()` 要求响应里存在 `ID` 字段；缺失时返回 `Parse` 错误
- `parseLeaseKeepAliveResponseId()` 如果响应里带了 `ID` 且它与期望租约 ID 不一致，会返回 `Parse` 错误；否则返回期望租约 ID
- `parsePipelineResponses()` / `parsePipelineTxnResponse()` 会检查：
  - `succeeded` 字段若显式为 `false`，则按 `Server` 错误返回
  - `responses` 数组是否存在，且长度是否与操作数一致
  - 每一项是否含有与操作类型匹配的 `response_put` / `response_range` / `response_delete_range`

## 6. 结果访问器的使用规则

最近结果访问器的共同规则：

- 每次 `connect/close/put/get/del/grantLease/keepAliveOnce/pipeline` 开始前，客户端都会清空上一轮 `last*()` 状态
- 它们表示“最近一次操作”的状态，不是历史日志
- 成功后再读取，例如 `put()` 成功后读 `lastBool()`
- `get()` / `pipeline()` 成功后再读 `lastKeyValues()` / `lastPipelineResults()`
- `grantLease()` / `keepAliveOnce()` 成功后再读 `lastLeaseId()`
- `del()` 成功后再读 `lastDeletedCount()`

`lastBool()` 的当前语义：

- `put()` / `grantLease()` / `keepAliveOnce()` / `pipeline()` 成功后置为 `true`
- `get()` 成功后表示“最近一次查询结果是否非空”
- `del()` 成功后表示“最近一次删除数是否大于 0”

## 7. 调用顺序、返回与失败语义

两条客户端路径都遵循同一套业务语义：

- 在第一次 `put/get/del/grantLease/keepAliveOnce/pipeline` 之前先完成 `connect()`
- 同步路径直接返回 `EtcdVoidResult`
- 异步路径的公开 awaitable 在 `co_await` 后同样产出 `EtcdVoidResult`
- 需要结构化结果时，不是从 `EtcdVoidResult` 里直接取，而是从最近一次成功操作对应的 `last*()` 访问器读取

失败语义：

- 参数、endpoint、网络、HTTP、解析、服务端错误都统一落到 `EtcdError`
- `lastStatusCode()` / `lastResponseBody()` 适合定位 HTTP / gRPC-JSON 网关层问题
- `keepalive` 只控制传输层连接保持，不自动替代 etcd 租约续约
- 异步请求类 awaitable 如果在构造阶段就发现参数错误 / 未连接 / endpoint 无效，可能根本不会挂起协程，而是直接在 `await_ready()` / `await_resume()` 路径返回错误

生命周期与共享边界：

- `EtcdClient` / `AsyncEtcdClient` 都是**有内部最近结果缓存的状态型对象**
- 如果多个线程 / 协程 / 调用方共享同一个客户端实例，后续请求会覆盖前一次 `last*()` 结果
- 需要稳定审计单次调用结果时，应在操作成功后立即复制 `lastKeyValues()` / `lastPipelineResults()` / `lastResponseBody()`
- `AsyncEtcdClient` 不能早于其未完成的 awaitable 销毁，因为这些 awaitable 内部保存的是原始 client 指针

## 8. 交叉验证入口

- 同步基础示例：`examples/E1-sync_basic.cc`
- 异步基础示例：`examples/E2-async_basic.cc`
- 测试入口统一位于 `test/`，用于交叉验证同步、异步、prefix 与 pipeline 语义
- 同步 smoke：`test/T1-etcd_smoke.cc`
- prefix / range 语义：`test/T2-etcd_prefix_ops.cc`
- pipeline 语义：`test/T3-etcd_pipeline.cc`、`test/T5-async_etcd_pipeline.cc`
- 异步 smoke：`test/T4-async_etcd_smoke.cc`
- 内部 helper / parser 交叉验证：`test/T6-etcd_internal_helpers.cc`
