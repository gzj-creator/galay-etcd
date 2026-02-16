# API 参考

## 1. 基础类型

### `EtcdConfig`

- `endpoint`：etcd HTTP 地址，默认 `http://127.0.0.1:2379`
- `api_prefix`：API 前缀，默认 `/v3`

### `AsyncEtcdConfig`

- `request_timeout`：请求超时，默认禁用
- `buffer_size`：HTTP 会话 ring buffer，默认 `16384`
- `keepalive`：是否使用 keep-alive，默认 `true`

### `EtcdKeyValue`

- `key` / `value`
- `create_revision` / `mod_revision` / `version` / `lease`

### `EtcdError`

- 错误码包括：`INVALID_PARAM`、`NOT_CONNECTED`、`CONNECTION`、`TIMEOUT`、`HTTP`、`SERVER`、`PARSE` 等。

## 2. `AsyncEtcdClient`（异步）

说明：所有异步接口均返回自定义等待体，直接 `co_await client.xxx(...)`，不需要 `.wait()`。

### 连接管理

- `connect()`
- `close()`
- `connected() const`

### KV 能力

- `put(key, value, lease_id?)`
- `get(key, prefix=false, limit?)`
- `del(key, prefix=false)`

### 租约能力

- `grantLease(ttl_seconds)`
- `keepAliveOnce(lease_id)`

### Pipeline 能力

- `pipeline(std::vector<PipelineOp>)`
- `PipelineOp::Put(key, value, lease_id?)`
- `PipelineOp::Get(key, prefix=false, limit?)`
- `PipelineOp::Del(key, prefix=false)`
- `lastPipelineResults()`

### 最近一次结果读取

- `lastError()`
- `lastBool()`
- `lastLeaseId()`
- `lastDeletedCount()`
- `lastKeyValues()`（返回内部缓存的只读引用）
- `lastStatusCode()`
- `lastResponseBody()`（返回内部缓存的只读引用）

### 实现说明（内部）

- 连接管理使用 `TcpSocket`，不依赖 `HttpClient` 包装。
- `postJsonInternal(...)` 使用 `PostJsonAwaitable`（自定义等待体），不再使用额外 `Coroutine` 包装。
- 为避免 `HttpSessionAwaitable` move 后内部 owner 指针失效，内部直接构造 `HttpSessionAwaitable`，并由 `Context` 固定持有直到请求完成。
- JSON 解析使用 `simdjson`，替代手写字符串扫描解析。
- 请求响应解析改为“每个业务操作单次解析”：`PostJsonAwaitable` 仅处理 HTTP 层，业务层统一做 JSON 与 etcd `code/message` 校验。

## 3. `EtcdClient`（协程封装）

说明：`EtcdClient` 仅做协程接口封装，不包含 `promise/future` 阻塞桥接。

### 连接管理

- `connect() -> ConnectAwaitable`
- `close() -> CloseAwaitable`
- `connected() const`

### KV 能力

- `put(key, value, lease_id?) -> PutAwaitable`
- `get(key, prefix=false, limit?) -> GetAwaitable`
- `del(key, prefix=false) -> DeleteAwaitable`

### 租约能力

- `grantLease(ttl_seconds) -> GrantLeaseAwaitable`
- `keepAliveOnce(lease_id) -> KeepAliveAwaitable`

### Pipeline 能力

- `pipeline(std::vector<PipelineOp>) -> PipelineAwaitable`
- `lastPipelineResults()`

### 结果读取

- 与 `AsyncEtcdClient` 一致：`lastError()`、`lastBool()`、`lastLeaseId()`、`lastDeletedCount()`、`lastKeyValues()`、`lastStatusCode()`、`lastResponseBody()`
