#include "etcd_client.h"

#include "galay-etcd/base/etcd_internal.h"

#include <galay-http/protoc/http/http_error.h>

#include <exception>
#include <utility>

namespace galay::etcd
{

using namespace internal;

namespace
{

EtcdError mapHttpError(const galay::http::HttpError& error)
{
    using galay::http::kConnectionClose;
    using galay::http::kRecvTimeOut;
    using galay::http::kRecvError;
    using galay::http::kRequestTimeOut;
    using galay::http::kSendError;
    using galay::http::kSendTimeOut;
    using galay::http::kTcpConnectError;
    using galay::http::kTcpRecvError;
    using galay::http::kTcpSendError;

    switch (error.code()) {
    case kRequestTimeOut:
    case kSendTimeOut:
    case kRecvTimeOut:
        return EtcdError(EtcdErrorType::Timeout, error.message());
    case kTcpConnectError:
        return EtcdError(EtcdErrorType::Connection, error.message());
    case kTcpSendError:
    case kSendError:
        return EtcdError(EtcdErrorType::Send, error.message());
    case kTcpRecvError:
    case kRecvError:
        return EtcdError(EtcdErrorType::Recv, error.message());
    case kConnectionClose:
        return EtcdError(EtcdErrorType::Connection, error.message());
    default:
        return EtcdError(EtcdErrorType::Http, error.message());
    }
}

EtcdError mapKernelIoError(const galay::kernel::IOError& error,
                           EtcdErrorType fallback = EtcdErrorType::Connection)
{
    using galay::kernel::IOError;
    using galay::kernel::kConnectFailed;
    using galay::kernel::kDisconnectError;
    using galay::kernel::kNotRunningOnIOScheduler;
    using galay::kernel::kRecvFailed;
    using galay::kernel::kSendFailed;
    using galay::kernel::kTimeout;

    if (IOError::contains(error.code(), kTimeout)) {
        return EtcdError(EtcdErrorType::Timeout, error.message());
    }
    if (IOError::contains(error.code(), kSendFailed)) {
        return EtcdError(EtcdErrorType::Send, error.message());
    }
    if (IOError::contains(error.code(), kRecvFailed)) {
        return EtcdError(EtcdErrorType::Recv, error.message());
    }
    if (IOError::contains(error.code(), kConnectFailed) ||
        IOError::contains(error.code(), kDisconnectError) ||
        IOError::contains(error.code(), kNotRunningOnIOScheduler)) {
        return EtcdError(EtcdErrorType::Connection, error.message());
    }
    return EtcdError(fallback, error.message());
}

galay::kernel::IOController& invalidController()
{
    static galay::kernel::IOController controller(GHandle::invalid());
    return controller;
}

std::string_view trimLeadingSlash(std::string_view path)
{
    while (!path.empty() && path.front() == '/') {
        path.remove_prefix(1);
    }
    return path;
}

} // namespace

AsyncEtcdClient::AsyncEtcdClient(galay::kernel::IOScheduler* scheduler,
                                 AsyncEtcdConfig config)
    : m_scheduler(scheduler)
    , m_config(std::move(config))
    , m_network_config(m_config)
    , m_api_prefix(normalizeApiPrefix(m_config.api_prefix))
{
    auto endpoint_result = parseEndpoint(m_config.endpoint);
    if (!endpoint_result.has_value()) {
        m_endpoint_error = endpoint_result.error();
        return;
    }

    if (endpoint_result->secure) {
        m_endpoint_error = "https endpoint is not supported in AsyncEtcdClient: " + m_config.endpoint;
        return;
    }

    m_ip_type = endpoint_result->ipv6 ? galay::kernel::IPType::IPV6 : galay::kernel::IPType::IPV4;
    m_server_host.emplace(m_ip_type, endpoint_result->host, endpoint_result->port);
    m_host_header = buildHostHeader(endpoint_result->host, endpoint_result->port, endpoint_result->ipv6);
    m_serialized_request_prefix = "POST " + m_api_prefix + "/";
    m_serialized_request_headers =
        " HTTP/1.1\r\n"
        "Host: " + m_host_header + "\r\n"
        "Accept: application/json\r\n"
        "Connection: " + std::string(m_network_config.keepalive ? "keep-alive" : "close") + "\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: ";
    m_endpoint_valid = true;
}

AsyncEtcdClient::PostJsonAwaitable::Context::Context(AsyncEtcdClient& client,
                                                     std::string api_path,
                                                     std::string body)
    : owner(&client)
    , awaitable(client.m_http_session->sendSerializedRequest(
          client.buildSerializedPostRequest(api_path, body)))
{
}

AsyncEtcdClient::PostJsonAwaitable::PostJsonAwaitable(AsyncEtcdClient& client,
                                                 std::string api_path,
                                                 std::string body,
                                                 std::optional<std::chrono::milliseconds> force_timeout)
    : m_ctx(std::nullopt)
{
    if (!client.m_connected || client.m_socket == nullptr || client.m_http_session == nullptr) {
        client.setError(EtcdErrorType::NotConnected, "etcd client is not connected");
        return;
    }

    m_ctx.emplace(client, std::move(api_path), std::move(body));

    if (force_timeout.has_value()) {
        m_ctx->awaitable.timeout(force_timeout.value());
    } else if (client.m_network_config.isRequestTimeoutEnabled()) {
        m_ctx->awaitable.timeout(client.m_network_config.request_timeout);
    }
}

bool AsyncEtcdClient::PostJsonAwaitable::await_ready() const noexcept
{
    return !m_ctx.has_value();
}

EtcdVoidResult AsyncEtcdClient::PostJsonAwaitable::await_resume()
{
    if (!m_ctx.has_value()) {
        return std::unexpected(EtcdError(EtcdErrorType::NotConnected, "etcd client is not connected"));
    }

    auto response_result = m_ctx->awaitable.await_resume();
    if (!response_result.has_value()) {
        const auto mapped = mapHttpError(response_result.error());
        m_ctx->owner->setError(mapped);
        return std::unexpected(mapped);
    }

    if (!response_result->has_value()) {
        EtcdError error(EtcdErrorType::Internal, "http response incomplete");
        m_ctx->owner->setError(error);
        return std::unexpected(error);
    }

    auto response = std::move(response_result->value());
    m_ctx->owner->m_last_status_code = static_cast<int>(response.header().code());
    m_ctx->owner->m_last_response_body = response.getBodyStr();

    if (m_ctx->owner->m_last_status_code < 200 || m_ctx->owner->m_last_status_code >= 300) {
        EtcdError error(
            EtcdErrorType::Server,
            "HTTP status=" + std::to_string(m_ctx->owner->m_last_status_code) +
            ", body=" + m_ctx->owner->m_last_response_body);
        m_ctx->owner->setError(error);
        return std::unexpected(error);
    }

    return {};
}

AsyncEtcdClient::JsonOpAwaitableBase::JsonOpAwaitableBase(AsyncEtcdClient& client)
    : m_client(&client)
{
}

void AsyncEtcdClient::JsonOpAwaitableBase::startPost(
    std::string api_path,
    std::string body,
    std::optional<std::chrono::milliseconds> force_timeout)
{
    m_post_awaitable.emplace(*m_client, std::move(api_path), std::move(body), force_timeout);
}

bool AsyncEtcdClient::JsonOpAwaitableBase::awaitReady() const noexcept
{
    return !m_post_awaitable.has_value() || m_post_awaitable->await_ready();
}

EtcdVoidResult AsyncEtcdClient::JsonOpAwaitableBase::resumePost()
{
    return m_client->resumePostOrCurrent(m_post_awaitable);
}

AsyncEtcdClient::PutAwaitable::PutAwaitable(AsyncEtcdClient& client,
                                       std::string key,
                                       std::string value,
                                       std::optional<int64_t> lease_id)
    : JsonOpAwaitableBase(client)
{
    m_client->resetLastOperation();
    auto body = buildPutRequestBody(key, value, lease_id);
    if (!body.has_value()) {
        m_client->setError(body.error());
        return;
    }

    startPost("/kv/put", std::move(body.value()));
}

bool AsyncEtcdClient::PutAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

EtcdVoidResult AsyncEtcdClient::PutAwaitable::await_resume()
{
    auto result = resumePost();
    if (!result.has_value()) {
        return result;
    }

    auto put_result = parsePutResponse(m_client->m_last_response_body);
    if (!put_result.has_value()) {
        m_client->setError(put_result.error());
        return std::unexpected(put_result.error());
    }

    m_client->m_last_bool = true;
    return {};
}

AsyncEtcdClient::ConnectAwaitable::SharedState::SharedState(AsyncEtcdClient& owner)
    : client(&owner)
{
    client->resetLastOperation();
    if (client->m_scheduler == nullptr) {
        EtcdError error(EtcdErrorType::Internal, "IOScheduler is null");
        client->setError(error);
        result = std::unexpected(error);
        return;
    }

    if (client->m_connected && client->m_socket != nullptr && client->m_http_session != nullptr) {
        result = Result{};
        return;
    }

    if (!client->m_endpoint_valid || !client->m_server_host.has_value()) {
        const std::string message = client->m_endpoint_error.empty()
            ? "invalid endpoint"
            : client->m_endpoint_error;
        EtcdError error(EtcdErrorType::InvalidEndpoint, message);
        client->setError(error);
        result = std::unexpected(error);
        return;
    }

    try {
        client->m_socket = std::make_unique<galay::async::TcpSocket>(client->m_ip_type);
        auto nonblock_result = client->m_socket->option().handleNonBlock();
        if (!nonblock_result.has_value()) {
            EtcdError error = mapKernelIoError(nonblock_result.error(), EtcdErrorType::Connection);
            client->setError(error);
            client->m_socket.reset();
            client->m_connected = false;
            result = std::unexpected(error);
            return;
        }

        host = client->m_server_host.value();
        phase = Phase::Connect;
    } catch (const std::exception& ex) {
        EtcdError error(EtcdErrorType::Connection, ex.what());
        client->setError(error);
        client->m_http_session.reset();
        client->m_socket.reset();
        client->m_connected = false;
        result = std::unexpected(error);
    }
}

AsyncEtcdClient::ConnectAwaitable::Machine::Machine(std::shared_ptr<SharedState> state)
    : m_state(std::move(state))
{
}

galay::kernel::MachineAction<AsyncEtcdClient::ConnectAwaitable::Result>
AsyncEtcdClient::ConnectAwaitable::Machine::advance()
{
    if (m_state->result.has_value()) {
        return galay::kernel::MachineAction<result_type>::complete(std::move(*m_state->result));
    }

    if (m_state->phase == Phase::Connect) {
        return galay::kernel::MachineAction<result_type>::waitConnect(m_state->host);
    }

    m_state->result = m_state->client->currentResult();
    return galay::kernel::MachineAction<result_type>::complete(std::move(*m_state->result));
}

void AsyncEtcdClient::ConnectAwaitable::Machine::onConnect(
    std::expected<void, galay::kernel::IOError> result)
{
    if (!result.has_value()) {
        EtcdError error = mapKernelIoError(result.error());
        m_state->client->setError(error);
        m_state->client->m_http_session.reset();
        m_state->client->m_socket.reset();
        m_state->client->m_connected = false;
        m_state->result = std::unexpected(error);
        m_state->phase = Phase::Done;
        return;
    }

    try {
        m_state->client->m_http_session = std::make_unique<galay::http::HttpSession>(
            *m_state->client->m_socket,
            m_state->client->m_network_config.buffer_size);
        m_state->client->m_connected = true;
        m_state->result = Result{};
    } catch (const std::exception& ex) {
        EtcdError error(EtcdErrorType::Internal,
                        std::string("create http session failed: ") + ex.what());
        m_state->client->setError(error);
        m_state->client->m_http_session.reset();
        m_state->client->m_socket.reset();
        m_state->client->m_connected = false;
        m_state->result = std::unexpected(error);
    }

    m_state->phase = Phase::Done;
}

void AsyncEtcdClient::ConnectAwaitable::Machine::onRead(
    std::expected<size_t, galay::kernel::IOError>)
{
}

void AsyncEtcdClient::ConnectAwaitable::Machine::onWrite(
    std::expected<size_t, galay::kernel::IOError>)
{
}

AsyncEtcdClient::ConnectAwaitable::ConnectAwaitable(AsyncEtcdClient& client)
    : m_state(std::make_shared<SharedState>(client))
{
    auto* controller =
        client.m_socket != nullptr ? client.m_socket->controller() : &invalidController();
    m_inner = std::make_unique<InnerAwaitable>(
        galay::kernel::AwaitableBuilder<Result>::fromStateMachine(
            controller,
            Machine(m_state))
            .build());
}

bool AsyncEtcdClient::ConnectAwaitable::await_ready() noexcept
{
    return m_inner->await_ready();
}

EtcdVoidResult AsyncEtcdClient::ConnectAwaitable::await_resume()
{
    return m_inner->await_resume();
}

AsyncEtcdClient::CloseAwaitable::CloseAwaitable(AsyncEtcdClient& client)
    : IoAwaitableBase(client)
{
    m_client->resetLastOperation();
    if (m_client->m_socket == nullptr) {
        m_client->m_http_session.reset();
        m_client->m_connected = false;
        return;
    }
    startIo(m_client->m_socket->close());
}

bool AsyncEtcdClient::CloseAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

EtcdVoidResult AsyncEtcdClient::CloseAwaitable::await_resume()
{
    EtcdVoidResult result{};
    auto& io_awaitable = awaitable();
    if (io_awaitable.has_value()) {
        auto close_result = io_awaitable->await_resume();
        if (!close_result.has_value()) {
            EtcdError error = mapKernelIoError(close_result.error());
            m_client->setError(error);
            result = std::unexpected(error);
        }
    } else {
        result = m_client->currentResult();
    }

    m_client->m_http_session.reset();
    m_client->m_socket.reset();
    m_client->m_connected = false;
    return result;
}

AsyncEtcdClient::PostJsonAwaitable AsyncEtcdClient::postJsonInternal(
    const std::string& api_path,
    const std::string& body,
    std::optional<std::chrono::milliseconds> force_timeout)
{
    return PostJsonAwaitable(*this, api_path, body, force_timeout);
}

std::string AsyncEtcdClient::buildSerializedPostRequest(std::string_view api_path,
                                                        std::string_view body) const
{
    const std::string_view normalized_path = trimLeadingSlash(api_path);
    const std::string content_length = std::to_string(body.size());
    std::string request;
    request.reserve(
        m_serialized_request_prefix.size() +
        m_serialized_request_headers.size() +
        normalized_path.size() +
        content_length.size() +
        4 +
        body.size());
    request.append(m_serialized_request_prefix);
    request.append(normalized_path.data(), normalized_path.size());
    request.append(m_serialized_request_headers);
    request.append(content_length);
    request.append("\r\n\r\n");
    request.append(body.data(), body.size());
    return request;
}

AsyncEtcdClient::GetAwaitable::GetAwaitable(AsyncEtcdClient& client,
                                       std::string key,
                                       bool prefix,
                                       std::optional<int64_t> limit)
    : JsonOpAwaitableBase(client)
{
    m_client->resetLastOperation();
    auto body = buildGetRequestBody(key, prefix, limit);
    if (!body.has_value()) {
        m_client->setError(body.error());
        return;
    }

    startPost("/kv/range", std::move(body.value()));
}

bool AsyncEtcdClient::GetAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

EtcdVoidResult AsyncEtcdClient::GetAwaitable::await_resume()
{
    auto result = resumePost();
    if (!result.has_value()) {
        return result;
    }

    auto kvs_result = parseGetResponseKvs(m_client->m_last_response_body);
    if (!kvs_result.has_value()) {
        m_client->setError(kvs_result.error());
        m_client->m_last_kvs.clear();
        return std::unexpected(kvs_result.error());
    }

    m_client->m_last_kvs = std::move(kvs_result.value());
    m_client->m_last_bool = !m_client->m_last_kvs.empty();
    return {};
}

AsyncEtcdClient::DeleteAwaitable::DeleteAwaitable(AsyncEtcdClient& client,
                                             std::string key,
                                             bool prefix)
    : JsonOpAwaitableBase(client)
{
    m_client->resetLastOperation();
    auto body = buildDeleteRequestBody(key, prefix);
    if (!body.has_value()) {
        m_client->setError(body.error());
        return;
    }

    startPost("/kv/deleterange", std::move(body.value()));
}

bool AsyncEtcdClient::DeleteAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

EtcdVoidResult AsyncEtcdClient::DeleteAwaitable::await_resume()
{
    auto result = resumePost();
    if (!result.has_value()) {
        return result;
    }

    auto deleted_result = parseDeleteResponseDeletedCount(m_client->m_last_response_body);
    if (!deleted_result.has_value()) {
        m_client->setError(deleted_result.error());
        return std::unexpected(deleted_result.error());
    }
    m_client->m_last_deleted_count = deleted_result.value();
    m_client->m_last_bool = m_client->m_last_deleted_count > 0;
    return {};
}

AsyncEtcdClient::GrantLeaseAwaitable::GrantLeaseAwaitable(AsyncEtcdClient& client, int64_t ttl_seconds)
    : JsonOpAwaitableBase(client)
{
    m_client->resetLastOperation();
    auto body = buildLeaseGrantRequestBody(ttl_seconds);
    if (!body.has_value()) {
        m_client->setError(body.error());
        return;
    }

    startPost("/lease/grant", std::move(body.value()));
}

bool AsyncEtcdClient::GrantLeaseAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

EtcdVoidResult AsyncEtcdClient::GrantLeaseAwaitable::await_resume()
{
    auto result = resumePost();
    if (!result.has_value()) {
        return result;
    }

    auto lease_result = parseLeaseGrantResponseId(m_client->m_last_response_body);
    if (!lease_result.has_value()) {
        m_client->setError(lease_result.error());
        return std::unexpected(lease_result.error());
    }
    m_client->m_last_lease_id = lease_result.value();
    m_client->m_last_bool = true;
    return {};
}

AsyncEtcdClient::KeepAliveAwaitable::KeepAliveAwaitable(AsyncEtcdClient& client, int64_t lease_id)
    : JsonOpAwaitableBase(client)
    , m_lease_id(lease_id)
{
    m_client->resetLastOperation();
    auto body = buildLeaseKeepAliveRequestBody(m_lease_id);
    if (!body.has_value()) {
        m_client->setError(body.error());
        return;
    }

    std::optional<std::chrono::milliseconds> timeout = std::nullopt;
    if (!m_client->m_network_config.isRequestTimeoutEnabled()) {
        timeout = std::chrono::seconds(5);
    }

    startPost("/lease/keepalive", std::move(body.value()), timeout);
}

bool AsyncEtcdClient::KeepAliveAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

EtcdVoidResult AsyncEtcdClient::KeepAliveAwaitable::await_resume()
{
    auto result = resumePost();
    if (!result.has_value()) {
        return result;
    }

    auto keepalive_result = parseLeaseKeepAliveResponseId(m_client->m_last_response_body, m_lease_id);
    if (!keepalive_result.has_value()) {
        m_client->setError(keepalive_result.error());
        return std::unexpected(keepalive_result.error());
    }

    m_client->m_last_lease_id = keepalive_result.value();
    m_client->m_last_bool = true;
    return {};
}

AsyncEtcdClient::PipelineAwaitable::PipelineAwaitable(AsyncEtcdClient& client,
                                                      std::span<const PipelineOp> operations)
    : JsonOpAwaitableBase(client)
{
    m_client->resetLastOperation();
    m_operation_types.reserve(operations.size());
    for (const auto& op : operations) {
        m_operation_types.push_back(op.type);
    }

    auto body = buildTxnBody(operations);
    if (!body.has_value()) {
        m_client->setError(body.error());
        return;
    }
    startPost("/kv/txn", std::move(body.value()));
}

AsyncEtcdClient::PipelineAwaitable::PipelineAwaitable(AsyncEtcdClient& client,
                                                      std::vector<PipelineOp> operations)
    : PipelineAwaitable(client, std::span<const PipelineOp>(operations.data(), operations.size()))
{
}

bool AsyncEtcdClient::PipelineAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

EtcdVoidResult AsyncEtcdClient::PipelineAwaitable::await_resume()
{
    auto result = resumePost();
    if (!result.has_value()) {
        return result;
    }

    auto pipeline_results = parsePipelineTxnResponse(
        m_client->m_last_response_body,
        std::span<const PipelineOpType>(m_operation_types.data(), m_operation_types.size()));
    if (!pipeline_results.has_value()) {
        m_client->setError(pipeline_results.error());
        m_client->m_last_pipeline_results.clear();
        return std::unexpected(pipeline_results.error());
    }

    m_client->m_last_pipeline_results = std::move(pipeline_results.value());
    m_client->m_last_bool = true;
    return {};
}

AsyncEtcdClient::ConnectAwaitable AsyncEtcdClient::connect()
{
    return ConnectAwaitable(*this);
}

AsyncEtcdClient::CloseAwaitable AsyncEtcdClient::close()
{
    return CloseAwaitable(*this);
}

AsyncEtcdClient::PutAwaitable AsyncEtcdClient::put(const std::string& key,
                                         const std::string& value,
                                         std::optional<int64_t> lease_id)
{
    return PutAwaitable(*this, key, value, lease_id);
}

AsyncEtcdClient::GetAwaitable AsyncEtcdClient::get(const std::string& key,
                                         bool prefix,
                                         std::optional<int64_t> limit)
{
    return GetAwaitable(*this, key, prefix, limit);
}

AsyncEtcdClient::DeleteAwaitable AsyncEtcdClient::del(const std::string& key, bool prefix)
{
    return DeleteAwaitable(*this, key, prefix);
}

AsyncEtcdClient::GrantLeaseAwaitable AsyncEtcdClient::grantLease(int64_t ttl_seconds)
{
    return GrantLeaseAwaitable(*this, ttl_seconds);
}

AsyncEtcdClient::KeepAliveAwaitable AsyncEtcdClient::keepAliveOnce(int64_t lease_id)
{
    return KeepAliveAwaitable(*this, lease_id);
}

AsyncEtcdClient::PipelineAwaitable AsyncEtcdClient::pipeline(std::span<const PipelineOp> operations)
{
    return PipelineAwaitable(*this, operations);
}

AsyncEtcdClient::PipelineAwaitable AsyncEtcdClient::pipeline(std::vector<PipelineOp> operations)
{
    return PipelineAwaitable(*this, std::span<const PipelineOp>(operations.data(), operations.size()));
}

bool AsyncEtcdClient::connected() const
{
    return m_connected;
}

EtcdError AsyncEtcdClient::lastError() const
{
    return m_last_error;
}

bool AsyncEtcdClient::lastBool() const
{
    return m_last_bool;
}

int64_t AsyncEtcdClient::lastLeaseId() const
{
    return m_last_lease_id;
}

int64_t AsyncEtcdClient::lastDeletedCount() const
{
    return m_last_deleted_count;
}

const std::vector<EtcdKeyValue>& AsyncEtcdClient::lastKeyValues() const
{
    return m_last_kvs;
}

const std::vector<AsyncEtcdClient::PipelineItemResult>& AsyncEtcdClient::lastPipelineResults() const
{
    return m_last_pipeline_results;
}

int AsyncEtcdClient::lastStatusCode() const
{
    return m_last_status_code;
}

const std::string& AsyncEtcdClient::lastResponseBody() const
{
    return m_last_response_body;
}

EtcdVoidResult AsyncEtcdClient::currentResult() const
{
    if (m_last_error.isOk()) {
        return {};
    }
    return std::unexpected(m_last_error);
}

EtcdVoidResult AsyncEtcdClient::resumePostOrCurrent(std::optional<PostJsonAwaitable>& post_awaitable)
{
    if (!post_awaitable.has_value()) {
        return currentResult();
    }

    auto post_result = post_awaitable->await_resume();
    if (!post_result.has_value()) {
        setError(post_result.error());
        return std::unexpected(post_result.error());
    }

    return {};
}

void AsyncEtcdClient::resetLastOperation()
{
    m_last_error = EtcdError(EtcdErrorType::Success);
    m_last_bool = false;
    m_last_lease_id = 0;
    m_last_deleted_count = 0;
    m_last_status_code = 0;
    m_last_response_body.clear();
    m_last_kvs.clear();
    m_last_pipeline_results.clear();
}

void AsyncEtcdClient::setError(EtcdErrorType type, const std::string& message)
{
    m_last_error = EtcdError(type, message);
}

void AsyncEtcdClient::setError(EtcdError error)
{
    m_last_error = std::move(error);
}

} // namespace galay::etcd
