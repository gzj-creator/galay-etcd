#include "AsyncEtcdClient.h"

#include "galay-etcd/base/EtcdInternal.h"

#include <galay-http/protoc/http/HttpError.h>

#include <exception>
#include <string_view>
#include <utility>

namespace galay::etcd
{

using namespace internal;

namespace
{

galay::http::HttpRequest buildJsonPostRequest(
    std::string uri,
    std::string body,
    const std::string& host_header,
    bool keepalive)
{
    galay::http::HttpRequest request;
    galay::http::HttpRequestHeader header;

    header.method() = galay::http::HttpMethod::POST;
    header.uri() = std::move(uri);
    header.version() = galay::http::HttpVersion::HttpVersion_1_1;
    header.headerPairs().addHeaderPair("Host", host_header);
    header.headerPairs().addHeaderPair("Accept", "application/json");
    header.headerPairs().addHeaderPair("Connection", keepalive ? "keep-alive" : "close");

    if (!body.empty()) {
        header.headerPairs().addHeaderPair("Content-Type", "application/json");
        header.headerPairs().addHeaderPair("Content-Length", std::to_string(body.size()));
    }

    request.setHeader(std::move(header));
    if (!body.empty()) {
        request.setBodyStr(std::move(body));
    }
    return request;
}

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
    m_endpoint_valid = true;
}

struct AsyncEtcdClient::PostJsonAwaitable::Context
{
    using HttpAwaitable = galay::http::HttpSessionAwaitable;

    AsyncEtcdClient* owner = nullptr;
    HttpAwaitable awaitable;

    Context(AsyncEtcdClient& client,
            std::string api_path,
            std::string body)
        : owner(&client)
        , awaitable(*client.m_http_session, buildJsonPostRequest(
                                 client.m_api_prefix + api_path,
                                 std::move(body),
                                 client.m_host_header,
                                 client.m_network_config.keepalive))
    {
    }
};

AsyncEtcdClient::PostJsonAwaitable::PostJsonAwaitable(AsyncEtcdClient& client,
                                                 std::string api_path,
                                                 std::string body,
                                                 std::optional<std::chrono::milliseconds> force_timeout)
    : m_ctx(nullptr)
{
    if (!client.m_connected || client.m_socket == nullptr || client.m_http_session == nullptr) {
        client.setError(EtcdErrorType::NotConnected, "etcd client is not connected");
        return;
    }

    m_ctx = std::make_unique<Context>(client, std::move(api_path), std::move(body));

    if (force_timeout.has_value()) {
        m_ctx->awaitable.timeout(force_timeout.value());
    } else if (client.m_network_config.isRequestTimeoutEnabled()) {
        m_ctx->awaitable.timeout(client.m_network_config.request_timeout);
    }
}

AsyncEtcdClient::PostJsonAwaitable::~PostJsonAwaitable() = default;

bool AsyncEtcdClient::PostJsonAwaitable::await_ready() const noexcept
{
    return m_ctx == nullptr;
}

bool AsyncEtcdClient::PostJsonAwaitable::await_suspend(std::coroutine_handle<> handle)
{
    return m_ctx->awaitable.await_suspend(handle);
}

EtcdVoidResult AsyncEtcdClient::PostJsonAwaitable::await_resume()
{
    if (m_ctx == nullptr) {
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

bool AsyncEtcdClient::JsonOpAwaitableBase::awaitSuspend(std::coroutine_handle<> handle)
{
    return m_post_awaitable->await_suspend(handle);
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
    if (key.empty()) {
        EtcdError error(EtcdErrorType::InvalidParam, "key must not be empty");
        m_client->setError(error);
        return;
    }

    std::string body = "{\"key\":\"" + encodeBase64(key) + "\",\"value\":\"" + encodeBase64(value) + "\"";
    if (lease_id.has_value()) {
        if (lease_id.value() <= 0) {
            EtcdError error(EtcdErrorType::InvalidParam, "lease id must be positive");
            m_client->setError(error);
            return;
        }
        body += ",\"lease\":\"" + std::to_string(lease_id.value()) + "\"";
    }
    body += "}";

    startPost("/kv/put", std::move(body));
}

bool AsyncEtcdClient::PutAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

bool AsyncEtcdClient::PutAwaitable::await_suspend(std::coroutine_handle<> handle)
{
    return awaitSuspend(handle);
}

EtcdVoidResult AsyncEtcdClient::PutAwaitable::await_resume()
{
    auto result = resumePost();
    if (!result.has_value()) {
        return result;
    }

    if (maybeContainsEtcdErrorFields(m_client->m_last_response_body)) {
        auto root = parseEtcdSuccessObject(
            m_client->m_last_response_body,
            "parse put response");
        if (!root.has_value()) {
            m_client->setError(root.error());
            return std::unexpected(root.error());
        }
    }

    m_client->m_last_bool = true;
    return {};
}

AsyncEtcdClient::ConnectAwaitable::ConnectAwaitable(AsyncEtcdClient& client)
    : IoAwaitableBase(client)
{
    m_client->resetLastOperation();
    if (m_client->m_scheduler == nullptr) {
        EtcdError error(EtcdErrorType::Internal, "IOScheduler is null");
        m_client->setError(error);
        return;
    }

    if (m_client->m_connected && m_client->m_socket != nullptr && m_client->m_http_session != nullptr) {
        return;
    }

    if (!m_client->m_endpoint_valid || !m_client->m_server_host.has_value()) {
        const std::string message = m_client->m_endpoint_error.empty()
            ? "invalid endpoint"
            : m_client->m_endpoint_error;
        m_client->setError(EtcdErrorType::InvalidEndpoint, message);
        return;
    }

    try {
        m_client->m_socket = std::make_unique<galay::async::TcpSocket>(m_client->m_ip_type);
        auto nonblock_result = m_client->m_socket->option().handleNonBlock();
        if (!nonblock_result.has_value()) {
            EtcdError error = mapKernelIoError(nonblock_result.error(), EtcdErrorType::Connection);
            m_client->setError(error);
            m_client->m_socket.reset();
            m_client->m_connected = false;
            return;
        }
        startIo(m_client->m_socket->connect(m_client->m_server_host.value()));
    } catch (const std::exception& ex) {
        EtcdError error(EtcdErrorType::Connection, ex.what());
        m_client->setError(error);
        m_client->m_http_session.reset();
        m_client->m_socket.reset();
        m_client->m_connected = false;
    }
}

bool AsyncEtcdClient::ConnectAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

bool AsyncEtcdClient::ConnectAwaitable::await_suspend(std::coroutine_handle<> handle)
{
    return awaitSuspend(handle);
}

EtcdVoidResult AsyncEtcdClient::ConnectAwaitable::await_resume()
{
    auto& io_awaitable = awaitable();
    if (!io_awaitable.has_value()) {
        return m_client->currentResult();
    }

    auto connect_result = io_awaitable->await_resume();
    if (!connect_result.has_value()) {
        EtcdError error = mapKernelIoError(connect_result.error());
        m_client->setError(error);
        m_client->m_http_session.reset();
        m_client->m_socket.reset();
        m_client->m_connected = false;
        return std::unexpected(error);
    }

    try {
        m_client->m_http_session = std::make_unique<galay::http::HttpSession>(
            *m_client->m_socket,
            m_client->m_network_config.buffer_size);
        m_client->m_connected = true;
        return {};
    } catch (const std::exception& ex) {
        EtcdError error(EtcdErrorType::Internal, std::string("create http session failed: ") + ex.what());
        m_client->setError(error);
        m_client->m_http_session.reset();
        m_client->m_socket.reset();
        m_client->m_connected = false;
        return std::unexpected(error);
    }
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

bool AsyncEtcdClient::CloseAwaitable::await_suspend(std::coroutine_handle<> handle)
{
    return awaitSuspend(handle);
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

AsyncEtcdClient::GetAwaitable::GetAwaitable(AsyncEtcdClient& client,
                                       std::string key,
                                       bool prefix,
                                       std::optional<int64_t> limit)
    : JsonOpAwaitableBase(client)
{
    m_client->resetLastOperation();
    if (key.empty()) {
        EtcdError error(EtcdErrorType::InvalidParam, "key must not be empty");
        m_client->setError(error);
        return;
    }
    if (limit.has_value() && limit.value() <= 0) {
        EtcdError error(EtcdErrorType::InvalidParam, "limit must be positive");
        m_client->setError(error);
        return;
    }

    std::string body = "{\"key\":\"" + encodeBase64(key) + "\"";
    if (prefix) {
        const std::string range_end = makePrefixRangeEnd(key);
        body += ",\"range_end\":\"" + encodeBase64(range_end) + "\"";
    }
    if (limit.has_value()) {
        body += ",\"limit\":" + std::to_string(limit.value());
    }
    body += "}";

    startPost("/kv/range", std::move(body));
}

bool AsyncEtcdClient::GetAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

bool AsyncEtcdClient::GetAwaitable::await_suspend(std::coroutine_handle<> handle)
{
    return awaitSuspend(handle);
}

EtcdVoidResult AsyncEtcdClient::GetAwaitable::await_resume()
{
    auto result = resumePost();
    if (!result.has_value()) {
        return result;
    }

    auto root = parseEtcdSuccessObject(
        m_client->m_last_response_body,
        "parse get response");
    if (!root.has_value()) {
        m_client->setError(root.error());
        m_client->m_last_kvs.clear();
        return std::unexpected(root.error());
    }

    auto kvs_result = parseKvsFromObject(root.value(), "parse get response");
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
    if (key.empty()) {
        EtcdError error(EtcdErrorType::InvalidParam, "key must not be empty");
        m_client->setError(error);
        return;
    }

    std::string body = "{\"key\":\"" + encodeBase64(key) + "\"";
    if (prefix) {
        const std::string range_end = makePrefixRangeEnd(key);
        body += ",\"range_end\":\"" + encodeBase64(range_end) + "\"";
    }
    body += "}";

    startPost("/kv/deleterange", std::move(body));
}

bool AsyncEtcdClient::DeleteAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

bool AsyncEtcdClient::DeleteAwaitable::await_suspend(std::coroutine_handle<> handle)
{
    return awaitSuspend(handle);
}

EtcdVoidResult AsyncEtcdClient::DeleteAwaitable::await_resume()
{
    auto result = resumePost();
    if (!result.has_value()) {
        return result;
    }

    auto root = parseEtcdSuccessObject(
        m_client->m_last_response_body,
        "parse delete response");
    if (!root.has_value()) {
        m_client->setError(root.error());
        return std::unexpected(root.error());
    }

    m_client->m_last_deleted_count = findIntField(root.value(), "deleted").value_or(0);
    m_client->m_last_bool = m_client->m_last_deleted_count > 0;
    return {};
}

AsyncEtcdClient::GrantLeaseAwaitable::GrantLeaseAwaitable(AsyncEtcdClient& client, int64_t ttl_seconds)
    : JsonOpAwaitableBase(client)
{
    m_client->resetLastOperation();
    if (ttl_seconds <= 0) {
        EtcdError error(EtcdErrorType::InvalidParam, "ttl must be positive");
        m_client->setError(error);
        return;
    }

    const std::string body = "{\"TTL\":" + std::to_string(ttl_seconds) + "}";
    startPost("/lease/grant", body);
}

bool AsyncEtcdClient::GrantLeaseAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

bool AsyncEtcdClient::GrantLeaseAwaitable::await_suspend(std::coroutine_handle<> handle)
{
    return awaitSuspend(handle);
}

EtcdVoidResult AsyncEtcdClient::GrantLeaseAwaitable::await_resume()
{
    auto result = resumePost();
    if (!result.has_value()) {
        return result;
    }

    auto root = parseEtcdSuccessObject(
        m_client->m_last_response_body,
        "parse lease grant response");
    if (!root.has_value()) {
        m_client->setError(root.error());
        return std::unexpected(root.error());
    }

    const auto lease_id = findIntField(root.value(), "ID");
    if (!lease_id.has_value()) {
        EtcdError error(EtcdErrorType::Parse, "lease grant response missing ID");
        m_client->setError(error);
        return std::unexpected(error);
    }
    m_client->m_last_lease_id = lease_id.value();
    m_client->m_last_bool = true;
    return {};
}

AsyncEtcdClient::KeepAliveAwaitable::KeepAliveAwaitable(AsyncEtcdClient& client, int64_t lease_id)
    : JsonOpAwaitableBase(client)
    , m_lease_id(lease_id)
{
    m_client->resetLastOperation();
    if (m_lease_id <= 0) {
        EtcdError error(EtcdErrorType::InvalidParam, "lease id must be positive");
        m_client->setError(error);
        return;
    }

    const std::string body = "{\"ID\":\"" + std::to_string(m_lease_id) + "\"}";
    std::optional<std::chrono::milliseconds> timeout = std::nullopt;
    if (!m_client->m_network_config.isRequestTimeoutEnabled()) {
        timeout = std::chrono::seconds(5);
    }

    startPost("/lease/keepalive", body, timeout);
}

bool AsyncEtcdClient::KeepAliveAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

bool AsyncEtcdClient::KeepAliveAwaitable::await_suspend(std::coroutine_handle<> handle)
{
    return awaitSuspend(handle);
}

EtcdVoidResult AsyncEtcdClient::KeepAliveAwaitable::await_resume()
{
    auto result = resumePost();
    if (!result.has_value()) {
        return result;
    }

    auto root = parseEtcdSuccessObject(
        m_client->m_last_response_body,
        "parse lease keepalive response");
    if (!root.has_value()) {
        m_client->setError(root.error());
        return std::unexpected(root.error());
    }

    const auto response_id = findIntField(root.value(), "ID");
    if (response_id.has_value() && response_id.value() != m_lease_id) {
        EtcdError error(EtcdErrorType::Parse, "lease keepalive response id mismatch");
        m_client->setError(error);
        return std::unexpected(error);
    }

    m_client->m_last_lease_id = m_lease_id;
    m_client->m_last_bool = true;
    return {};
}

AsyncEtcdClient::PipelineAwaitable::PipelineAwaitable(AsyncEtcdClient& client, std::vector<PipelineOp> operations)
    : JsonOpAwaitableBase(client)
    , m_operations(std::move(operations))
{
    m_client->resetLastOperation();
    auto body = buildTxnBody(m_operations);
    if (!body.has_value()) {
        m_client->setError(body.error());
        return;
    }
    startPost("/kv/txn", std::move(body.value()));
}

bool AsyncEtcdClient::PipelineAwaitable::await_ready() const noexcept
{
    return awaitReady();
}

bool AsyncEtcdClient::PipelineAwaitable::await_suspend(std::coroutine_handle<> handle)
{
    return awaitSuspend(handle);
}

EtcdVoidResult AsyncEtcdClient::PipelineAwaitable::await_resume()
{
    auto result = resumePost();
    if (!result.has_value()) {
        return result;
    }

    auto root = parseEtcdSuccessObject(
        m_client->m_last_response_body,
        "parse pipeline txn response");
    if (!root.has_value()) {
        m_client->setError(root.error());
        m_client->m_last_pipeline_results.clear();
        return std::unexpected(root.error());
    }

    auto succeeded_field = root.value()["succeeded"];
    if (!succeeded_field.error()) {
        auto succeeded_result = succeeded_field.value_unsafe().get_bool();
        if (!succeeded_result.error() && !succeeded_result.value_unsafe()) {
            EtcdError error(EtcdErrorType::Server, "pipeline txn returned succeeded=false");
            m_client->setError(error);
            m_client->m_last_pipeline_results.clear();
            return std::unexpected(error);
        }
    }

    auto responses_field = root.value()["responses"];
    if (responses_field.error()) {
        EtcdError error(EtcdErrorType::Parse, "pipeline txn response missing responses field");
        m_client->setError(error);
        m_client->m_last_pipeline_results.clear();
        return std::unexpected(error);
    }

    auto responses_array_result = responses_field.value_unsafe().get_array();
    if (responses_array_result.error()) {
        EtcdError error = makeJsonParseError("parse pipeline responses as array", responses_array_result.error());
        m_client->setError(error);
        m_client->m_last_pipeline_results.clear();
        return std::unexpected(error);
    }

    const auto responses = responses_array_result.value_unsafe();
    if (responses.size() != m_operations.size()) {
        EtcdError error(
            EtcdErrorType::Parse,
            "pipeline responses size mismatch, expected=" + std::to_string(m_operations.size()) +
                ", actual=" + std::to_string(responses.size()));
        m_client->setError(error);
        m_client->m_last_pipeline_results.clear();
        return std::unexpected(error);
    }

    m_client->m_last_pipeline_results.clear();
    m_client->m_last_pipeline_results.reserve(m_operations.size());

    for (size_t i = 0; i < m_operations.size(); ++i) {
        auto item_object_result = responses.at(i).get_object();
        if (item_object_result.error()) {
            EtcdError error = makeJsonParseError("parse pipeline response item as object", item_object_result.error());
            m_client->setError(error);
            m_client->m_last_pipeline_results.clear();
            return std::unexpected(error);
        }

        const auto item_object = item_object_result.value_unsafe();
        PipelineItemResult item;
        item.type = m_operations[i].type;

        switch (m_operations[i].type) {
        case PipelineOpType::Put: {
            auto put_field = item_object["response_put"];
            if (put_field.error()) {
                EtcdError error(EtcdErrorType::Parse, "pipeline put response missing response_put");
                m_client->setError(error);
                m_client->m_last_pipeline_results.clear();
                return std::unexpected(error);
            }
            auto put_object_result = put_field.value_unsafe().get_object();
            if (put_object_result.error()) {
                EtcdError error = makeJsonParseError("parse pipeline response_put as object", put_object_result.error());
                m_client->setError(error);
                m_client->m_last_pipeline_results.clear();
                return std::unexpected(error);
            }
            item.ok = true;
            break;
        }
        case PipelineOpType::Get: {
            auto range_field = item_object["response_range"];
            if (range_field.error()) {
                EtcdError error(EtcdErrorType::Parse, "pipeline get response missing response_range");
                m_client->setError(error);
                m_client->m_last_pipeline_results.clear();
                return std::unexpected(error);
            }
            auto range_object_result = range_field.value_unsafe().get_object();
            if (range_object_result.error()) {
                EtcdError error = makeJsonParseError("parse pipeline response_range as object", range_object_result.error());
                m_client->setError(error);
                m_client->m_last_pipeline_results.clear();
                return std::unexpected(error);
            }

            auto kvs_result = parseKvsFromObject(range_object_result.value_unsafe(), "parse pipeline response_range");
            if (!kvs_result.has_value()) {
                m_client->setError(kvs_result.error());
                m_client->m_last_pipeline_results.clear();
                return std::unexpected(kvs_result.error());
            }
            item.kvs = std::move(kvs_result.value());
            item.ok = true;
            break;
        }
        case PipelineOpType::Delete: {
            auto del_field = item_object["response_delete_range"];
            if (del_field.error()) {
                EtcdError error(EtcdErrorType::Parse, "pipeline delete response missing response_delete_range");
                m_client->setError(error);
                m_client->m_last_pipeline_results.clear();
                return std::unexpected(error);
            }
            auto del_object_result = del_field.value_unsafe().get_object();
            if (del_object_result.error()) {
                EtcdError error = makeJsonParseError("parse pipeline response_delete_range as object", del_object_result.error());
                m_client->setError(error);
                m_client->m_last_pipeline_results.clear();
                return std::unexpected(error);
            }
            item.deleted_count = findIntField(del_object_result.value_unsafe(), "deleted").value_or(0);
            item.ok = true;
            break;
        }
        }

        m_client->m_last_pipeline_results.push_back(std::move(item));
    }

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

AsyncEtcdClient::PipelineAwaitable AsyncEtcdClient::pipeline(std::vector<PipelineOp> operations)
{
    return PipelineAwaitable(*this, std::move(operations));
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
