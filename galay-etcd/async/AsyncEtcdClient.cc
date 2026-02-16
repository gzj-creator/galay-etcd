#include "AsyncEtcdClient.h"

#include <galay-http/protoc/http/HttpError.h>
#include <galay-utils/algorithm/Base64.hpp>
#include <simdjson.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstddef>
#include <exception>
#include <limits>
#include <regex>
#include <string_view>
#include <utility>

namespace galay::etcd
{

namespace
{

std::optional<int64_t> parseSignedInt(std::string_view value)
{
    if (value.empty()) {
        return std::nullopt;
    }
    int64_t parsed = 0;
    const char* begin = value.data();
    const char* end = value.data() + value.size();
    auto [ptr, ec] = std::from_chars(begin, end, parsed);
    if (ec != std::errc() || ptr != end) {
        return std::nullopt;
    }
    return parsed;
}

std::optional<int64_t> asInt64(const simdjson::dom::element& element)
{
    auto int64_result = element.get_int64();
    if (!int64_result.error()) {
        return int64_result.value_unsafe();
    }

    auto uint64_result = element.get_uint64();
    if (!uint64_result.error()) {
        const uint64_t value = uint64_result.value_unsafe();
        if (value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return static_cast<int64_t>(value);
        }
    }

    auto string_result = element.get_string();
    if (!string_result.error()) {
        return parseSignedInt(string_result.value_unsafe());
    }
    return std::nullopt;
}

std::optional<int64_t> findIntField(const simdjson::dom::object& object, std::string_view field)
{
    auto field_result = object[field];
    if (field_result.error()) {
        return std::nullopt;
    }
    return asInt64(field_result.value_unsafe());
}

std::optional<std::string> findStringField(const simdjson::dom::object& object, std::string_view field)
{
    auto field_result = object[field];
    if (field_result.error()) {
        return std::nullopt;
    }

    auto string_result = field_result.value_unsafe().get_string();
    if (string_result.error()) {
        return std::nullopt;
    }
    return std::string(string_result.value_unsafe());
}

EtcdError makeJsonParseError(const std::string& context, simdjson::error_code error)
{
    return EtcdError(
        ETCD_ERROR_PARSE,
        context + ": " + std::string(simdjson::error_message(error)));
}

std::optional<simdjson::dom::object> parseJsonObject(
    const std::string& body,
    simdjson::dom::parser& parser,
    EtcdError* parse_error,
    const std::string& context)
{
    auto doc_result = parser.parse(body);
    if (doc_result.error()) {
        if (parse_error != nullptr) {
            *parse_error = makeJsonParseError(context, doc_result.error());
        }
        return std::nullopt;
    }

    auto object_result = doc_result.value_unsafe().get_object();
    if (object_result.error()) {
        if (parse_error != nullptr) {
            *parse_error = makeJsonParseError(context, object_result.error());
        }
        return std::nullopt;
    }

    return object_result.value_unsafe();
}

std::string normalizeApiPrefix(std::string prefix)
{
    if (prefix.empty()) {
        return "/v3";
    }
    if (prefix.front() != '/') {
        prefix.insert(prefix.begin(), '/');
    }
    while (prefix.size() > 1 && prefix.back() == '/') {
        prefix.pop_back();
    }
    return prefix;
}

std::string makePrefixRangeEnd(std::string key)
{
    for (std::ptrdiff_t i = static_cast<std::ptrdiff_t>(key.size()) - 1; i >= 0; --i) {
        const unsigned char ch = static_cast<unsigned char>(key[static_cast<size_t>(i)]);
        if (ch < 0xFF) {
            key[static_cast<size_t>(i)] = static_cast<char>(ch + 1);
            key.resize(static_cast<size_t>(i) + 1);
            return key;
        }
    }
    return std::string(1, '\0');
}

std::string encodeBase64(std::string_view data)
{
    return galay::utils::Base64Util::Base64EncodeView(data);
}

std::optional<std::string> decodeBase64(std::string_view data)
{
    try {
        return galay::utils::Base64Util::Base64DecodeView(data);
    } catch (...) {
        return std::nullopt;
    }
}

struct ParsedEndpoint
{
    std::string host;
    uint16_t port = 0;
    galay::kernel::IPType ip_type = galay::kernel::IPType::IPV4;
    bool secure = false;
};

std::expected<ParsedEndpoint, std::string> parseEndpoint(const std::string& endpoint)
{
    static const std::regex kEndpointRegex(
        R"(^(http|https)://(\[[^\]]+\]|[^/:]+)(?::(\d+))?(?:/.*)?$)",
        std::regex::icase);
    std::smatch matches;
    if (!std::regex_match(endpoint, matches, kEndpointRegex)) {
        return std::unexpected("invalid endpoint: " + endpoint);
    }

    ParsedEndpoint parsed;
    std::string scheme = matches[1].str();
    std::transform(
        scheme.begin(),
        scheme.end(),
        scheme.begin(),
        [](const unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    parsed.secure = scheme == "https";

    std::string host = matches[2].str();
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
        host = host.substr(1, host.size() - 2);
        parsed.ip_type = galay::kernel::IPType::IPV6;
    } else if (host.find(':') != std::string::npos) {
        parsed.ip_type = galay::kernel::IPType::IPV6;
    }
    parsed.host = std::move(host);

    int port = parsed.secure ? 443 : 80;
    if (matches[3].matched) {
        try {
            port = std::stoi(matches[3].str());
        } catch (...) {
            return std::unexpected("invalid endpoint port: " + endpoint);
        }
        if (port <= 0 || port > 65535) {
            return std::unexpected("endpoint port out of range: " + endpoint);
        }
    }
    parsed.port = static_cast<uint16_t>(port);
    return parsed;
}

std::string buildHostHeader(const std::string& host, uint16_t port, galay::kernel::IPType ip_type)
{
    if (ip_type == galay::kernel::IPType::IPV6) {
        return "[" + host + "]:" + std::to_string(port);
    }
    return host + ":" + std::to_string(port);
}

simdjson::dom::parser& threadLocalJsonParser()
{
    thread_local simdjson::dom::parser parser;
    return parser;
}

std::expected<simdjson::dom::object, EtcdError> parseEtcdSuccessObject(
    const std::string& body,
    const std::string& context)
{
    EtcdError parse_error(ETCD_ERROR_SUCCESS);
    auto root = parseJsonObject(body, threadLocalJsonParser(), &parse_error, context);
    if (!root.has_value()) {
        return std::unexpected(std::move(parse_error));
    }

    if (auto error_code = findIntField(root.value(), "code");
        error_code.has_value() && error_code.value() != 0) {
        const std::string message =
            findStringField(root.value(), "message")
                .value_or("etcd server returned error");
        return std::unexpected(EtcdError(
            ETCD_ERROR_SERVER,
            "code=" + std::to_string(error_code.value()) + ", message=" + message));
    }

    return root.value();
}

std::expected<std::vector<EtcdKeyValue>, EtcdError> parseKvsFromObject(
    const simdjson::dom::object& object,
    const std::string& context)
{
    auto kvs_field = object["kvs"];
    if (kvs_field.error()) {
        return std::vector<EtcdKeyValue>{};
    }

    auto kvs_array_result = kvs_field.value_unsafe().get_array();
    if (kvs_array_result.error()) {
        return std::unexpected(makeJsonParseError(context + ".kvs as array", kvs_array_result.error()));
    }

    const auto kvs_array = kvs_array_result.value_unsafe();
    std::vector<EtcdKeyValue> kvs;
    kvs.reserve(kvs_array.size());

    for (auto kv_element : kvs_array) {
        auto kv_object_result = kv_element.get_object();
        if (kv_object_result.error()) {
            return std::unexpected(makeJsonParseError(context + ".kv item as object", kv_object_result.error()));
        }

        const auto kv_object = kv_object_result.value_unsafe();
        const auto encoded_key = findStringField(kv_object, "key");
        if (!encoded_key.has_value()) {
            return std::unexpected(EtcdError(ETCD_ERROR_PARSE, context + ": missing key in kv item"));
        }

        const auto decoded_key = decodeBase64(encoded_key.value());
        if (!decoded_key.has_value()) {
            return std::unexpected(EtcdError(ETCD_ERROR_PARSE, context + ": failed to decode base64 key"));
        }

        const auto encoded_value = findStringField(kv_object, "value").value_or("");
        const auto decoded_value = decodeBase64(encoded_value);
        if (!decoded_value.has_value()) {
            return std::unexpected(EtcdError(ETCD_ERROR_PARSE, context + ": failed to decode base64 value"));
        }

        EtcdKeyValue item;
        item.key = decoded_key.value();
        item.value = decoded_value.value();
        item.create_revision = findIntField(kv_object, "create_revision").value_or(0);
        item.mod_revision = findIntField(kv_object, "mod_revision").value_or(0);
        item.version = findIntField(kv_object, "version").value_or(0);
        item.lease = findIntField(kv_object, "lease").value_or(0);
        kvs.push_back(std::move(item));
    }

    return kvs;
}

bool maybeContainsEtcdErrorFields(const std::string& body)
{
    return body.find("\"code\"") != std::string::npos ||
        body.find("\"message\"") != std::string::npos ||
        body.find("\"error\"") != std::string::npos;
}

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

std::expected<std::string, EtcdError> buildTxnBody(const std::vector<AsyncEtcdClient::PipelineOp>& operations)
{
    if (operations.empty()) {
        return std::unexpected(EtcdError(ETCD_ERROR_INVALID_PARAM, "pipeline operations must not be empty"));
    }

    std::string body = "{\"compare\":[],\"success\":[";
    bool first = true;
    for (const auto& op : operations) {
        if (op.key.empty()) {
            return std::unexpected(EtcdError(ETCD_ERROR_INVALID_PARAM, "pipeline op key must not be empty"));
        }
        if (op.limit.has_value() && op.limit.value() <= 0) {
            return std::unexpected(EtcdError(ETCD_ERROR_INVALID_PARAM, "pipeline op limit must be positive"));
        }
        if (op.lease_id.has_value() && op.lease_id.value() <= 0) {
            return std::unexpected(EtcdError(ETCD_ERROR_INVALID_PARAM, "pipeline op lease id must be positive"));
        }

        if (!first) {
            body += ",";
        }
        first = false;

        switch (op.type) {
        case AsyncEtcdClient::PipelineOpType::Put: {
            body += "{\"request_put\":{\"key\":\"" + encodeBase64(op.key) + "\",\"value\":\"" + encodeBase64(op.value) + "\"";
            if (op.lease_id.has_value()) {
                body += ",\"lease\":\"" + std::to_string(op.lease_id.value()) + "\"";
            }
            body += "}}";
            break;
        }
        case AsyncEtcdClient::PipelineOpType::Get: {
            body += "{\"request_range\":{\"key\":\"" + encodeBase64(op.key) + "\"";
            if (op.prefix) {
                const std::string range_end = makePrefixRangeEnd(op.key);
                body += ",\"range_end\":\"" + encodeBase64(range_end) + "\"";
            }
            if (op.limit.has_value()) {
                body += ",\"limit\":" + std::to_string(op.limit.value());
            }
            body += "}}";
            break;
        }
        case AsyncEtcdClient::PipelineOpType::Delete: {
            body += "{\"request_delete_range\":{\"key\":\"" + encodeBase64(op.key) + "\"";
            if (op.prefix) {
                const std::string range_end = makePrefixRangeEnd(op.key);
                body += ",\"range_end\":\"" + encodeBase64(range_end) + "\"";
            }
            body += "}}";
            break;
        }
        }
    }

    body += "],\"failure\":[]}";
    return body;
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
        return EtcdError(ETCD_ERROR_TIMEOUT, error.message());
    case kTcpConnectError:
        return EtcdError(ETCD_ERROR_CONNECTION, error.message());
    case kTcpSendError:
    case kSendError:
        return EtcdError(ETCD_ERROR_SEND, error.message());
    case kTcpRecvError:
    case kRecvError:
        return EtcdError(ETCD_ERROR_RECV, error.message());
    case kConnectionClose:
        return EtcdError(ETCD_ERROR_CONNECTION, error.message());
    default:
        return EtcdError(ETCD_ERROR_HTTP, error.message());
    }
}

EtcdError mapKernelIoError(const galay::kernel::IOError& error,
                           EtcdErrorType fallback = ETCD_ERROR_CONNECTION)
{
    using galay::kernel::IOError;
    using galay::kernel::kConnectFailed;
    using galay::kernel::kDisconnectError;
    using galay::kernel::kNotRunningOnIOScheduler;
    using galay::kernel::kRecvFailed;
    using galay::kernel::kSendFailed;
    using galay::kernel::kTimeout;

    if (IOError::contains(error.code(), kTimeout)) {
        return EtcdError(ETCD_ERROR_TIMEOUT, error.message());
    }
    if (IOError::contains(error.code(), kSendFailed)) {
        return EtcdError(ETCD_ERROR_SEND, error.message());
    }
    if (IOError::contains(error.code(), kRecvFailed)) {
        return EtcdError(ETCD_ERROR_RECV, error.message());
    }
    if (IOError::contains(error.code(), kConnectFailed) ||
        IOError::contains(error.code(), kDisconnectError) ||
        IOError::contains(error.code(), kNotRunningOnIOScheduler)) {
        return EtcdError(ETCD_ERROR_CONNECTION, error.message());
    }
    return EtcdError(fallback, error.message());
}

} // namespace

AsyncEtcdClient::AsyncEtcdClient(galay::kernel::IOScheduler* scheduler,
                       EtcdConfig config,
                       AsyncEtcdConfig async_config)
    : m_scheduler(scheduler)
    , m_config(std::move(config))
    , m_async_config(async_config)
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

    m_ip_type = endpoint_result->ip_type;
    m_server_host.emplace(endpoint_result->ip_type, endpoint_result->host, endpoint_result->port);
    m_host_header = buildHostHeader(endpoint_result->host, endpoint_result->port, endpoint_result->ip_type);
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
                                 client.m_async_config.keepalive))
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
        client.setError(ETCD_ERROR_NOT_CONNECTED, "etcd client is not connected");
        return;
    }

    m_ctx = std::make_unique<Context>(client, std::move(api_path), std::move(body));

    if (force_timeout.has_value()) {
        m_ctx->awaitable.timeout(force_timeout.value());
    } else if (client.m_async_config.isRequestTimeoutEnabled()) {
        m_ctx->awaitable.timeout(client.m_async_config.request_timeout);
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
        return std::unexpected(EtcdError(ETCD_ERROR_NOT_CONNECTED, "etcd client is not connected"));
    }

    auto response_result = m_ctx->awaitable.await_resume();
    if (!response_result.has_value()) {
        const auto mapped = mapHttpError(response_result.error());
        m_ctx->owner->setError(mapped);
        return std::unexpected(mapped);
    }

    if (!response_result->has_value()) {
        EtcdError error(ETCD_ERROR_INTERNAL, "http response incomplete");
        m_ctx->owner->setError(error);
        return std::unexpected(error);
    }

    auto response = std::move(response_result->value());
    m_ctx->owner->m_last_status_code = static_cast<int>(response.header().code());
    m_ctx->owner->m_last_response_body = response.getBodyStr();

    if (m_ctx->owner->m_last_status_code < 200 || m_ctx->owner->m_last_status_code >= 300) {
        EtcdError error(
            ETCD_ERROR_SERVER,
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
        EtcdError error(ETCD_ERROR_INVALID_PARAM, "key must not be empty");
        m_client->setError(error);
        return;
    }

    std::string body = "{\"key\":\"" + encodeBase64(key) + "\",\"value\":\"" + encodeBase64(value) + "\"";
    if (lease_id.has_value()) {
        if (lease_id.value() <= 0) {
            EtcdError error(ETCD_ERROR_INVALID_PARAM, "lease id must be positive");
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
        EtcdError error(ETCD_ERROR_INTERNAL, "IOScheduler is null");
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
        m_client->setError(ETCD_ERROR_INVALID_ENDPOINT, message);
        return;
    }

    try {
        m_client->m_socket = std::make_unique<galay::async::TcpSocket>(m_client->m_ip_type);
        auto nonblock_result = m_client->m_socket->option().handleNonBlock();
        if (!nonblock_result.has_value()) {
            EtcdError error = mapKernelIoError(nonblock_result.error(), ETCD_ERROR_CONNECTION);
            m_client->setError(error);
            m_client->m_socket.reset();
            m_client->m_connected = false;
            return;
        }
        startIo(m_client->m_socket->connect(m_client->m_server_host.value()));
    } catch (const std::exception& ex) {
        EtcdError error(ETCD_ERROR_CONNECTION, ex.what());
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
            m_client->m_async_config.buffer_size);
        m_client->m_connected = true;
        return {};
    } catch (const std::exception& ex) {
        EtcdError error(ETCD_ERROR_INTERNAL, std::string("create http session failed: ") + ex.what());
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
        EtcdError error(ETCD_ERROR_INVALID_PARAM, "key must not be empty");
        m_client->setError(error);
        return;
    }
    if (limit.has_value() && limit.value() <= 0) {
        EtcdError error(ETCD_ERROR_INVALID_PARAM, "limit must be positive");
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
        EtcdError error(ETCD_ERROR_INVALID_PARAM, "key must not be empty");
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
        EtcdError error(ETCD_ERROR_INVALID_PARAM, "ttl must be positive");
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
        EtcdError error(ETCD_ERROR_PARSE, "lease grant response missing ID");
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
        EtcdError error(ETCD_ERROR_INVALID_PARAM, "lease id must be positive");
        m_client->setError(error);
        return;
    }

    const std::string body = "{\"ID\":\"" + std::to_string(m_lease_id) + "\"}";
    std::optional<std::chrono::milliseconds> timeout = std::nullopt;
    if (!m_client->m_async_config.isRequestTimeoutEnabled()) {
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
        EtcdError error(ETCD_ERROR_PARSE, "lease keepalive response id mismatch");
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
            EtcdError error(ETCD_ERROR_SERVER, "pipeline txn returned succeeded=false");
            m_client->setError(error);
            m_client->m_last_pipeline_results.clear();
            return std::unexpected(error);
        }
    }

    auto responses_field = root.value()["responses"];
    if (responses_field.error()) {
        EtcdError error(ETCD_ERROR_PARSE, "pipeline txn response missing responses field");
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
            ETCD_ERROR_PARSE,
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
                EtcdError error(ETCD_ERROR_PARSE, "pipeline put response missing response_put");
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
                EtcdError error(ETCD_ERROR_PARSE, "pipeline get response missing response_range");
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
                EtcdError error(ETCD_ERROR_PARSE, "pipeline delete response missing response_delete_range");
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
    m_last_error = EtcdError(ETCD_ERROR_SUCCESS);
    m_last_bool = false;
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
