#include "EtcdClient.h"

#include "galay-etcd/base/EtcdInternal.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <string_view>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <utility>

namespace galay::etcd
{

using namespace internal;

namespace
{

std::string_view trimAscii(std::string_view value)
{
    size_t begin = 0;
    while (begin < value.size() && std::isspace(static_cast<unsigned char>(value[begin]))) {
        ++begin;
    }
    size_t end = value.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
        --end;
    }
    return value.substr(begin, end - begin);
}

std::string toLowerCopy(std::string_view value)
{
    std::string lower(value);
    std::transform(
        lower.begin(),
        lower.end(),
        lower.begin(),
        [](const unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return lower;
}

std::string buildJsonPostRequest(
    std::string_view uri,
    std::string_view body,
    std::string_view host_header,
    bool keepalive)
{
    std::string request;
    request.reserve(uri.size() + body.size() + 256);
    request += "POST ";
    request += uri;
    request += " HTTP/1.1\r\n";
    request += "Host: ";
    request += host_header;
    request += "\r\n";
    request += "Accept: application/json\r\n";
    request += "Connection: ";
    request += keepalive ? "keep-alive\r\n" : "close\r\n";
    request += "Content-Type: application/json\r\n";
    request += "Content-Length: ";
    request += std::to_string(body.size());
    request += "\r\n\r\n";
    request += body;
    return request;
}

bool isTimeoutErrno(int error_number)
{
    return error_number == EAGAIN || error_number == EWOULDBLOCK || error_number == ETIMEDOUT;
}

EtcdError makeErrnoError(EtcdErrorType type, const std::string& action, int error_number)
{
    return EtcdError(
        type,
        action + ": " + std::string(std::strerror(error_number)));
}

bool setSocketBlocking(int fd, bool blocking)
{
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }

    if (blocking) {
        flags &= ~O_NONBLOCK;
    } else {
        flags |= O_NONBLOCK;
    }
    return ::fcntl(fd, F_SETFL, flags) == 0;
}

EtcdVoidResult connectWithTimeout(
    int fd,
    const sockaddr* address,
    socklen_t address_len,
    std::chrono::milliseconds timeout)
{
    if (timeout.count() < 0) {
        if (::connect(fd, address, address_len) == 0) {
            return {};
        }
        const int error_number = errno;
        if (isTimeoutErrno(error_number)) {
            return std::unexpected(makeErrnoError(EtcdErrorType::Timeout, "connect timeout", error_number));
        }
        return std::unexpected(makeErrnoError(EtcdErrorType::Connection, "connect failed", error_number));
    }

    if (!setSocketBlocking(fd, false)) {
        return std::unexpected(makeErrnoError(EtcdErrorType::Connection, "set nonblocking for connect failed", errno));
    }

    if (::connect(fd, address, address_len) == 0) {
        if (!setSocketBlocking(fd, true)) {
            return std::unexpected(makeErrnoError(EtcdErrorType::Connection, "restore blocking mode failed", errno));
        }
        return {};
    }

    if (errno != EINPROGRESS) {
        const int error_number = errno;
        (void)setSocketBlocking(fd, true);
        if (isTimeoutErrno(error_number)) {
            return std::unexpected(makeErrnoError(EtcdErrorType::Timeout, "connect timeout", error_number));
        }
        return std::unexpected(makeErrnoError(EtcdErrorType::Connection, "connect failed", error_number));
    }

    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLOUT;

    const long long timeout_count = timeout.count();
    const int timeout_ms = timeout_count > static_cast<long long>(INT_MAX)
        ? INT_MAX
        : static_cast<int>(std::max<long long>(0, timeout_count));

    int poll_result = 0;
    do {
        poll_result = ::poll(&pfd, 1, timeout_ms);
    } while (poll_result < 0 && errno == EINTR);

    if (poll_result == 0) {
        (void)setSocketBlocking(fd, true);
        return std::unexpected(EtcdError(EtcdErrorType::Timeout, "connect timeout"));
    }
    if (poll_result < 0) {
        const int error_number = errno;
        (void)setSocketBlocking(fd, true);
        return std::unexpected(makeErrnoError(EtcdErrorType::Connection, "poll connect failed", error_number));
    }

    int socket_error = 0;
    socklen_t socket_error_len = sizeof(socket_error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_len) != 0) {
        const int error_number = errno;
        (void)setSocketBlocking(fd, true);
        return std::unexpected(makeErrnoError(EtcdErrorType::Connection, "getsockopt connect failed", error_number));
    }

    if (!setSocketBlocking(fd, true)) {
        return std::unexpected(makeErrnoError(EtcdErrorType::Connection, "restore blocking mode failed", errno));
    }

    if (socket_error != 0) {
        if (isTimeoutErrno(socket_error)) {
            return std::unexpected(makeErrnoError(EtcdErrorType::Timeout, "connect timeout", socket_error));
        }
        return std::unexpected(makeErrnoError(EtcdErrorType::Connection, "connect failed", socket_error));
    }

    return {};
}

EtcdVoidResult sendAll(int fd, std::string_view payload)
{
    size_t sent = 0;
    while (sent < payload.size()) {
        const char* begin = payload.data() + sent;
        const size_t remaining = payload.size() - sent;
        const ssize_t sent_now = ::send(fd, begin, remaining, 0);
        if (sent_now > 0) {
            sent += static_cast<size_t>(sent_now);
            continue;
        }
        if (sent_now == 0) {
            return std::unexpected(EtcdError(EtcdErrorType::Send, "send returned zero"));
        }
        if (errno == EINTR) {
            continue;
        }
        if (isTimeoutErrno(errno)) {
            return std::unexpected(makeErrnoError(EtcdErrorType::Timeout, "send timeout", errno));
        }
        return std::unexpected(makeErrnoError(EtcdErrorType::Send, "send failed", errno));
    }
    return {};
}

struct ParsedHttpHeaders
{
    int status_code = 0;
    std::optional<size_t> content_length = std::nullopt;
    bool chunked = false;
    bool connection_close = false;
};

std::expected<ParsedHttpHeaders, EtcdError> parseHttpHeaders(std::string_view header_block)
{
    ParsedHttpHeaders headers;

    const size_t status_line_end = header_block.find("\r\n");
    const std::string_view status_line = status_line_end == std::string_view::npos
        ? header_block
        : header_block.substr(0, status_line_end);
    if (status_line.empty()) {
        return std::unexpected(EtcdError(EtcdErrorType::Parse, "invalid http response status line"));
    }
    const size_t first_space = status_line.find(' ');
    if (first_space == std::string_view::npos) {
        return std::unexpected(EtcdError(EtcdErrorType::Parse, "invalid http status line format"));
    }
    size_t second_space = status_line.find(' ', first_space + 1);
    if (second_space == std::string_view::npos) {
        second_space = status_line.size();
    }

    int status_code = 0;
    const std::string_view status_code_view = trimAscii(status_line.substr(first_space + 1, second_space - first_space - 1));
    const char* code_begin = status_code_view.data();
    const char* code_end = code_begin + status_code_view.size();
    auto [status_ptr, status_ec] = std::from_chars(code_begin, code_end, status_code);
    if (status_ec != std::errc() || status_ptr != code_end) {
        return std::unexpected(EtcdError(EtcdErrorType::Parse, "invalid http status code"));
    }
    headers.status_code = status_code;

    size_t line_pos = status_line_end == std::string_view::npos
        ? header_block.size()
        : status_line_end + 2;
    while (line_pos < header_block.size()) {
        size_t line_end = header_block.find("\r\n", line_pos);
        if (line_end == std::string_view::npos) {
            line_end = header_block.size();
        }
        if (line_end == line_pos) {
            line_pos = line_end + 2;
            continue;
        }

        const std::string_view line = header_block.substr(line_pos, line_end - line_pos);
        const size_t colon = line.find(':');
        if (colon != std::string_view::npos) {
            const std::string key = toLowerCopy(trimAscii(line.substr(0, colon)));
            const std::string value = toLowerCopy(trimAscii(line.substr(colon + 1)));

            if (key == "content-length") {
                uint64_t parsed = 0;
                const char* len_begin = value.data();
                const char* len_end = len_begin + value.size();
                auto [len_ptr, len_ec] = std::from_chars(len_begin, len_end, parsed);
                if (len_ec != std::errc() || len_ptr != len_end) {
                    return std::unexpected(EtcdError(EtcdErrorType::Parse, "invalid content-length value"));
                }
                if (parsed > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
                    return std::unexpected(EtcdError(EtcdErrorType::Parse, "content-length value too large"));
                }
                headers.content_length = static_cast<size_t>(parsed);
            } else if (key == "transfer-encoding") {
                if (value.find("chunked") != std::string::npos) {
                    headers.chunked = true;
                }
            } else if (key == "connection") {
                if (value.find("close") != std::string::npos) {
                    headers.connection_close = true;
                }
            }
        }

        if (line_end == header_block.size()) {
            break;
        }
        line_pos = line_end + 2;
    }

    return headers;
}

enum class ChunkDecodeState
{
    Complete,
    Incomplete,
    Error,
};

struct ChunkDecodeResult
{
    ChunkDecodeState state = ChunkDecodeState::Incomplete;
    std::string body;
    size_t consumed = 0;
    std::string error;
};

ChunkDecodeResult decodeChunkedBody(std::string_view raw)
{
    ChunkDecodeResult result;
    size_t pos = 0;
    std::string decoded;

    auto make_error = [](const std::string& message) {
        ChunkDecodeResult res;
        res.state = ChunkDecodeState::Error;
        res.error = message;
        return res;
    };

    while (true) {
        const size_t line_end = raw.find("\r\n", pos);
        if (line_end == std::string_view::npos) {
            return result;
        }

        std::string_view size_line = trimAscii(raw.substr(pos, line_end - pos));
        const size_t ext_sep = size_line.find(';');
        if (ext_sep != std::string_view::npos) {
            size_line = trimAscii(size_line.substr(0, ext_sep));
        }
        if (size_line.empty()) {
            return make_error("invalid chunk size line");
        }

        uint64_t chunk_size = 0;
        auto [size_ptr, size_ec] = std::from_chars(
            size_line.data(),
            size_line.data() + size_line.size(),
            chunk_size,
            16);
        if (size_ec != std::errc() || size_ptr != size_line.data() + size_line.size()) {
            return make_error("invalid chunk size value");
        }

        pos = line_end + 2;
        if (chunk_size == 0) {
            const size_t trailer_end = raw.find("\r\n\r\n", pos);
            if (trailer_end == std::string_view::npos) {
                return result;
            }
            result.state = ChunkDecodeState::Complete;
            result.body = std::move(decoded);
            result.consumed = trailer_end + 4;
            return result;
        }

        if (chunk_size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            return make_error("chunk size too large");
        }
        const size_t body_size = static_cast<size_t>(chunk_size);
        if (raw.size() < pos + body_size + 2) {
            return result;
        }

        decoded.append(raw.data() + pos, body_size);
        pos += body_size;
        if (raw.compare(pos, 2, "\r\n") != 0) {
            return make_error("missing CRLF after chunk data");
        }
        pos += 2;
    }
}

struct HttpResponseData
{
    int status_code = 0;
    std::string body;
    bool connection_close = false;
};

std::expected<HttpResponseData, EtcdError> recvHttpResponse(int fd, size_t buffer_size)
{
    const size_t read_size = std::max<size_t>(buffer_size, 1024);
    std::string raw;
    raw.reserve(read_size * 2);
    std::vector<char> buffer(read_size);

    std::optional<ParsedHttpHeaders> headers = std::nullopt;
    size_t header_end = std::string::npos;
    bool peer_closed = false;

    while (true) {
        if (headers.has_value()) {
            const size_t body_offset = header_end + 4;

            if (headers->chunked) {
                auto chunked = decodeChunkedBody(std::string_view(raw.data() + body_offset, raw.size() - body_offset));
                if (chunked.state == ChunkDecodeState::Complete) {
                    return HttpResponseData{
                        headers->status_code,
                        std::move(chunked.body),
                        headers->connection_close || peer_closed,
                    };
                }
                if (chunked.state == ChunkDecodeState::Error) {
                    return std::unexpected(EtcdError(EtcdErrorType::Parse, chunked.error));
                }
            } else if (headers->content_length.has_value()) {
                const size_t total_needed = body_offset + headers->content_length.value();
                if (raw.size() >= total_needed) {
                    return HttpResponseData{
                        headers->status_code,
                        raw.substr(body_offset, headers->content_length.value()),
                        headers->connection_close || peer_closed,
                    };
                }
            } else if (peer_closed) {
                return HttpResponseData{
                    headers->status_code,
                    raw.substr(body_offset),
                    true,
                };
            }
        }

        const ssize_t recv_bytes = ::recv(fd, buffer.data(), buffer.size(), 0);
        if (recv_bytes > 0) {
            raw.append(buffer.data(), static_cast<size_t>(recv_bytes));
            if (!headers.has_value()) {
                header_end = raw.find("\r\n\r\n");
                if (header_end != std::string::npos) {
                    auto parsed = parseHttpHeaders(std::string_view(raw.data(), header_end));
                    if (!parsed.has_value()) {
                        return std::unexpected(parsed.error());
                    }
                    headers = parsed.value();

                    if (!headers->chunked &&
                        !headers->content_length.has_value() &&
                        !headers->connection_close) {
                        return std::unexpected(EtcdError(
                            EtcdErrorType::Parse,
                            "response missing content-length or chunked encoding"));
                    }
                }
            }
            continue;
        }

        if (recv_bytes == 0) {
            peer_closed = true;
            if (!headers.has_value()) {
                return std::unexpected(EtcdError(EtcdErrorType::Connection, "connection closed before response header"));
            }

            const size_t body_offset = header_end + 4;
            if (headers->chunked) {
                auto chunked = decodeChunkedBody(std::string_view(raw.data() + body_offset, raw.size() - body_offset));
                if (chunked.state == ChunkDecodeState::Complete) {
                    return HttpResponseData{
                        headers->status_code,
                        std::move(chunked.body),
                        true,
                    };
                }
                return std::unexpected(EtcdError(EtcdErrorType::Recv, "connection closed before complete chunked body"));
            }

            if (headers->content_length.has_value()) {
                const size_t expected_size = body_offset + headers->content_length.value();
                if (raw.size() < expected_size) {
                    return std::unexpected(EtcdError(EtcdErrorType::Recv, "connection closed before complete response body"));
                }
                return HttpResponseData{
                    headers->status_code,
                    raw.substr(body_offset, headers->content_length.value()),
                    true,
                };
            }

            return HttpResponseData{
                headers->status_code,
                raw.substr(body_offset),
                true,
            };
        }

        if (errno == EINTR) {
            continue;
        }
        if (isTimeoutErrno(errno)) {
            return std::unexpected(makeErrnoError(EtcdErrorType::Timeout, "recv timeout", errno));
        }
        return std::unexpected(makeErrnoError(EtcdErrorType::Recv, "recv failed", errno));
    }
}

} // namespace

EtcdClient::EtcdClient(EtcdConfig config)
    : m_config(std::move(config))
    , m_network_config(m_config)
    , m_api_prefix(normalizeApiPrefix(m_config.api_prefix))
{
    auto endpoint_result = parseEndpoint(m_config.endpoint);
    if (!endpoint_result.has_value()) {
        m_endpoint_error = endpoint_result.error();
        return;
    }

    if (endpoint_result->secure) {
        m_endpoint_error = "https endpoint is not supported in EtcdClient: " + m_config.endpoint;
        return;
    }

    m_endpoint_host = endpoint_result->host;
    m_endpoint_port = endpoint_result->port;
    m_endpoint_secure = endpoint_result->secure;
    m_endpoint_ipv6 = endpoint_result->ipv6;
    m_host_header = buildHostHeader(endpoint_result->host, endpoint_result->port, endpoint_result->ipv6);
    m_endpoint_valid = true;
}

EtcdClient::~EtcdClient()
{
    if (m_socket_fd >= 0) {
        (void)::close(m_socket_fd);
        m_socket_fd = -1;
    }
}

EtcdVoidResult EtcdClient::applySocketTimeout(std::optional<std::chrono::milliseconds> timeout)
{
    if (m_socket_fd < 0) {
        return std::unexpected(EtcdError(EtcdErrorType::NotConnected, "socket not connected"));
    }

    timeval tv{};
    if (timeout.has_value() && timeout.value().count() >= 0) {
        const auto total_ms = timeout.value().count();
        tv.tv_sec = static_cast<decltype(tv.tv_sec)>(total_ms / 1000);
        tv.tv_usec = static_cast<decltype(tv.tv_usec)>((total_ms % 1000) * 1000);
    }

    if (::setsockopt(m_socket_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) != 0) {
        return std::unexpected(makeErrnoError(EtcdErrorType::Connection, "setsockopt SO_SNDTIMEO failed", errno));
    }
    if (::setsockopt(m_socket_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) != 0) {
        return std::unexpected(makeErrnoError(EtcdErrorType::Connection, "setsockopt SO_RCVTIMEO failed", errno));
    }
    return {};
}

EtcdVoidResult EtcdClient::connect()
{
    resetLastOperation();

    if (m_connected && m_socket_fd >= 0) {
        return {};
    }

    if (!m_endpoint_valid) {
        const std::string message = m_endpoint_error.empty()
            ? "invalid endpoint"
            : m_endpoint_error;
        setError(EtcdErrorType::InvalidEndpoint, message);
        return std::unexpected(m_last_error);
    }

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    addrinfo* results = nullptr;
    const std::string port_string = std::to_string(m_endpoint_port);
    const int gai_rc = ::getaddrinfo(m_endpoint_host.c_str(), port_string.c_str(), &hints, &results);
    if (gai_rc != 0) {
        setError(EtcdErrorType::Connection, std::string("getaddrinfo failed: ") + gai_strerror(gai_rc));
        return std::unexpected(m_last_error);
    }

    std::optional<EtcdError> last_connect_error = std::nullopt;
    for (addrinfo* it = results; it != nullptr; it = it->ai_next) {
        const int fd = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd < 0) {
            last_connect_error = makeErrnoError(EtcdErrorType::Connection, "socket create failed", errno);
            continue;
        }

#ifdef SO_NOSIGPIPE
        {
            int nosigpipe = 1;
            (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &nosigpipe, sizeof(nosigpipe));
        }
#endif

        if (m_network_config.keepalive) {
            int enable_keepalive = 1;
            if (::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &enable_keepalive, sizeof(enable_keepalive)) != 0) {
                last_connect_error = makeErrnoError(EtcdErrorType::Connection, "setsockopt SO_KEEPALIVE failed", errno);
                (void)::close(fd);
                continue;
            }
        }

        EtcdVoidResult connect_result{};
        if (m_network_config.isRequestTimeoutEnabled()) {
            connect_result = connectWithTimeout(fd, it->ai_addr, static_cast<socklen_t>(it->ai_addrlen), m_network_config.request_timeout);
        } else {
            connect_result = connectWithTimeout(fd, it->ai_addr, static_cast<socklen_t>(it->ai_addrlen), std::chrono::milliseconds(-1));
        }

        if (!connect_result.has_value()) {
            last_connect_error = connect_result.error();
            (void)::close(fd);
            continue;
        }

        m_socket_fd = fd;
        m_connected = true;

        auto timeout_result = applySocketTimeout(
            m_network_config.isRequestTimeoutEnabled()
                ? std::optional<std::chrono::milliseconds>(m_network_config.request_timeout)
                : std::nullopt);
        if (!timeout_result.has_value()) {
            setError(timeout_result.error());
            (void)::close(m_socket_fd);
            m_socket_fd = -1;
            m_connected = false;
            (void)::freeaddrinfo(results);
            return std::unexpected(m_last_error);
        }

        (void)::freeaddrinfo(results);
        return {};
    }

    (void)::freeaddrinfo(results);
    if (last_connect_error.has_value()) {
        setError(last_connect_error.value());
    } else {
        setError(EtcdErrorType::Connection, "connect failed");
    }
    m_connected = false;
    m_socket_fd = -1;
    return std::unexpected(m_last_error);
}

EtcdVoidResult EtcdClient::close()
{
    resetLastOperation();

    if (m_socket_fd < 0) {
        m_connected = false;
        return {};
    }

    if (::close(m_socket_fd) != 0) {
        const EtcdError error = makeErrnoError(EtcdErrorType::Connection, "close failed", errno);
        setError(error);
        m_socket_fd = -1;
        m_connected = false;
        return std::unexpected(error);
    }

    m_socket_fd = -1;
    m_connected = false;
    return {};
}

EtcdVoidResult EtcdClient::postJsonInternal(
    const std::string& api_path,
    std::string body,
    std::optional<std::chrono::milliseconds> force_timeout)
{
    if (!m_connected || m_socket_fd < 0) {
        EtcdError error(EtcdErrorType::NotConnected, "etcd client is not connected");
        setError(error);
        return std::unexpected(error);
    }

    std::optional<std::chrono::milliseconds> timeout = std::nullopt;
    if (force_timeout.has_value()) {
        timeout = force_timeout.value();
    } else if (m_network_config.isRequestTimeoutEnabled()) {
        timeout = m_network_config.request_timeout;
    }

    auto timeout_result = applySocketTimeout(timeout);
    if (!timeout_result.has_value()) {
        setError(timeout_result.error());
        return std::unexpected(timeout_result.error());
    }

    const std::string request = buildJsonPostRequest(
        m_api_prefix + api_path,
        body,
        m_host_header,
        m_network_config.keepalive);

    auto send_result = sendAll(m_socket_fd, request);
    if (!send_result.has_value()) {
        setError(send_result.error());
        if (m_socket_fd >= 0) {
            (void)::close(m_socket_fd);
            m_socket_fd = -1;
            m_connected = false;
        }
        return std::unexpected(send_result.error());
    }

    auto response_result = recvHttpResponse(m_socket_fd, m_network_config.buffer_size);
    if (!response_result.has_value()) {
        setError(response_result.error());
        if (m_socket_fd >= 0) {
            (void)::close(m_socket_fd);
            m_socket_fd = -1;
            m_connected = false;
        }
        return std::unexpected(response_result.error());
    }

    m_last_status_code = response_result->status_code;
    m_last_response_body = response_result->body;

    if (response_result->connection_close) {
        if (m_socket_fd >= 0) {
            (void)::close(m_socket_fd);
            m_socket_fd = -1;
        }
        m_connected = false;
    }

    if (m_last_status_code < 200 || m_last_status_code >= 300) {
        EtcdError error(
            EtcdErrorType::Server,
            "HTTP status=" + std::to_string(m_last_status_code) +
                ", body=" + m_last_response_body);
        setError(error);
        return std::unexpected(error);
    }

    return {};
}

EtcdVoidResult EtcdClient::put(const std::string& key,
                               const std::string& value,
                               std::optional<int64_t> lease_id)
{
    resetLastOperation();
    if (key.empty()) {
        EtcdError error(EtcdErrorType::InvalidParam, "key must not be empty");
        setError(error);
        return std::unexpected(error);
    }

    std::string body = "{\"key\":\"" + encodeBase64(key) + "\",\"value\":\"" + encodeBase64(value) + "\"";
    if (lease_id.has_value()) {
        if (lease_id.value() <= 0) {
            EtcdError error(EtcdErrorType::InvalidParam, "lease id must be positive");
            setError(error);
            return std::unexpected(error);
        }
        body += ",\"lease\":\"" + std::to_string(lease_id.value()) + "\"";
    }
    body += "}";

    auto result = postJsonInternal("/kv/put", std::move(body));
    if (!result.has_value()) {
        return result;
    }

    if (maybeContainsEtcdErrorFields(m_last_response_body)) {
        auto root = parseEtcdSuccessObject(
            m_last_response_body,
            "parse put response");
        if (!root.has_value()) {
            setError(root.error());
            return std::unexpected(root.error());
        }
    }

    m_last_bool = true;
    return {};
}

EtcdVoidResult EtcdClient::get(const std::string& key,
                               bool prefix,
                               std::optional<int64_t> limit)
{
    resetLastOperation();
    if (key.empty()) {
        EtcdError error(EtcdErrorType::InvalidParam, "key must not be empty");
        setError(error);
        return std::unexpected(error);
    }
    if (limit.has_value() && limit.value() <= 0) {
        EtcdError error(EtcdErrorType::InvalidParam, "limit must be positive");
        setError(error);
        return std::unexpected(error);
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

    auto result = postJsonInternal("/kv/range", std::move(body));
    if (!result.has_value()) {
        return result;
    }

    auto root = parseEtcdSuccessObject(
        m_last_response_body,
        "parse get response");
    if (!root.has_value()) {
        setError(root.error());
        m_last_kvs.clear();
        return std::unexpected(root.error());
    }

    auto kvs_result = parseKvsFromObject(root.value(), "parse get response");
    if (!kvs_result.has_value()) {
        setError(kvs_result.error());
        m_last_kvs.clear();
        return std::unexpected(kvs_result.error());
    }

    m_last_kvs = std::move(kvs_result.value());
    m_last_bool = !m_last_kvs.empty();
    return {};
}

EtcdVoidResult EtcdClient::del(const std::string& key, bool prefix)
{
    resetLastOperation();
    if (key.empty()) {
        EtcdError error(EtcdErrorType::InvalidParam, "key must not be empty");
        setError(error);
        return std::unexpected(error);
    }

    std::string body = "{\"key\":\"" + encodeBase64(key) + "\"";
    if (prefix) {
        const std::string range_end = makePrefixRangeEnd(key);
        body += ",\"range_end\":\"" + encodeBase64(range_end) + "\"";
    }
    body += "}";

    auto result = postJsonInternal("/kv/deleterange", std::move(body));
    if (!result.has_value()) {
        return result;
    }

    auto root = parseEtcdSuccessObject(
        m_last_response_body,
        "parse delete response");
    if (!root.has_value()) {
        setError(root.error());
        return std::unexpected(root.error());
    }

    m_last_deleted_count = findIntField(root.value(), "deleted").value_or(0);
    m_last_bool = m_last_deleted_count > 0;
    return {};
}

EtcdVoidResult EtcdClient::grantLease(int64_t ttl_seconds)
{
    resetLastOperation();
    if (ttl_seconds <= 0) {
        EtcdError error(EtcdErrorType::InvalidParam, "ttl must be positive");
        setError(error);
        return std::unexpected(error);
    }

    const std::string body = "{\"TTL\":" + std::to_string(ttl_seconds) + "}";
    auto result = postJsonInternal("/lease/grant", body);
    if (!result.has_value()) {
        return result;
    }

    auto root = parseEtcdSuccessObject(
        m_last_response_body,
        "parse lease grant response");
    if (!root.has_value()) {
        setError(root.error());
        return std::unexpected(root.error());
    }

    const auto lease_id = findIntField(root.value(), "ID");
    if (!lease_id.has_value()) {
        EtcdError error(EtcdErrorType::Parse, "lease grant response missing ID");
        setError(error);
        return std::unexpected(error);
    }
    m_last_lease_id = lease_id.value();
    m_last_bool = true;
    return {};
}

EtcdVoidResult EtcdClient::keepAliveOnce(int64_t lease_id)
{
    resetLastOperation();
    if (lease_id <= 0) {
        EtcdError error(EtcdErrorType::InvalidParam, "lease id must be positive");
        setError(error);
        return std::unexpected(error);
    }

    const std::string body = "{\"ID\":\"" + std::to_string(lease_id) + "\"}";
    std::optional<std::chrono::milliseconds> timeout = std::nullopt;
    if (!m_network_config.isRequestTimeoutEnabled()) {
        timeout = std::chrono::seconds(5);
    }

    auto result = postJsonInternal("/lease/keepalive", body, timeout);
    if (!result.has_value()) {
        return result;
    }

    auto root = parseEtcdSuccessObject(
        m_last_response_body,
        "parse lease keepalive response");
    if (!root.has_value()) {
        setError(root.error());
        return std::unexpected(root.error());
    }

    const auto response_id = findIntField(root.value(), "ID");
    if (response_id.has_value() && response_id.value() != lease_id) {
        EtcdError error(EtcdErrorType::Parse, "lease keepalive response id mismatch");
        setError(error);
        return std::unexpected(error);
    }

    m_last_lease_id = lease_id;
    m_last_bool = true;
    return {};
}

EtcdVoidResult EtcdClient::pipeline(std::vector<PipelineOp> operations)
{
    resetLastOperation();
    auto body = buildTxnBody(operations);
    if (!body.has_value()) {
        setError(body.error());
        return std::unexpected(body.error());
    }

    auto result = postJsonInternal("/kv/txn", std::move(body.value()));
    if (!result.has_value()) {
        return result;
    }

    auto root = parseEtcdSuccessObject(
        m_last_response_body,
        "parse pipeline txn response");
    if (!root.has_value()) {
        setError(root.error());
        m_last_pipeline_results.clear();
        return std::unexpected(root.error());
    }

    auto succeeded_field = root.value()["succeeded"];
    if (!succeeded_field.error()) {
        auto succeeded_result = succeeded_field.value_unsafe().get_bool();
        if (!succeeded_result.error() && !succeeded_result.value_unsafe()) {
            EtcdError error(EtcdErrorType::Server, "pipeline txn returned succeeded=false");
            setError(error);
            m_last_pipeline_results.clear();
            return std::unexpected(error);
        }
    }

    auto responses_field = root.value()["responses"];
    if (responses_field.error()) {
        EtcdError error(EtcdErrorType::Parse, "pipeline txn response missing responses field");
        setError(error);
        m_last_pipeline_results.clear();
        return std::unexpected(error);
    }

    auto responses_array_result = responses_field.value_unsafe().get_array();
    if (responses_array_result.error()) {
        EtcdError error = makeJsonParseError("parse pipeline responses as array", responses_array_result.error());
        setError(error);
        m_last_pipeline_results.clear();
        return std::unexpected(error);
    }

    const auto responses = responses_array_result.value_unsafe();
    if (responses.size() != operations.size()) {
        EtcdError error(
            EtcdErrorType::Parse,
            "pipeline responses size mismatch, expected=" + std::to_string(operations.size()) +
                ", actual=" + std::to_string(responses.size()));
        setError(error);
        m_last_pipeline_results.clear();
        return std::unexpected(error);
    }

    m_last_pipeline_results.clear();
    m_last_pipeline_results.reserve(operations.size());

    for (size_t i = 0; i < operations.size(); ++i) {
        auto item_object_result = responses.at(i).get_object();
        if (item_object_result.error()) {
            EtcdError error = makeJsonParseError("parse pipeline response item as object", item_object_result.error());
            setError(error);
            m_last_pipeline_results.clear();
            return std::unexpected(error);
        }

        const auto item_object = item_object_result.value_unsafe();
        PipelineItemResult item;
        item.type = operations[i].type;

        switch (operations[i].type) {
        case PipelineOpType::Put: {
            auto put_field = item_object["response_put"];
            if (put_field.error()) {
                EtcdError error(EtcdErrorType::Parse, "pipeline put response missing response_put");
                setError(error);
                m_last_pipeline_results.clear();
                return std::unexpected(error);
            }
            auto put_object_result = put_field.value_unsafe().get_object();
            if (put_object_result.error()) {
                EtcdError error = makeJsonParseError("parse pipeline response_put as object", put_object_result.error());
                setError(error);
                m_last_pipeline_results.clear();
                return std::unexpected(error);
            }
            item.ok = true;
            break;
        }
        case PipelineOpType::Get: {
            auto range_field = item_object["response_range"];
            if (range_field.error()) {
                EtcdError error(EtcdErrorType::Parse, "pipeline get response missing response_range");
                setError(error);
                m_last_pipeline_results.clear();
                return std::unexpected(error);
            }
            auto range_object_result = range_field.value_unsafe().get_object();
            if (range_object_result.error()) {
                EtcdError error = makeJsonParseError("parse pipeline response_range as object", range_object_result.error());
                setError(error);
                m_last_pipeline_results.clear();
                return std::unexpected(error);
            }

            auto kvs_result = parseKvsFromObject(range_object_result.value_unsafe(), "parse pipeline response_range");
            if (!kvs_result.has_value()) {
                setError(kvs_result.error());
                m_last_pipeline_results.clear();
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
                setError(error);
                m_last_pipeline_results.clear();
                return std::unexpected(error);
            }
            auto del_object_result = del_field.value_unsafe().get_object();
            if (del_object_result.error()) {
                EtcdError error = makeJsonParseError("parse pipeline response_delete_range as object", del_object_result.error());
                setError(error);
                m_last_pipeline_results.clear();
                return std::unexpected(error);
            }
            item.deleted_count = findIntField(del_object_result.value_unsafe(), "deleted").value_or(0);
            item.ok = true;
            break;
        }
        }

        m_last_pipeline_results.push_back(std::move(item));
    }

    m_last_bool = true;
    return {};
}

bool EtcdClient::connected() const
{
    return m_connected && m_socket_fd >= 0;
}

EtcdError EtcdClient::lastError() const
{
    return m_last_error;
}

bool EtcdClient::lastBool() const
{
    return m_last_bool;
}

int64_t EtcdClient::lastLeaseId() const
{
    return m_last_lease_id;
}

int64_t EtcdClient::lastDeletedCount() const
{
    return m_last_deleted_count;
}

const std::vector<EtcdKeyValue>& EtcdClient::lastKeyValues() const
{
    return m_last_kvs;
}

const std::vector<EtcdClient::PipelineItemResult>& EtcdClient::lastPipelineResults() const
{
    return m_last_pipeline_results;
}

int EtcdClient::lastStatusCode() const
{
    return m_last_status_code;
}

const std::string& EtcdClient::lastResponseBody() const
{
    return m_last_response_body;
}

EtcdVoidResult EtcdClient::currentResult() const
{
    if (m_last_error.isOk()) {
        return {};
    }
    return std::unexpected(m_last_error);
}

void EtcdClient::resetLastOperation()
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

void EtcdClient::setError(EtcdErrorType type, const std::string& message)
{
    m_last_error = EtcdError(type, message);
}

void EtcdClient::setError(EtcdError error)
{
    m_last_error = std::move(error);
}

} // namespace galay::etcd
