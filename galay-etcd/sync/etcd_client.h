#ifndef GALAY_ETCD_SYNC_CLIENT_H
#define GALAY_ETCD_SYNC_CLIENT_H

#include "galay-etcd/base/etcd_config.h"
#include "galay-etcd/base/etcd_error.h"
#include "galay-etcd/base/network_cfg.h"
#include "galay-etcd/base/etcd_types.h"
#include "galay-etcd/base/etcd_value.h"

#include <chrono>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace galay::etcd
{

class EtcdClient;

class EtcdClientBuilder
{
public:
    EtcdClientBuilder& endpoint(std::string endpoint)
    {
        m_config.endpoint = std::move(endpoint);
        return *this;
    }

    EtcdClientBuilder& apiPrefix(std::string prefix)
    {
        m_config.api_prefix = std::move(prefix);
        return *this;
    }

    EtcdClientBuilder& requestTimeout(std::chrono::milliseconds timeout)
    {
        m_config.request_timeout = timeout;
        return *this;
    }

    EtcdClientBuilder& bufferSize(size_t size)
    {
        m_config.buffer_size = size;
        return *this;
    }

    EtcdClientBuilder& keepAlive(bool enabled)
    {
        m_config.keepalive = enabled;
        return *this;
    }

    EtcdClientBuilder& config(EtcdConfig config)
    {
        m_config = std::move(config);
        return *this;
    }

    EtcdClient build() const;

    EtcdConfig buildConfig() const
    {
        return m_config;
    }

private:
    EtcdConfig m_config{};
};

class EtcdClient
{
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

    EtcdBoolResult connect();
    EtcdBoolResult close();

    EtcdBoolResult put(const std::string& key,
                       const std::string& value,
                       std::optional<int64_t> lease_id = std::nullopt);

    EtcdGetResult get(const std::string& key,
                      bool prefix = false,
                      std::optional<int64_t> limit = std::nullopt);

    EtcdDeleteResult del(const std::string& key, bool prefix = false);
    EtcdLeaseGrantResult grantLease(int64_t ttl_seconds);
    EtcdLeaseGrantResult keepAliveOnce(int64_t lease_id);
    EtcdPipelineResult pipeline(std::span<const PipelineOp> operations);
    EtcdPipelineResult pipeline(std::vector<PipelineOp> operations);

    [[nodiscard]] bool connected() const;

private:
    void resetLastOperation();
    void setError(EtcdErrorType type, const std::string& message);
    void setError(EtcdError error);
    EtcdVoidResult applySocketTimeout(std::optional<std::chrono::milliseconds> timeout);
    std::expected<std::string, EtcdError> postJsonInternal(
        const std::string& api_path,
        std::string body,
        std::optional<std::chrono::milliseconds> force_timeout = std::nullopt);

private:
    EtcdConfig m_config;
    EtcdNetworkConfig m_network_config;
    std::string m_api_prefix;
    std::string m_host_header;
    std::string m_endpoint_host;
    uint16_t m_endpoint_port = 0;
    bool m_endpoint_secure = false;
    bool m_endpoint_ipv6 = false;
    std::string m_endpoint_error;
    bool m_endpoint_valid = false;

    int m_socket_fd = -1;
    bool m_connected = false;
    std::optional<std::chrono::milliseconds> m_applied_socket_timeout;
    bool m_socket_timeout_cached = false;
    std::string m_request_buffer;
    std::string m_response_raw_buffer;
    std::vector<char> m_recv_buffer;

    EtcdError m_last_error;
};

} // namespace galay::etcd

inline galay::etcd::EtcdClient galay::etcd::EtcdClientBuilder::build() const
{
    return EtcdClient(m_config);
}

#endif // GALAY_ETCD_SYNC_CLIENT_H
