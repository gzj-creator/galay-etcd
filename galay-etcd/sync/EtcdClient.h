#ifndef GALAY_ETCD_SYNC_CLIENT_H
#define GALAY_ETCD_SYNC_CLIENT_H

#include "galay-etcd/base/EtcdConfig.h"
#include "galay-etcd/base/EtcdError.h"
#include "galay-etcd/base/EtcdNetworkConfig.h"
#include "galay-etcd/base/EtcdTypes.h"
#include "galay-etcd/base/EtcdValue.h"

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

using EtcdVoidResult = std::expected<void, EtcdError>;

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

    [[nodiscard]] bool connected() const;
    [[nodiscard]] EtcdError lastError() const;
    [[nodiscard]] bool lastBool() const;
    [[nodiscard]] int64_t lastLeaseId() const;
    [[nodiscard]] int64_t lastDeletedCount() const;
    [[nodiscard]] const std::vector<EtcdKeyValue>& lastKeyValues() const;
    [[nodiscard]] const std::vector<PipelineItemResult>& lastPipelineResults() const;
    [[nodiscard]] int lastStatusCode() const;
    [[nodiscard]] const std::string& lastResponseBody() const;

private:
    void resetLastOperation();
    void setError(EtcdErrorType type, const std::string& message);
    void setError(EtcdError error);
    [[nodiscard]] EtcdVoidResult currentResult() const;
    EtcdVoidResult applySocketTimeout(std::optional<std::chrono::milliseconds> timeout);
    EtcdVoidResult postJsonInternal(const std::string& api_path,
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
    bool m_last_bool = false;
    int64_t m_last_lease_id = 0;
    int64_t m_last_deleted_count = 0;
    int m_last_status_code = 0;
    std::string m_last_response_body;
    std::vector<EtcdKeyValue> m_last_kvs;
    std::vector<PipelineItemResult> m_last_pipeline_results;
};

} // namespace galay::etcd

inline galay::etcd::EtcdClient galay::etcd::EtcdClientBuilder::build() const
{
    return EtcdClient(m_config);
}

#endif // GALAY_ETCD_SYNC_CLIENT_H
