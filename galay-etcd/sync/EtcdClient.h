#ifndef GALAY_ETCD_SYNC_CLIENT_H
#define GALAY_ETCD_SYNC_CLIENT_H

#include "galay-etcd/async/AsyncEtcdConfig.h"
#include "galay-etcd/base/EtcdConfig.h"
#include "galay-etcd/base/EtcdError.h"
#include "galay-etcd/base/EtcdValue.h"

#include <chrono>
#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <vector>

namespace galay::etcd
{

using EtcdVoidResult = std::expected<void, EtcdError>;

class EtcdClient
{
public:
    enum class PipelineOpType
    {
        Put,
        Get,
        Delete,
    };

    struct PipelineOp
    {
        PipelineOpType type = PipelineOpType::Put;
        std::string key;
        std::string value;
        bool prefix = false;
        std::optional<int64_t> limit = std::nullopt;
        std::optional<int64_t> lease_id = std::nullopt;

        static PipelineOp Put(std::string key,
                              std::string value,
                              std::optional<int64_t> lease_id = std::nullopt)
        {
            PipelineOp op;
            op.type = PipelineOpType::Put;
            op.key = std::move(key);
            op.value = std::move(value);
            op.lease_id = lease_id;
            return op;
        }

        static PipelineOp Get(std::string key,
                              bool prefix = false,
                              std::optional<int64_t> limit = std::nullopt)
        {
            PipelineOp op;
            op.type = PipelineOpType::Get;
            op.key = std::move(key);
            op.prefix = prefix;
            op.limit = limit;
            return op;
        }

        static PipelineOp Del(std::string key, bool prefix = false)
        {
            PipelineOp op;
            op.type = PipelineOpType::Delete;
            op.key = std::move(key);
            op.prefix = prefix;
            return op;
        }
    };

    struct PipelineItemResult
    {
        PipelineOpType type = PipelineOpType::Put;
        bool ok = false;
        int64_t deleted_count = 0;
        std::vector<EtcdKeyValue> kvs;
    };

    explicit EtcdClient(EtcdConfig config = {},
                        AsyncEtcdConfig async_config = {});
    ~EtcdClient();

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
                                    const std::string& body,
                                    std::optional<std::chrono::milliseconds> force_timeout = std::nullopt);

private:
    EtcdConfig m_config;
    AsyncEtcdConfig m_async_config;
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

#endif // GALAY_ETCD_SYNC_CLIENT_H
