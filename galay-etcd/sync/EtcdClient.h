#ifndef GALAY_ETCD_SYNC_CLIENT_H
#define GALAY_ETCD_SYNC_CLIENT_H

#include "galay-etcd/async/AsyncEtcdClient.h"
#include "galay-etcd/async/AsyncEtcdConfig.h"
#include "galay-etcd/base/EtcdConfig.h"
#include "galay-etcd/base/EtcdError.h"
#include "galay-etcd/base/EtcdValue.h"

#include <galay-kernel/kernel/IOScheduler.hpp>

#include <optional>
#include <string>
#include <vector>

namespace galay::etcd
{

class EtcdClient
{
public:
    using ConnectAwaitable = AsyncEtcdClient::ConnectAwaitable;
    using CloseAwaitable = AsyncEtcdClient::CloseAwaitable;
    using PutAwaitable = AsyncEtcdClient::PutAwaitable;
    using GetAwaitable = AsyncEtcdClient::GetAwaitable;
    using DeleteAwaitable = AsyncEtcdClient::DeleteAwaitable;
    using GrantLeaseAwaitable = AsyncEtcdClient::GrantLeaseAwaitable;
    using KeepAliveAwaitable = AsyncEtcdClient::KeepAliveAwaitable;
    using PipelineAwaitable = AsyncEtcdClient::PipelineAwaitable;
    using PipelineOp = AsyncEtcdClient::PipelineOp;
    using PipelineItemResult = AsyncEtcdClient::PipelineItemResult;

    explicit EtcdClient(galay::kernel::IOScheduler* scheduler,
                        EtcdConfig config = {},
                        AsyncEtcdConfig async_config = {});

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
    PipelineAwaitable pipeline(std::vector<PipelineOp> operations);

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
    AsyncEtcdClient m_client;
};

} // namespace galay::etcd

#endif // GALAY_ETCD_SYNC_CLIENT_H

