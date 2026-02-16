#include "EtcdClient.h"

#include <utility>

namespace galay::etcd
{

EtcdClient::EtcdClient(galay::kernel::IOScheduler* scheduler,
                       EtcdConfig config,
                       AsyncEtcdConfig async_config)
    : m_client(scheduler, std::move(config), async_config)
{
}

EtcdClient::ConnectAwaitable EtcdClient::connect()
{
    return m_client.connect();
}

EtcdClient::CloseAwaitable EtcdClient::close()
{
    return m_client.close();
}

EtcdClient::PutAwaitable EtcdClient::put(const std::string& key,
                                         const std::string& value,
                                         std::optional<int64_t> lease_id)
{
    return m_client.put(key, value, lease_id);
}

EtcdClient::GetAwaitable EtcdClient::get(const std::string& key,
                                         bool prefix,
                                         std::optional<int64_t> limit)
{
    return m_client.get(key, prefix, limit);
}

EtcdClient::DeleteAwaitable EtcdClient::del(const std::string& key, bool prefix)
{
    return m_client.del(key, prefix);
}

EtcdClient::GrantLeaseAwaitable EtcdClient::grantLease(int64_t ttl_seconds)
{
    return m_client.grantLease(ttl_seconds);
}

EtcdClient::KeepAliveAwaitable EtcdClient::keepAliveOnce(int64_t lease_id)
{
    return m_client.keepAliveOnce(lease_id);
}

EtcdClient::PipelineAwaitable EtcdClient::pipeline(std::vector<PipelineOp> operations)
{
    return m_client.pipeline(std::move(operations));
}

bool EtcdClient::connected() const
{
    return m_client.connected();
}

EtcdError EtcdClient::lastError() const
{
    return m_client.lastError();
}

bool EtcdClient::lastBool() const
{
    return m_client.lastBool();
}

int64_t EtcdClient::lastLeaseId() const
{
    return m_client.lastLeaseId();
}

int64_t EtcdClient::lastDeletedCount() const
{
    return m_client.lastDeletedCount();
}

const std::vector<EtcdKeyValue>& EtcdClient::lastKeyValues() const
{
    return m_client.lastKeyValues();
}

const std::vector<EtcdClient::PipelineItemResult>& EtcdClient::lastPipelineResults() const
{
    return m_client.lastPipelineResults();
}

int EtcdClient::lastStatusCode() const
{
    return m_client.lastStatusCode();
}

const std::string& EtcdClient::lastResponseBody() const
{
    return m_client.lastResponseBody();
}

} // namespace galay::etcd

