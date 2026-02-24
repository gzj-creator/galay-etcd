#ifndef GALAY_ETCD_NETWORK_CONFIG_H
#define GALAY_ETCD_NETWORK_CONFIG_H

#include <chrono>
#include <cstddef>

namespace galay::etcd
{

struct EtcdNetworkConfig
{
    std::chrono::milliseconds request_timeout = std::chrono::milliseconds(-1);
    size_t buffer_size = 16384;
    bool keepalive = true;

    [[nodiscard]] bool isRequestTimeoutEnabled() const
    {
        return request_timeout >= std::chrono::milliseconds(0);
    }

    static EtcdNetworkConfig withTimeout(std::chrono::milliseconds timeout)
    {
        EtcdNetworkConfig cfg;
        cfg.request_timeout = timeout;
        return cfg;
    }
};

} // namespace galay::etcd

#endif // GALAY_ETCD_NETWORK_CONFIG_H
