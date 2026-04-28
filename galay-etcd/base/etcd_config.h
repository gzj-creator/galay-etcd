#ifndef GALAY_ETCD_CONFIG_H
#define GALAY_ETCD_CONFIG_H

#include "galay-etcd/base/network_cfg.h"

#include <string>

namespace galay::etcd
{

struct EtcdConfig : EtcdNetworkConfig
{
    std::string endpoint = "http://127.0.0.1:2379";
    std::string api_prefix = "/v3";

    static EtcdConfig withTimeout(std::chrono::milliseconds timeout)
    {
        EtcdConfig cfg;
        cfg.request_timeout = timeout;
        return cfg;
    }
};

} // namespace galay::etcd

#endif // GALAY_ETCD_CONFIG_H
