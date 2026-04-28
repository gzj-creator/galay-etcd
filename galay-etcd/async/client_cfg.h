#ifndef GALAY_ETCD_ASYNC_CONFIG_H
#define GALAY_ETCD_ASYNC_CONFIG_H

#include "galay-etcd/base/etcd_config.h"

namespace galay::etcd
{

struct AsyncEtcdConfig : EtcdConfig
{
    static AsyncEtcdConfig withTimeout(std::chrono::milliseconds timeout)
    {
        AsyncEtcdConfig cfg;
        cfg.request_timeout = timeout;
        return cfg;
    }
};

} // namespace galay::etcd

#endif // GALAY_ETCD_ASYNC_CONFIG_H
