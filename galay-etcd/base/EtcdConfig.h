#ifndef GALAY_ETCD_CONFIG_H
#define GALAY_ETCD_CONFIG_H

#include <string>

namespace galay::etcd
{

struct EtcdConfig
{
    std::string endpoint = "http://127.0.0.1:2379";
    std::string api_prefix = "/v3";
};

} // namespace galay::etcd

#endif // GALAY_ETCD_CONFIG_H
