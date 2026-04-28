#ifndef GALAY_ETCD_VALUE_H
#define GALAY_ETCD_VALUE_H

#include <cstdint>
#include <string>

namespace galay::etcd
{

struct EtcdKeyValue
{
    std::string key;
    std::string value;
    int64_t create_revision = 0;
    int64_t mod_revision = 0;
    int64_t version = 0;
    int64_t lease = 0;
};

} // namespace galay::etcd

#endif // GALAY_ETCD_VALUE_H
