module;

#include "galay-etcd/module/ModulePrelude.hpp"

export module galay.etcd;

export {
#include "galay-etcd/base/EtcdConfig.h"
#include "galay-etcd/base/EtcdError.h"
#include "galay-etcd/base/EtcdValue.h"
#include "galay-etcd/async/AsyncEtcdConfig.h"
#include "galay-etcd/async/AsyncEtcdClient.h"
#include "galay-etcd/sync/EtcdClient.h"
}
