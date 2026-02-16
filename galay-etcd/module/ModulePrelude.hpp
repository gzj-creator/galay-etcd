#pragma once

#if __has_include(<chrono>)
#include <chrono>
#endif
#if __has_include(<cstdint>)
#include <cstdint>
#endif
#if __has_include(<expected>)
#include <expected>
#endif
#if __has_include(<memory>)
#include <memory>
#endif
#if __has_include(<optional>)
#include <optional>
#endif
#if __has_include(<string>)
#include <string>
#endif
#if __has_include(<vector>)
#include <vector>
#endif

#if __has_include(<galay-http/kernel/http/HttpSession.h>)
#include <galay-http/kernel/http/HttpSession.h>
#endif
#if __has_include(<galay-kernel/async/TcpSocket.h>)
#include <galay-kernel/async/TcpSocket.h>
#endif
#if __has_include(<galay-kernel/kernel/Coroutine.h>)
#include <galay-kernel/kernel/Coroutine.h>
#endif
#if __has_include(<galay-kernel/kernel/IOScheduler.hpp>)
#include <galay-kernel/kernel/IOScheduler.hpp>
#endif
#if __has_include(<galay-kernel/kernel/Runtime.h>)
#include <galay-kernel/kernel/Runtime.h>
#endif
#if __has_include(<galay-utils/algorithm/Base64.hpp>)
#include <galay-utils/algorithm/Base64.hpp>
#endif

#if __has_include("galay-etcd/base/EtcdConfig.h")
#include "galay-etcd/base/EtcdConfig.h"
#endif
#if __has_include("galay-etcd/base/EtcdError.h")
#include "galay-etcd/base/EtcdError.h"
#endif
#if __has_include("galay-etcd/base/EtcdValue.h")
#include "galay-etcd/base/EtcdValue.h"
#endif
#if __has_include("galay-etcd/async/AsyncEtcdConfig.h")
#include "galay-etcd/async/AsyncEtcdConfig.h"
#endif
#if __has_include("galay-etcd/async/AsyncEtcdClient.h")
#include "galay-etcd/async/AsyncEtcdClient.h"
#endif
#if __has_include("galay-etcd/sync/EtcdClient.h")
#include "galay-etcd/sync/EtcdClient.h"
#endif
