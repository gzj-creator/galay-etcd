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

#if __has_include(<spdlog/sinks/stdout_color_sinks.h>)
#include <spdlog/sinks/stdout_color_sinks.h>
#endif
#if __has_include(<spdlog/sinks/basic_file_sink.h>)
#include <spdlog/sinks/basic_file_sink.h>
#endif
#if __has_include(<spdlog/spdlog.h>)
#include <spdlog/spdlog.h>
#endif

#if __has_include(<galay-http/kernel/http/http_session.h>)
#include <galay-http/kernel/http/http_session.h>
#endif
#if __has_include(<galay-kernel/async/tcp_socket.h>)
#include <galay-kernel/async/tcp_socket.h>
#endif
#if __has_include(<coroutine>)
#include <coroutine>
#endif
#if __has_include(<galay-kernel/kernel/io_scheduler.hpp>)
#include <galay-kernel/kernel/io_scheduler.hpp>
#endif
#if __has_include(<galay-kernel/kernel/runtime.h>)
#include <galay-kernel/kernel/runtime.h>
#endif
#if __has_include(<galay-utils/algorithm/base64.hpp>)
#include <galay-utils/algorithm/base64.hpp>
#endif

#if __has_include("galay-etcd/base/etcd_config.h")
#include "galay-etcd/base/etcd_config.h"
#endif
#if __has_include("galay-etcd/base/etcd_error.h")
#include "galay-etcd/base/etcd_error.h"
#endif
#if __has_include("galay-etcd/base/etcd_log.h")
#include "galay-etcd/base/etcd_log.h"
#endif
#if __has_include("galay-etcd/base/etcd_value.h")
#include "galay-etcd/base/etcd_value.h"
#endif
#if __has_include("galay-etcd/async/client_cfg.h")
#include "galay-etcd/async/client_cfg.h"
#endif
#if __has_include("galay-etcd/async/etcd_client.h")
#include "galay-etcd/async/etcd_client.h"
#endif
#if __has_include("galay-etcd/sync/etcd_client.h")
#include "galay-etcd/sync/etcd_client.h"
#endif
