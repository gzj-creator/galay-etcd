#include "AsyncBenchmarkSupport.h"

#include "galay-etcd/async/AsyncEtcdClient.h"

#include <galay-kernel/kernel/Runtime.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using galay::kernel::IOScheduler;
using galay::kernel::Runtime;
using galay::kernel::RuntimeBuilder;
using galay::kernel::Task;

namespace galay::etcd::benchmark
{

namespace
{

struct SharedState
{
    explicit SharedState(int worker_count)
        : pending_workers(worker_count)
        , latency_by_worker(static_cast<size_t>(worker_count))
    {
    }

    std::atomic<int> pending_workers;
    std::atomic<int64_t> success{0};
    std::atomic<int64_t> failure{0};
    std::vector<std::vector<int64_t>> latency_by_worker;
    std::mutex error_mutex;
    std::string first_error;
};

std::string payloadOfSize(int size)
{
    return std::string(static_cast<size_t>(std::max(size, 1)), 'x');
}

std::string nowSuffix()
{
    const auto now = std::chrono::high_resolution_clock::now().time_since_epoch();
    return std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(now).count());
}

int resolvedSchedulerCount(const AsyncBenchmarkArgs& args)
{
    if (args.io_schedulers > 0) {
        return std::max(1, std::min(args.io_schedulers, args.workers));
    }

    const unsigned int hardware = std::thread::hardware_concurrency();
    const int hardware_count = hardware == 0 ? 1 : static_cast<int>(hardware);
    return std::max(1, std::min(args.workers, hardware_count));
}

void rememberFirstError(const std::shared_ptr<SharedState>& state, const std::string& error)
{
    if (error.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(state->error_mutex);
    if (state->first_error.empty()) {
        state->first_error = error;
    }
}

Task<void> runWorker(std::shared_ptr<SharedState> state,
                     int worker_id,
                     AsyncBenchmarkArgs args,
                     std::string key_prefix,
                     std::string value,
                     IOScheduler* scheduler)
{
    auto finish = [&]() {
        state->pending_workers.fetch_sub(1, std::memory_order_release);
    };

    int completed_ops = 0;

    try {
        AsyncEtcdConfig config;
        config.endpoint = std::move(args.endpoint);

        auto client = AsyncEtcdClientBuilder().scheduler(scheduler).config(config).build();
        auto connect_result = co_await client.connect();
        if (!connect_result.has_value()) {
            rememberFirstError(state, connect_result.error().message());
            state->failure.fetch_add(args.ops_per_worker, std::memory_order_relaxed);
            finish();
            co_return;
        }

        auto& worker_latency = state->latency_by_worker[static_cast<size_t>(worker_id)];
        worker_latency.reserve(static_cast<size_t>(args.ops_per_worker));

        for (int i = 0; i < args.ops_per_worker; ++i) {
            const std::string key =
                key_prefix + std::to_string(worker_id) + "/" + std::to_string(i);
            const auto begin = std::chrono::steady_clock::now();

            bool ok = false;
            if (args.mode == AsyncBenchmarkMode::Mixed) {
                auto put_result = co_await client.put(key, value);
                if (put_result.has_value()) {
                    auto get_result = co_await client.get(key);
                    ok = get_result.has_value() &&
                        !client.lastKeyValues().empty() &&
                        client.lastKeyValues().front().value == value;
                    if (!ok && !get_result.has_value()) {
                        rememberFirstError(state, get_result.error().message());
                    } else if (!ok) {
                        rememberFirstError(state, "mixed benchmark verification failed");
                    }
                } else {
                    rememberFirstError(state, put_result.error().message());
                }
            } else {
                auto put_result = co_await client.put(key, value);
                ok = put_result.has_value();
                if (!ok) {
                    rememberFirstError(state, put_result.error().message());
                }
            }

            const auto end = std::chrono::steady_clock::now();
            worker_latency.push_back(
                std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count());

            if (ok) {
                state->success.fetch_add(1, std::memory_order_relaxed);
            } else {
                state->failure.fetch_add(1, std::memory_order_relaxed);
            }
            ++completed_ops;
        }

        auto close_result = co_await client.close();
        if (!close_result.has_value()) {
            rememberFirstError(state, close_result.error().message());
        }
    } catch (const std::exception& ex) {
        rememberFirstError(state, ex.what());
        state->failure.fetch_add(args.ops_per_worker - completed_ops, std::memory_order_relaxed);
    } catch (...) {
        rememberFirstError(state, "unknown async benchmark worker exception");
        state->failure.fetch_add(args.ops_per_worker - completed_ops, std::memory_order_relaxed);
    }

    finish();
}

} // namespace

const char* toString(AsyncBenchmarkMode mode) noexcept
{
    switch (mode) {
    case AsyncBenchmarkMode::Put:
        return "put";
    case AsyncBenchmarkMode::Mixed:
        return "mixed(put+get)";
    }
    return "unknown";
}

std::expected<AsyncBenchmarkMode, std::string> parseAsyncBenchmarkMode(const std::string& value)
{
    if (value.empty() || value == "put") {
        return AsyncBenchmarkMode::Put;
    }
    if (value == "mixed") {
        return AsyncBenchmarkMode::Mixed;
    }
    return std::unexpected("unsupported mode: " + value);
}

double percentile(std::vector<int64_t> samples_us, double p)
{
    if (samples_us.empty()) {
        return 0.0;
    }
    std::sort(samples_us.begin(), samples_us.end());
    const double rank = p * static_cast<double>(samples_us.size() - 1);
    const size_t idx = static_cast<size_t>(rank);
    return static_cast<double>(samples_us[idx]);
}

std::expected<AsyncBenchmarkResult, std::string>
runAsyncBenchmark(const AsyncBenchmarkArgs& args)
{
    if (args.workers <= 0) {
        return std::unexpected("workers must be > 0");
    }
    if (args.ops_per_worker <= 0) {
        return std::unexpected("ops_per_worker must be > 0");
    }
    if (args.value_size <= 0) {
        return std::unexpected("value_size must be > 0");
    }
    if (args.timeout_seconds <= 0) {
        return std::unexpected("timeout_seconds must be > 0");
    }

    const int io_schedulers = resolvedSchedulerCount(args);
    Runtime runtime = RuntimeBuilder().ioSchedulerCount(io_schedulers).computeSchedulerCount(0).build();
    runtime.start();

    std::vector<IOScheduler*> schedulers;
    schedulers.reserve(static_cast<size_t>(io_schedulers));
    for (int i = 0; i < io_schedulers; ++i) {
        auto* scheduler = runtime.getNextIOScheduler();
        if (scheduler == nullptr) {
            runtime.stop();
            return std::unexpected("failed to get io scheduler");
        }
        schedulers.push_back(scheduler);
    }

    auto state = std::make_shared<SharedState>(args.workers);
    const std::string value = payloadOfSize(args.value_size);
    const std::string key_prefix = "/galay-etcd/bench/async/" + nowSuffix() + "/";

    const auto begin = std::chrono::steady_clock::now();
    for (int worker_id = 0; worker_id < args.workers; ++worker_id) {
        IOScheduler* scheduler = schedulers[static_cast<size_t>(worker_id % io_schedulers)];
        if (!galay::kernel::scheduleTask(
                scheduler,
                runWorker(state, worker_id, args, key_prefix, value, scheduler))) {
            runtime.stop();
            return std::unexpected("failed to schedule async benchmark worker " + std::to_string(worker_id));
        }
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(args.timeout_seconds);
    while (state->pending_workers.load(std::memory_order_acquire) > 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    if (state->pending_workers.load(std::memory_order_acquire) > 0) {
        runtime.stop();
        return std::unexpected("async benchmark timeout after " + std::to_string(args.timeout_seconds) + " seconds");
    }

    const auto end = std::chrono::steady_clock::now();
    runtime.stop();

    AsyncBenchmarkResult result;
    result.endpoint = args.endpoint;
    result.mode = args.mode;
    result.workers = args.workers;
    result.ops_per_worker = args.ops_per_worker;
    result.value_size = args.value_size;
    result.io_schedulers = io_schedulers;
    result.success = state->success.load(std::memory_order_relaxed);
    result.failure = state->failure.load(std::memory_order_relaxed);
    result.total_ops = result.success + result.failure;
    result.duration_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - begin).count();
    result.throughput = result.duration_seconds > 0.0
        ? static_cast<double>(result.success) / result.duration_seconds
        : 0.0;
    result.first_error = state->first_error;

    result.latency_us.reserve(static_cast<size_t>(args.workers * args.ops_per_worker));
    for (const auto& worker_latency : state->latency_by_worker) {
        result.latency_us.insert(result.latency_us.end(), worker_latency.begin(), worker_latency.end());
    }

    return result;
}

} // namespace galay::etcd::benchmark
