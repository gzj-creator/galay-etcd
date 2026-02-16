#include "galay-etcd/sync/EtcdClient.h"

#include <galay-kernel/kernel/Runtime.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using galay::etcd::EtcdConfig;
using galay::etcd::EtcdClient;
using galay::kernel::Coroutine;
using galay::kernel::IOScheduler;
using galay::kernel::Runtime;

namespace
{

struct Args
{
    std::string endpoint = "http://140.143.142.251:2379";
    int threads = 8;
    int ops_per_thread = 500;
    int value_size = 64;
    bool mixed = false;
};

Args parseArgs(int argc, char** argv)
{
    Args args;
    if (argc > 1) args.endpoint = argv[1];
    if (argc > 2) args.threads = std::max(1, std::stoi(argv[2]));
    if (argc > 3) args.ops_per_thread = std::max(1, std::stoi(argv[3]));
    if (argc > 4) args.value_size = std::max(1, std::stoi(argv[4]));
    if (argc > 5) args.mixed = (std::string(argv[5]) == "mixed");
    return args;
}

std::string payloadOfSize(int size)
{
    return std::string(static_cast<size_t>(size), 'x');
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

Coroutine runWorker(IOScheduler* scheduler,
                    std::string endpoint,
                    std::string key_prefix,
                    std::string value,
                    int worker_id,
                    int ops_per_thread,
                    bool mixed,
                    std::vector<std::vector<int64_t>>* latency_by_worker,
                    std::atomic<int64_t>* success,
                    std::atomic<int64_t>* failure,
                    std::atomic<int>* finished)
{
    EtcdConfig config;
    config.endpoint = std::move(endpoint);

    EtcdClient session(scheduler, config);
    auto conn = co_await session.connect();
    if (!conn.has_value()) {
        failure->fetch_add(ops_per_thread, std::memory_order_relaxed);
        std::cerr << "[worker-" << worker_id << "] connect failed: " << conn.error().message() << '\n';
        finished->fetch_add(1, std::memory_order_release);
        co_return;
    }

    for (int i = 0; i < ops_per_thread; ++i) {
        const std::string key = key_prefix + std::to_string(worker_id) + "/" + std::to_string(i);
        const auto begin = std::chrono::steady_clock::now();

        bool ok = false;
        if (mixed) {
            auto put = co_await session.put(key, value);
            if (put.has_value()) {
                auto get = co_await session.get(key);
                ok = get.has_value() && !session.lastKeyValues().empty();
            }
        } else {
            auto put = co_await session.put(key, value);
            ok = put.has_value();
        }

        const auto end = std::chrono::steady_clock::now();
        const int64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        (*latency_by_worker)[static_cast<size_t>(worker_id)].push_back(us);

        if (ok) {
            success->fetch_add(1, std::memory_order_relaxed);
        } else {
            failure->fetch_add(1, std::memory_order_relaxed);
        }
    }

    (void)co_await session.close();
    finished->fetch_add(1, std::memory_order_release);
}

} // namespace

int main(int argc, char** argv)
{
    const Args args = parseArgs(argc, argv);
    const std::string value = payloadOfSize(args.value_size);
    const std::string key_prefix = "/galay-etcd/bench/" +
        std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count()) + "/";

    std::atomic<int64_t> success{0};
    std::atomic<int64_t> failure{0};
    std::atomic<int> finished{0};
    std::vector<std::vector<int64_t>> latency_by_worker(static_cast<size_t>(args.threads));
    for (auto& latencies : latency_by_worker) {
        latencies.reserve(static_cast<size_t>(args.ops_per_thread));
    }

    Runtime runtime(static_cast<size_t>(args.threads), 1);
    runtime.start();

    const auto benchmark_begin = std::chrono::steady_clock::now();
    for (int worker = 0; worker < args.threads; ++worker) {
        auto* scheduler = runtime.getNextIOScheduler();
        if (scheduler == nullptr) {
            std::cerr << "No IO scheduler available for worker " << worker << '\n';
            failure.fetch_add(args.ops_per_thread, std::memory_order_relaxed);
            finished.fetch_add(1, std::memory_order_release);
            continue;
        }

        scheduler->spawn(runWorker(
            scheduler,
            args.endpoint,
            key_prefix,
            value,
            worker,
            args.ops_per_thread,
            args.mixed,
            &latency_by_worker,
            &success,
            &failure,
            &finished));
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(20);
    while (finished.load(std::memory_order_acquire) < args.threads &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    const bool timeout = finished.load(std::memory_order_acquire) < args.threads;
    const auto benchmark_end = std::chrono::steady_clock::now();
    runtime.stop();

    if (timeout) {
        std::cerr << "Benchmark timeout: finished workers="
                  << finished.load(std::memory_order_relaxed)
                  << ", expected=" << args.threads << '\n';
    }

    const double seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(benchmark_end - benchmark_begin).count();

    std::vector<int64_t> all_latency;
    all_latency.reserve(static_cast<size_t>(args.threads * args.ops_per_thread));
    for (const auto& latencies : latency_by_worker) {
        all_latency.insert(all_latency.end(), latencies.begin(), latencies.end());
    }

    const int64_t ok_count = success.load(std::memory_order_relaxed);
    const int64_t fail_count = failure.load(std::memory_order_relaxed);
    const int64_t total = ok_count + fail_count;
    const double qps = seconds > 0 ? static_cast<double>(ok_count) / seconds : 0.0;

    std::cout << "Endpoint      : " << args.endpoint << '\n';
    std::cout << "Mode          : " << (args.mixed ? "mixed(put+get)" : "put") << '\n';
    std::cout << "Workers       : " << args.threads << '\n';
    std::cout << "Ops/worker    : " << args.ops_per_thread << '\n';
    std::cout << "Value size    : " << args.value_size << " bytes\n";
    std::cout << "Total ops     : " << total << '\n';
    std::cout << "Success       : " << ok_count << '\n';
    std::cout << "Failure       : " << fail_count << '\n';
    std::cout << "Duration      : " << seconds << " s\n";
    std::cout << "Throughput    : " << qps << " ops/s\n";
    std::cout << "Latency p50   : " << percentile(all_latency, 0.50) << " us\n";
    std::cout << "Latency p95   : " << percentile(all_latency, 0.95) << " us\n";
    std::cout << "Latency p99   : " << percentile(all_latency, 0.99) << " us\n";
    std::cout << "Latency max   : "
              << (all_latency.empty() ? 0 : *std::max_element(all_latency.begin(), all_latency.end()))
              << " us\n";

    if (timeout) {
        return 3;
    }
    return fail_count == 0 ? 0 : 2;
}
