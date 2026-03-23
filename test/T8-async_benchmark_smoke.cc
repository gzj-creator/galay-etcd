#include "benchmark/AsyncBenchmarkSupport.h"

#include <iostream>

int main()
{
    galay::etcd::benchmark::AsyncBenchmarkArgs args;
    args.workers = 1;
    args.ops_per_worker = 1;
    args.value_size = 16;
    args.io_schedulers = 1;
    args.mode = galay::etcd::benchmark::AsyncBenchmarkMode::Put;

    auto result = galay::etcd::benchmark::runAsyncBenchmark(args);
    if (!result.has_value()) {
        std::cerr << "async benchmark run failed: " << result.error() << '\n';
        return 1;
    }

    if (result->success != 1) {
        std::cerr << "expected success == 1, got " << result->success << '\n';
        return 1;
    }
    if (result->failure != 0) {
        std::cerr << "expected failure == 0, got " << result->failure << '\n';
        return 1;
    }
    if (result->total_ops != 1) {
        std::cerr << "expected total_ops == 1, got " << result->total_ops << '\n';
        return 1;
    }
    if (result->latency_us.size() != 1) {
        std::cerr << "expected exactly one latency sample, got " << result->latency_us.size() << '\n';
        return 1;
    }
    if (!(result->throughput > 0.0)) {
        std::cerr << "expected throughput > 0, got " << result->throughput << '\n';
        return 1;
    }

    std::cout << "ASYNC BENCHMARK SMOKE TEST PASSED\n";
    return 0;
}
