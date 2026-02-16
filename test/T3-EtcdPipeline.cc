#include "galay-etcd/sync/EtcdClient.h"

#include <galay-kernel/kernel/Runtime.h>

#include <atomic>
#include <chrono>
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

std::string nowSuffix()
{
    const auto now = std::chrono::high_resolution_clock::now().time_since_epoch();
    return std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(now).count());
}

int fail(const std::string& message)
{
    std::cerr << "[FAIL] " << message << '\n';
    return 1;
}

Coroutine runPipelineCase(IOScheduler* scheduler,
                          std::string endpoint,
                          std::atomic<bool>* done,
                          int* exit_code)
{
    auto finish = [&](int code) {
        *exit_code = code;
        done->store(true, std::memory_order_release);
    };

    EtcdConfig config;
    config.endpoint = endpoint;

    EtcdClient session(scheduler, config);
    auto conn = co_await session.connect();
    if (!conn.has_value()) {
        finish(fail("connect failed: " + conn.error().message()));
        co_return;
    }

    const std::string base = "/galay-etcd/pipeline/" + nowSuffix();
    const std::string k1 = base + "/k1";
    const std::string k2 = base + "/k2";
    const std::string v1 = "seed";
    const std::string v2 = "pipeline-v2";

    auto seed = co_await session.put(k1, v1);
    if (!seed.has_value()) {
        finish(fail("seed put failed: " + seed.error().message()));
        co_return;
    }

    std::vector<EtcdClient::PipelineOp> ops;
    ops.push_back(EtcdClient::PipelineOp::Get(k1));
    ops.push_back(EtcdClient::PipelineOp::Put(k2, v2));
    ops.push_back(EtcdClient::PipelineOp::Del(k1));

    auto pipeline = co_await session.pipeline(std::move(ops));
    if (!pipeline.has_value()) {
        finish(fail("pipeline failed: " + pipeline.error().message()));
        co_return;
    }

    const auto& results = session.lastPipelineResults();
    if (results.size() != 3) {
        finish(fail("pipeline result size mismatch, expect=3, actual=" + std::to_string(results.size())));
        co_return;
    }

    if (results[0].kvs.empty() || results[0].kvs.front().value != v1) {
        finish(fail("pipeline get result mismatch"));
        co_return;
    }
    if (!results[1].ok) {
        finish(fail("pipeline put result not ok"));
        co_return;
    }
    if (results[2].deleted_count <= 0) {
        finish(fail("pipeline del result should delete at least one key"));
        co_return;
    }
    std::cout << "[OK] pipeline responses parsed\n";

    auto verify_k1 = co_await session.get(k1);
    if (!verify_k1.has_value()) {
        finish(fail("verify k1 failed: " + verify_k1.error().message()));
        co_return;
    }
    if (!session.lastKeyValues().empty()) {
        finish(fail("k1 should be deleted by pipeline"));
        co_return;
    }

    auto verify_k2 = co_await session.get(k2);
    if (!verify_k2.has_value()) {
        finish(fail("verify k2 failed: " + verify_k2.error().message()));
        co_return;
    }
    if (session.lastKeyValues().empty() || session.lastKeyValues().front().value != v2) {
        finish(fail("k2 should be written by pipeline"));
        co_return;
    }

    (void)co_await session.del(k2);

    auto close = co_await session.close();
    if (!close.has_value()) {
        finish(fail("close failed: " + close.error().message()));
        co_return;
    }

    std::cout << "PIPELINE TEST PASSED\n";
    finish(0);
}

} // namespace

int main(int argc, char** argv)
{
    const std::string endpoint = argc > 1 ? argv[1] : "http://140.143.142.251:2379";

    Runtime runtime(1, 1);
    runtime.start();

    auto* scheduler = runtime.getNextIOScheduler();
    if (scheduler == nullptr) {
        runtime.stop();
        return fail("failed to get io scheduler");
    }

    std::atomic<bool> done{false};
    int exit_code = 1;
    scheduler->spawn(runPipelineCase(scheduler, endpoint, &done, &exit_code));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    while (!done.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (!done.load(std::memory_order_acquire)) {
        exit_code = fail("pipeline test timeout");
    }

    runtime.stop();
    return exit_code;
}

