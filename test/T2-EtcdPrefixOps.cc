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

Coroutine runPrefixCase(IOScheduler* scheduler,
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

    const std::string prefix = "/galay-etcd/prefix/" + nowSuffix() + "/";
    const std::vector<std::pair<std::string, std::string>> kvs = {
        {prefix + "a", "1"},
        {prefix + "b", "2"},
        {prefix + "c", "3"},
    };

    for (const auto& [key, value] : kvs) {
        auto put = co_await session.put(key, value);
        if (!put.has_value()) {
            finish(fail("put failed: " + key + " error=" + put.error().message()));
            co_return;
        }
    }

    auto range = co_await session.get(prefix, true);
    if (!range.has_value()) {
        finish(fail("prefix get failed: " + range.error().message()));
        co_return;
    }
    if (session.lastKeyValues().size() < kvs.size()) {
        finish(fail("prefix get size mismatch"));
        co_return;
    }
    std::cout << "[OK] prefix get count=" << session.lastKeyValues().size() << '\n';

    auto deleted = co_await session.del(prefix, true);
    if (!deleted.has_value()) {
        finish(fail("prefix delete failed: " + deleted.error().message()));
        co_return;
    }
    if (session.lastDeletedCount() < static_cast<int64_t>(kvs.size())) {
        finish(fail("prefix delete count mismatch"));
        co_return;
    }
    std::cout << "[OK] prefix delete count=" << session.lastDeletedCount() << '\n';

    auto range_after = co_await session.get(prefix, true);
    if (!range_after.has_value()) {
        finish(fail("prefix get after delete failed: " + range_after.error().message()));
        co_return;
    }
    if (!session.lastKeyValues().empty()) {
        finish(fail("prefix keys should be empty after delete"));
        co_return;
    }

    auto close = co_await session.close();
    if (!close.has_value()) {
        finish(fail("close failed: " + close.error().message()));
        co_return;
    }

    std::cout << "PREFIX TEST PASSED\n";
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
    scheduler->spawn(runPrefixCase(scheduler, endpoint, &done, &exit_code));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    while (!done.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (!done.load(std::memory_order_acquire)) {
        exit_code = fail("prefix test timeout");
    }

    runtime.stop();
    return exit_code;
}
