#include "galay-etcd/sync/EtcdClient.h"

#include <galay-kernel/kernel/Runtime.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

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

Coroutine runSmoke(IOScheduler* scheduler,
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
    config.api_prefix = "/v3";

    EtcdClient session(scheduler, config);

    auto conn = co_await session.connect();
    if (!conn.has_value()) {
        finish(fail("connect failed: " + conn.error().message()));
        co_return;
    }
    std::cout << "[OK] connect" << std::endl;

    const std::string key = "/galay-etcd/smoke/" + nowSuffix();
    const std::string value = "v-" + nowSuffix();

    auto put = co_await session.put(key, value);
    if (!put.has_value()) {
        finish(fail("put failed: " + put.error().message()));
        co_return;
    }
    std::cout << "[OK] put"
              << " status=" << session.lastStatusCode()
              << " body=" << session.lastResponseBody() << std::endl;

    auto get = co_await session.get(key);
    if (!get.has_value()) {
        finish(fail("get failed: " + get.error().message() +
                    ", status=" + std::to_string(session.lastStatusCode()) +
                    ", body=" + session.lastResponseBody()));
        co_return;
    }

    const auto& kvs = session.lastKeyValues();
    if (kvs.empty() || kvs.front().value != value) {
        const std::string actual = kvs.empty() ? "<empty>" : kvs.front().value;
        finish(fail("get value mismatch, key=" + key + ", expected=" + value +
                    ", actual=" + actual +
                    ", status=" + std::to_string(session.lastStatusCode()) +
                    ", body=" + session.lastResponseBody()));
        co_return;
    }
    std::cout << "[OK] get" << std::endl;

    auto deleted = co_await session.del(key);
    if (!deleted.has_value()) {
        finish(fail("delete failed: " + deleted.error().message() +
                    ", status=" + std::to_string(session.lastStatusCode()) +
                    ", body=" + session.lastResponseBody()));
        co_return;
    }
    if (session.lastDeletedCount() <= 0) {
        finish(fail("delete count should be > 0, status=" +
                    std::to_string(session.lastStatusCode()) +
                    ", body=" + session.lastResponseBody()));
        co_return;
    }
    std::cout << "[OK] delete" << std::endl;

    auto lease = co_await session.grantLease(3);
    if (!lease.has_value()) {
        finish(fail("grant lease failed: " + lease.error().message()));
        co_return;
    }
    if (session.lastLeaseId() <= 0) {
        finish(fail("lease id should be > 0"));
        co_return;
    }
    std::cout << "[OK] grant lease id=" << session.lastLeaseId() << std::endl;

    const std::string lease_key = key + "/lease";
    std::cout << "[RUN] put with lease" << std::endl;
    auto put_lease = co_await session.put(lease_key, value, session.lastLeaseId());
    if (!put_lease.has_value()) {
        finish(fail("put with lease failed: " + put_lease.error().message()));
        co_return;
    }

    auto get_lease = co_await session.get(lease_key);
    if (!get_lease.has_value()) {
        finish(fail("get leased key failed: " + get_lease.error().message()));
        co_return;
    }
    if (session.lastKeyValues().empty()) {
        finish(fail("leased key should exist immediately after keepalive"));
        co_return;
    }
    std::cout << "[OK] leased key exists" << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(5));
    auto get_after_ttl = co_await session.get(lease_key);
    if (!get_after_ttl.has_value()) {
        finish(fail("get after ttl failed: " + get_after_ttl.error().message()));
        co_return;
    }
    if (!session.lastKeyValues().empty()) {
        finish(fail("leased key should expire after ttl"));
        co_return;
    }
    std::cout << "[OK] lease expiration" << std::endl;

    auto close = co_await session.close();
    if (!close.has_value()) {
        finish(fail("close failed: " + close.error().message()));
        co_return;
    }
    std::cout << "[OK] close" << std::endl;

    std::cout << "SMOKE TEST PASSED" << std::endl;
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
    scheduler->spawn(runSmoke(scheduler, endpoint, &done, &exit_code));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    while (!done.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (!done.load(std::memory_order_acquire)) {
        exit_code = fail("smoke test timeout");
    }

    runtime.stop();
    return exit_code;
}
