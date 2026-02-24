#include "galay-etcd/sync/EtcdClient.h"

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

using galay::etcd::EtcdConfig;
using galay::etcd::EtcdClient;

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

} // namespace

int main(int argc, char** argv)
{
    const std::string endpoint = argc > 1 ? argv[1] : "http://140.143.142.251:2379";

    EtcdConfig config;
    config.endpoint = endpoint;
    config.api_prefix = "/v3";

    EtcdClient session(config);

    auto conn = session.connect();
    if (!conn.has_value()) {
        return fail("connect failed: " + conn.error().message());
    }
    std::cout << "[OK] connect" << std::endl;

    const std::string key = "/galay-etcd/smoke/" + nowSuffix();
    const std::string value = "v-" + nowSuffix();

    auto put = session.put(key, value);
    if (!put.has_value()) {
        return fail("put failed: " + put.error().message());
    }
    std::cout << "[OK] put"
              << " status=" << session.lastStatusCode()
              << " body=" << session.lastResponseBody() << std::endl;

    auto get = session.get(key);
    if (!get.has_value()) {
        return fail("get failed: " + get.error().message() +
                    ", status=" + std::to_string(session.lastStatusCode()) +
                    ", body=" + session.lastResponseBody());
    }

    const auto& kvs = session.lastKeyValues();
    if (kvs.empty() || kvs.front().value != value) {
        const std::string actual = kvs.empty() ? "<empty>" : kvs.front().value;
        return fail("get value mismatch, key=" + key + ", expected=" + value +
                    ", actual=" + actual +
                    ", status=" + std::to_string(session.lastStatusCode()) +
                    ", body=" + session.lastResponseBody());
    }
    std::cout << "[OK] get" << std::endl;

    auto deleted = session.del(key);
    if (!deleted.has_value()) {
        return fail("delete failed: " + deleted.error().message() +
                    ", status=" + std::to_string(session.lastStatusCode()) +
                    ", body=" + session.lastResponseBody());
    }
    if (session.lastDeletedCount() <= 0) {
        return fail("delete count should be > 0, status=" +
                    std::to_string(session.lastStatusCode()) +
                    ", body=" + session.lastResponseBody());
    }
    std::cout << "[OK] delete" << std::endl;

    auto lease = session.grantLease(3);
    if (!lease.has_value()) {
        return fail("grant lease failed: " + lease.error().message());
    }
    if (session.lastLeaseId() <= 0) {
        return fail("lease id should be > 0");
    }
    std::cout << "[OK] grant lease id=" << session.lastLeaseId() << std::endl;

    const std::string lease_key = key + "/lease";
    std::cout << "[RUN] put with lease" << std::endl;
    auto put_lease = session.put(lease_key, value, session.lastLeaseId());
    if (!put_lease.has_value()) {
        return fail("put with lease failed: " + put_lease.error().message());
    }

    auto get_lease = session.get(lease_key);
    if (!get_lease.has_value()) {
        return fail("get leased key failed: " + get_lease.error().message());
    }
    if (session.lastKeyValues().empty()) {
        return fail("leased key should exist immediately after keepalive");
    }
    std::cout << "[OK] leased key exists" << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(5));
    auto get_after_ttl = session.get(lease_key);
    if (!get_after_ttl.has_value()) {
        return fail("get after ttl failed: " + get_after_ttl.error().message());
    }
    if (!session.lastKeyValues().empty()) {
        return fail("leased key should expire after ttl");
    }
    std::cout << "[OK] lease expiration" << std::endl;

    auto close = session.close();
    if (!close.has_value()) {
        return fail("close failed: " + close.error().message());
    }
    std::cout << "[OK] close" << std::endl;

    std::cout << "SMOKE TEST PASSED" << std::endl;
    return 0;
}
