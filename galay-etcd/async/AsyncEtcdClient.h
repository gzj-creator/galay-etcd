#ifndef GALAY_ETCD_ASYNC_ETCD_CLIENT_H
#define GALAY_ETCD_ASYNC_ETCD_CLIENT_H

#include "galay-etcd/async/AsyncEtcdConfig.h"
#include "galay-etcd/base/EtcdConfig.h"
#include "galay-etcd/base/EtcdError.h"
#include "galay-etcd/base/EtcdValue.h"

#include <galay-http/kernel/http/HttpSession.h>
#include <galay-kernel/async/TcpSocket.h>
#include <galay-kernel/common/Host.hpp>
#include <galay-kernel/kernel/IOScheduler.hpp>

#include <chrono>
#include <cstdint>
#include <coroutine>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace galay::etcd
{

using EtcdVoidResult = std::expected<void, EtcdError>;
using EtcdGetResult = std::expected<std::vector<EtcdKeyValue>, EtcdError>;
using EtcdDeleteResult = std::expected<int64_t, EtcdError>;
using EtcdLeaseGrantResult = std::expected<int64_t, EtcdError>;

class AsyncEtcdClient
{
public:
    enum class PipelineOpType
    {
        Put,
        Get,
        Delete,
    };

    struct PipelineOp
    {
        PipelineOpType type = PipelineOpType::Put;
        std::string key;
        std::string value;
        bool prefix = false;
        std::optional<int64_t> limit = std::nullopt;
        std::optional<int64_t> lease_id = std::nullopt;

        static PipelineOp Put(std::string key,
                              std::string value,
                              std::optional<int64_t> lease_id = std::nullopt)
        {
            PipelineOp op;
            op.type = PipelineOpType::Put;
            op.key = std::move(key);
            op.value = std::move(value);
            op.lease_id = lease_id;
            return op;
        }

        static PipelineOp Get(std::string key,
                              bool prefix = false,
                              std::optional<int64_t> limit = std::nullopt)
        {
            PipelineOp op;
            op.type = PipelineOpType::Get;
            op.key = std::move(key);
            op.prefix = prefix;
            op.limit = limit;
            return op;
        }

        static PipelineOp Del(std::string key, bool prefix = false)
        {
            PipelineOp op;
            op.type = PipelineOpType::Delete;
            op.key = std::move(key);
            op.prefix = prefix;
            return op;
        }
    };

    struct PipelineItemResult
    {
        PipelineOpType type = PipelineOpType::Put;
        bool ok = false;
        int64_t deleted_count = 0;
        std::vector<EtcdKeyValue> kvs;
    };

    using ConnectIoAwaitable =
        decltype(std::declval<galay::async::TcpSocket&>().connect(std::declval<const galay::kernel::Host&>()));
    using CloseIoAwaitable = decltype(std::declval<galay::async::TcpSocket&>().close());

    template <typename AwaitableType>
    class IoAwaitableBase
    {
    protected:
        explicit IoAwaitableBase(AsyncEtcdClient& client)
            : m_client(&client)
        {
        }

        void startIo(AwaitableType&& awaitable)
        {
            m_awaitable.emplace(std::move(awaitable));
        }

        bool awaitReady() const noexcept
        {
            return !m_awaitable.has_value();
        }

        bool awaitSuspend(std::coroutine_handle<> handle)
        {
            return m_awaitable->await_suspend(handle);
        }

        std::optional<AwaitableType>& awaitable()
        {
            return m_awaitable;
        }

        const std::optional<AwaitableType>& awaitable() const
        {
            return m_awaitable;
        }

        AsyncEtcdClient* m_client = nullptr;

    private:
        std::optional<AwaitableType> m_awaitable;
    };

    class ConnectAwaitable : private IoAwaitableBase<ConnectIoAwaitable>
    {
    public:
        ConnectAwaitable(AsyncEtcdClient& client);

        ConnectAwaitable(const ConnectAwaitable&) = delete;
        ConnectAwaitable& operator=(const ConnectAwaitable&) = delete;
        ConnectAwaitable(ConnectAwaitable&&) noexcept = default;
        ConnectAwaitable& operator=(ConnectAwaitable&&) noexcept = default;

        bool await_ready() const noexcept;
        bool await_suspend(std::coroutine_handle<> handle);
        EtcdVoidResult await_resume();
    };

    class CloseAwaitable : private IoAwaitableBase<CloseIoAwaitable>
    {
    public:
        CloseAwaitable(AsyncEtcdClient& client);

        CloseAwaitable(const CloseAwaitable&) = delete;
        CloseAwaitable& operator=(const CloseAwaitable&) = delete;
        CloseAwaitable(CloseAwaitable&&) noexcept = default;
        CloseAwaitable& operator=(CloseAwaitable&&) noexcept = default;

        bool await_ready() const noexcept;
        bool await_suspend(std::coroutine_handle<> handle);
        EtcdVoidResult await_resume();
    };

    class PostJsonAwaitable
    {
    public:
        PostJsonAwaitable(AsyncEtcdClient& client,
                          std::string api_path,
                          std::string body,
                          std::optional<std::chrono::milliseconds> force_timeout);

        PostJsonAwaitable(const PostJsonAwaitable&) = delete;
        PostJsonAwaitable& operator=(const PostJsonAwaitable&) = delete;
        PostJsonAwaitable(PostJsonAwaitable&&) noexcept = default;
        PostJsonAwaitable& operator=(PostJsonAwaitable&&) noexcept = default;
        ~PostJsonAwaitable();

        bool await_ready() const noexcept;
        bool await_suspend(std::coroutine_handle<> handle);
        EtcdVoidResult await_resume();

    private:
        struct Context;
        std::unique_ptr<Context> m_ctx;
    };

    class JsonOpAwaitableBase
    {
    protected:
        explicit JsonOpAwaitableBase(AsyncEtcdClient& client);

        void startPost(std::string api_path,
                       std::string body,
                       std::optional<std::chrono::milliseconds> force_timeout = std::nullopt);
        bool awaitReady() const noexcept;
        bool awaitSuspend(std::coroutine_handle<> handle);
        EtcdVoidResult resumePost();

        AsyncEtcdClient* m_client = nullptr;
        std::optional<PostJsonAwaitable> m_post_awaitable;
    };

    class PutAwaitable : private JsonOpAwaitableBase
    {
    public:
        PutAwaitable(AsyncEtcdClient& client,
                     std::string key,
                     std::string value,
                     std::optional<int64_t> lease_id);

        PutAwaitable(const PutAwaitable&) = delete;
        PutAwaitable& operator=(const PutAwaitable&) = delete;
        PutAwaitable(PutAwaitable&&) noexcept = default;
        PutAwaitable& operator=(PutAwaitable&&) noexcept = default;

        bool await_ready() const noexcept;
        bool await_suspend(std::coroutine_handle<> handle);
        EtcdVoidResult await_resume();
    };

    class GetAwaitable : private JsonOpAwaitableBase
    {
    public:
        GetAwaitable(AsyncEtcdClient& client,
                     std::string key,
                     bool prefix,
                     std::optional<int64_t> limit);

        GetAwaitable(const GetAwaitable&) = delete;
        GetAwaitable& operator=(const GetAwaitable&) = delete;
        GetAwaitable(GetAwaitable&&) noexcept = default;
        GetAwaitable& operator=(GetAwaitable&&) noexcept = default;

        bool await_ready() const noexcept;
        bool await_suspend(std::coroutine_handle<> handle);
        EtcdVoidResult await_resume();
    };

    class DeleteAwaitable : private JsonOpAwaitableBase
    {
    public:
        DeleteAwaitable(AsyncEtcdClient& client,
                        std::string key,
                        bool prefix);

        DeleteAwaitable(const DeleteAwaitable&) = delete;
        DeleteAwaitable& operator=(const DeleteAwaitable&) = delete;
        DeleteAwaitable(DeleteAwaitable&&) noexcept = default;
        DeleteAwaitable& operator=(DeleteAwaitable&&) noexcept = default;

        bool await_ready() const noexcept;
        bool await_suspend(std::coroutine_handle<> handle);
        EtcdVoidResult await_resume();
    };

    class GrantLeaseAwaitable : private JsonOpAwaitableBase
    {
    public:
        GrantLeaseAwaitable(AsyncEtcdClient& client, int64_t ttl_seconds);

        GrantLeaseAwaitable(const GrantLeaseAwaitable&) = delete;
        GrantLeaseAwaitable& operator=(const GrantLeaseAwaitable&) = delete;
        GrantLeaseAwaitable(GrantLeaseAwaitable&&) noexcept = default;
        GrantLeaseAwaitable& operator=(GrantLeaseAwaitable&&) noexcept = default;

        bool await_ready() const noexcept;
        bool await_suspend(std::coroutine_handle<> handle);
        EtcdVoidResult await_resume();
    };

    class KeepAliveAwaitable : private JsonOpAwaitableBase
    {
    public:
        KeepAliveAwaitable(AsyncEtcdClient& client, int64_t lease_id);

        KeepAliveAwaitable(const KeepAliveAwaitable&) = delete;
        KeepAliveAwaitable& operator=(const KeepAliveAwaitable&) = delete;
        KeepAliveAwaitable(KeepAliveAwaitable&&) noexcept = default;
        KeepAliveAwaitable& operator=(KeepAliveAwaitable&&) noexcept = default;

        bool await_ready() const noexcept;
        bool await_suspend(std::coroutine_handle<> handle);
        EtcdVoidResult await_resume();

    private:
        int64_t m_lease_id = 0;
    };

    class PipelineAwaitable : private JsonOpAwaitableBase
    {
    public:
        PipelineAwaitable(AsyncEtcdClient& client, std::vector<PipelineOp> operations);

        PipelineAwaitable(const PipelineAwaitable&) = delete;
        PipelineAwaitable& operator=(const PipelineAwaitable&) = delete;
        PipelineAwaitable(PipelineAwaitable&&) noexcept = default;
        PipelineAwaitable& operator=(PipelineAwaitable&&) noexcept = default;

        bool await_ready() const noexcept;
        bool await_suspend(std::coroutine_handle<> handle);
        EtcdVoidResult await_resume();

    private:
        std::vector<PipelineOp> m_operations;
    };

    AsyncEtcdClient(galay::kernel::IOScheduler* scheduler,
               EtcdConfig config = {},
               AsyncEtcdConfig async_config = {});

    ConnectAwaitable connect();
    CloseAwaitable close();

    PutAwaitable put(const std::string& key,
                     const std::string& value,
                     std::optional<int64_t> lease_id = std::nullopt);

    GetAwaitable get(const std::string& key,
                     bool prefix = false,
                     std::optional<int64_t> limit = std::nullopt);

    DeleteAwaitable del(const std::string& key, bool prefix = false);
    GrantLeaseAwaitable grantLease(int64_t ttl_seconds);
    KeepAliveAwaitable keepAliveOnce(int64_t lease_id);
    PipelineAwaitable pipeline(std::vector<PipelineOp> operations);

    [[nodiscard]] bool connected() const;
    [[nodiscard]] EtcdError lastError() const;
    [[nodiscard]] bool lastBool() const;
    [[nodiscard]] int64_t lastLeaseId() const;
    [[nodiscard]] int64_t lastDeletedCount() const;
    [[nodiscard]] const std::vector<EtcdKeyValue>& lastKeyValues() const;
    [[nodiscard]] const std::vector<PipelineItemResult>& lastPipelineResults() const;
    [[nodiscard]] int lastStatusCode() const;
    [[nodiscard]] const std::string& lastResponseBody() const;

private:
    void resetLastOperation();
    void setError(EtcdErrorType type, const std::string& message);
    void setError(EtcdError error);

    [[nodiscard]] EtcdVoidResult currentResult() const;
    EtcdVoidResult resumePostOrCurrent(std::optional<PostJsonAwaitable>& post_awaitable);

    PostJsonAwaitable postJsonInternal(const std::string& api_path,
                                       const std::string& body,
                                       std::optional<std::chrono::milliseconds> force_timeout = std::nullopt);

private:
    galay::kernel::IOScheduler* m_scheduler;
    EtcdConfig m_config;
    AsyncEtcdConfig m_async_config;
    std::string m_api_prefix;
    std::string m_host_header;
    galay::kernel::IPType m_ip_type = galay::kernel::IPType::IPV4;
    std::optional<galay::kernel::Host> m_server_host;
    std::string m_endpoint_error;
    bool m_endpoint_valid = false;

    std::unique_ptr<galay::async::TcpSocket> m_socket;
    std::unique_ptr<galay::http::HttpSession> m_http_session;
    bool m_connected = false;

    EtcdError m_last_error;
    bool m_last_bool = false;
    int64_t m_last_lease_id = 0;
    int64_t m_last_deleted_count = 0;
    int m_last_status_code = 0;
    std::string m_last_response_body;
    std::vector<EtcdKeyValue> m_last_kvs;
    std::vector<PipelineItemResult> m_last_pipeline_results;
};

} // namespace galay::etcd

#endif // GALAY_ETCD_ASYNC_ETCD_CLIENT_H
