/**
 * @file T7-etcd_awaitable_surface.cc
 * @brief 用途：锁定 AsyncEtcdClient 公开 header 不再泄露旧 awaitable 辅助类型。
 * 关键覆盖点：`ConnectIoAwaitable` / `CloseIoAwaitable` / `HttpPostAwaitable` /
 * `IoAwaitableBase` / `JsonOpAwaitableBase` 不再作为公开 surface 暴露。
 * 通过条件：目标成功编译，静态断言成立，程序返回 0。
 */

#include "galay-etcd/async/AsyncEtcdClient.h"

using galay::etcd::AsyncEtcdClient;

namespace {

template <typename ClientT>
concept HasConnectIoAwaitable = requires {
    typename ClientT::ConnectIoAwaitable;
};

template <typename ClientT>
concept HasCloseIoAwaitable = requires {
    typename ClientT::CloseIoAwaitable;
};

template <typename ClientT>
concept HasHttpPostAwaitable = requires {
    typename ClientT::HttpPostAwaitable;
};

template <typename ClientT>
concept HasIoAwaitableBase = requires {
    typename ClientT::template IoAwaitableBase<int>;
};

template <typename ClientT>
concept HasJsonOpAwaitableBase = requires {
    typename ClientT::JsonOpAwaitableBase;
};

}  // namespace

static_assert(!HasConnectIoAwaitable<AsyncEtcdClient>);
static_assert(!HasCloseIoAwaitable<AsyncEtcdClient>);
static_assert(!HasHttpPostAwaitable<AsyncEtcdClient>);
static_assert(!HasIoAwaitableBase<AsyncEtcdClient>);
static_assert(!HasJsonOpAwaitableBase<AsyncEtcdClient>);

int main()
{
    return 0;
}
