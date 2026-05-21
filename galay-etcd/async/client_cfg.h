/**
 * @file client_cfg.h
 * @brief etcd 异步客户端配置结构定义
 * @author galay-etcd
 * @version 1.0.0
 *
 * @details 定义异步 etcd 客户端(AsyncEtcdClient)使用的配置结构，
 *          继承自同步配置(EtcdConfig)，当前阶段保持相同的配置项。
 *          用于 AsyncEtcdClient 和 AsyncEtcdClientBuilder。
 */

#ifndef GALAY_ETCD_ASYNC_CONFIG_H
#define GALAY_ETCD_ASYNC_CONFIG_H

#include "galay-etcd/base/etcd_config.h"

namespace galay::etcd
{

/**
 * @brief etcd 异步客户端配置
 * @details 继承 EtcdConfig 的全部配置项，
 *          用于 AsyncEtcdClient 的构建。
 */
struct AsyncEtcdConfig : EtcdConfig
{
    /**
     * @brief 创建一个指定超时时间的异步配置
     * @param timeout 请求超时时间
     * @return 配置好超时时间的 AsyncEtcdConfig 实例
     */
    static AsyncEtcdConfig withTimeout(std::chrono::milliseconds timeout)
    {
        AsyncEtcdConfig cfg;
        cfg.request_timeout = timeout;
        return cfg;
    }
};

} // namespace galay::etcd

#endif // GALAY_ETCD_ASYNC_CONFIG_H
