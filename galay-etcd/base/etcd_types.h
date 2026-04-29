#ifndef GALAY_ETCD_TYPES_H
#define GALAY_ETCD_TYPES_H

#include "galay-etcd/base/etcd_error.h"
#include "galay-etcd/base/etcd_value.h"

#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace galay::etcd
{

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

enum class EtcdWatchEventType
{
    Put,
    Delete,
    Unknown,
};

/**
 * @brief 单条 watch 事件。
 * @note `prev_kv` 只有服务端返回时才有值。
 */
struct EtcdWatchEvent
{
    EtcdWatchEventType type = EtcdWatchEventType::Unknown;
    EtcdKeyValue kv;
    std::optional<EtcdKeyValue> prev_kv = std::nullopt;
};

/**
 * @brief 一次 watch 回调批次的结果。
 * @note etcd 可能先返回 `created=true` 且 `events` 为空的创建确认，再返回实际事件批次。
 */
struct EtcdWatchResponse
{
    int64_t watch_id = 0;
    bool created = false;
    bool canceled = false;
    int64_t compact_revision = 0;
    std::vector<EtcdWatchEvent> events;
};

using EtcdVoidResult = std::expected<void, EtcdError>;
using EtcdBoolResult = std::expected<bool, EtcdError>;
using EtcdGetResult = std::expected<std::vector<EtcdKeyValue>, EtcdError>;
using EtcdDeleteResult = std::expected<int64_t, EtcdError>;
using EtcdLeaseGrantResult = std::expected<int64_t, EtcdError>;
using EtcdPipelineResult = std::expected<std::vector<PipelineItemResult>, EtcdError>;

} // namespace galay::etcd

#endif // GALAY_ETCD_TYPES_H
