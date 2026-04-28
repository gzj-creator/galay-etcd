#ifndef GALAY_ETCD_TYPES_H
#define GALAY_ETCD_TYPES_H

#include "galay-etcd/base/etcd_value.h"

#include <cstdint>
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

} // namespace galay::etcd

#endif // GALAY_ETCD_TYPES_H
