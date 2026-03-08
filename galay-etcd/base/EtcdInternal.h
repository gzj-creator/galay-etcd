#ifndef GALAY_ETCD_INTERNAL_H
#define GALAY_ETCD_INTERNAL_H

#include "galay-etcd/base/EtcdError.h"
#include "galay-etcd/base/EtcdTypes.h"
#include "galay-etcd/base/EtcdValue.h"

#include <galay-utils/algorithm/Base64.hpp>
#include <simdjson.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <optional>
#include <regex>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace galay::etcd::internal
{

inline std::optional<int64_t> parseSignedInt(std::string_view value)
{
    if (value.empty()) {
        return std::nullopt;
    }
    int64_t parsed = 0;
    const char* begin = value.data();
    const char* end = value.data() + value.size();
    auto [ptr, ec] = std::from_chars(begin, end, parsed);
    if (ec != std::errc() || ptr != end) {
        return std::nullopt;
    }
    return parsed;
}

inline std::optional<int64_t> asInt64(const simdjson::dom::element& element)
{
    auto int64_result = element.get_int64();
    if (!int64_result.error()) {
        return int64_result.value_unsafe();
    }

    auto uint64_result = element.get_uint64();
    if (!uint64_result.error()) {
        const uint64_t value = uint64_result.value_unsafe();
        if (value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return static_cast<int64_t>(value);
        }
    }

    auto string_result = element.get_string();
    if (!string_result.error()) {
        return parseSignedInt(string_result.value_unsafe());
    }
    return std::nullopt;
}

inline std::optional<int64_t> findIntField(const simdjson::dom::object& object, std::string_view field)
{
    auto field_result = object[field];
    if (field_result.error()) {
        return std::nullopt;
    }
    return asInt64(field_result.value_unsafe());
}

inline std::optional<std::string> findStringField(const simdjson::dom::object& object, std::string_view field)
{
    auto field_result = object[field];
    if (field_result.error()) {
        return std::nullopt;
    }

    auto string_result = field_result.value_unsafe().get_string();
    if (string_result.error()) {
        return std::nullopt;
    }
    return std::string(string_result.value_unsafe());
}

inline EtcdError makeJsonParseError(const std::string& context, simdjson::error_code error)
{
    return EtcdError(
        EtcdErrorType::Parse,
        context + ": " + std::string(simdjson::error_message(error)));
}

inline std::optional<simdjson::dom::object> parseJsonObject(
    const std::string& body,
    simdjson::dom::parser& parser,
    EtcdError* parse_error,
    const std::string& context)
{
    auto doc_result = parser.parse(body);
    if (doc_result.error()) {
        if (parse_error != nullptr) {
            *parse_error = makeJsonParseError(context, doc_result.error());
        }
        return std::nullopt;
    }

    auto object_result = doc_result.value_unsafe().get_object();
    if (object_result.error()) {
        if (parse_error != nullptr) {
            *parse_error = makeJsonParseError(context, object_result.error());
        }
        return std::nullopt;
    }

    return object_result.value_unsafe();
}

inline std::string normalizeApiPrefix(std::string prefix)
{
    if (prefix.empty()) {
        return "/v3";
    }
    if (prefix.front() != '/') {
        prefix.insert(prefix.begin(), '/');
    }
    while (prefix.size() > 1 && prefix.back() == '/') {
        prefix.pop_back();
    }
    return prefix;
}

inline std::string makePrefixRangeEnd(std::string key)
{
    for (std::ptrdiff_t i = static_cast<std::ptrdiff_t>(key.size()) - 1; i >= 0; --i) {
        const unsigned char ch = static_cast<unsigned char>(key[static_cast<size_t>(i)]);
        if (ch < 0xFF) {
            key[static_cast<size_t>(i)] = static_cast<char>(ch + 1);
            key.resize(static_cast<size_t>(i) + 1);
            return key;
        }
    }
    return std::string(1, '\0');
}

inline std::string encodeBase64(std::string_view data)
{
    return galay::utils::Base64Util::Base64EncodeView(data);
}

inline std::optional<std::string> decodeBase64(std::string_view data)
{
    try {
        return galay::utils::Base64Util::Base64DecodeView(data);
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        return std::nullopt;
    }
}

struct ParsedEndpoint
{
    std::string host;
    uint16_t port = 0;
    bool secure = false;
    bool ipv6 = false;
};

inline std::expected<ParsedEndpoint, std::string> parseEndpoint(const std::string& endpoint)
{
    thread_local const std::regex kEndpointRegex(
        R"(^(http|https)://(\[[^\]]+\]|[^/:]+)(?::(\d+))?(?:/.*)?$)",
        std::regex::icase);
    std::smatch matches;
    if (!std::regex_match(endpoint, matches, kEndpointRegex)) {
        return std::unexpected("invalid endpoint: " + endpoint);
    }

    ParsedEndpoint parsed;
    std::string scheme = matches[1].str();
    std::transform(
        scheme.begin(),
        scheme.end(),
        scheme.begin(),
        [](const unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    parsed.secure = scheme == "https";

    std::string host = matches[2].str();
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
        host = host.substr(1, host.size() - 2);
        parsed.ipv6 = true;
    } else if (host.find(':') != std::string::npos) {
        parsed.ipv6 = true;
    }
    parsed.host = std::move(host);

    int port = parsed.secure ? 443 : 80;
    if (matches[3].matched) {
        try {
            port = std::stoi(matches[3].str());
        } catch (const std::bad_alloc&) {
            throw;
        } catch (...) {
            return std::unexpected("invalid endpoint port: " + endpoint);
        }
        if (port <= 0 || port > 65535) {
            return std::unexpected("endpoint port out of range: " + endpoint);
        }
    }
    parsed.port = static_cast<uint16_t>(port);
    return parsed;
}

inline std::string buildHostHeader(const std::string& host, uint16_t port, bool ipv6)
{
    if (ipv6) {
        return "[" + host + "]:" + std::to_string(port);
    }
    return host + ":" + std::to_string(port);
}

inline simdjson::dom::parser& threadLocalJsonParser()
{
    thread_local simdjson::dom::parser parser;
    return parser;
}

inline std::expected<simdjson::dom::object, EtcdError> parseEtcdSuccessObject(
    const std::string& body,
    const std::string& context)
{
    EtcdError parse_error(EtcdErrorType::Success);
    auto root = parseJsonObject(body, threadLocalJsonParser(), &parse_error, context);
    if (!root.has_value()) {
        return std::unexpected(std::move(parse_error));
    }

    if (auto error_code = findIntField(root.value(), "code");
        error_code.has_value() && error_code.value() != 0) {
        const std::string message =
            findStringField(root.value(), "message")
                .value_or("etcd server returned error");
        return std::unexpected(EtcdError(
            EtcdErrorType::Server,
            "code=" + std::to_string(error_code.value()) + ", message=" + message));
    }

    return root.value();
}

inline std::expected<std::vector<EtcdKeyValue>, EtcdError> parseKvsFromObject(
    const simdjson::dom::object& object,
    const std::string& context)
{
    auto kvs_field = object["kvs"];
    if (kvs_field.error()) {
        return std::vector<EtcdKeyValue>{};
    }

    auto kvs_array_result = kvs_field.value_unsafe().get_array();
    if (kvs_array_result.error()) {
        return std::unexpected(makeJsonParseError(context + ".kvs as array", kvs_array_result.error()));
    }

    const auto kvs_array = kvs_array_result.value_unsafe();
    std::vector<EtcdKeyValue> kvs;
    kvs.reserve(kvs_array.size());

    for (auto kv_element : kvs_array) {
        auto kv_object_result = kv_element.get_object();
        if (kv_object_result.error()) {
            return std::unexpected(makeJsonParseError(context + ".kv item as object", kv_object_result.error()));
        }

        const auto kv_object = kv_object_result.value_unsafe();
        const auto encoded_key = findStringField(kv_object, "key");
        if (!encoded_key.has_value()) {
            return std::unexpected(EtcdError(EtcdErrorType::Parse, context + ": missing key in kv item"));
        }

        const auto decoded_key = decodeBase64(encoded_key.value());
        if (!decoded_key.has_value()) {
            return std::unexpected(EtcdError(EtcdErrorType::Parse, context + ": failed to decode base64 key"));
        }

        const auto encoded_value = findStringField(kv_object, "value").value_or("");
        const auto decoded_value = decodeBase64(encoded_value);
        if (!decoded_value.has_value()) {
            return std::unexpected(EtcdError(EtcdErrorType::Parse, context + ": failed to decode base64 value"));
        }

        EtcdKeyValue item;
        item.key = decoded_key.value();
        item.value = decoded_value.value();
        item.create_revision = findIntField(kv_object, "create_revision").value_or(0);
        item.mod_revision = findIntField(kv_object, "mod_revision").value_or(0);
        item.version = findIntField(kv_object, "version").value_or(0);
        item.lease = findIntField(kv_object, "lease").value_or(0);
        kvs.push_back(std::move(item));
    }

    return kvs;
}

inline bool maybeContainsEtcdErrorFields(const std::string& body)
{
    return body.find("\"code\"") != std::string::npos ||
        body.find("\"message\"") != std::string::npos ||
        body.find("\"error\"") != std::string::npos;
}

inline std::expected<std::string, EtcdError> buildPutRequestBody(
    std::string_view key,
    std::string_view value,
    std::optional<int64_t> lease_id = std::nullopt)
{
    if (key.empty()) {
        return std::unexpected(EtcdError(EtcdErrorType::InvalidParam, "key must not be empty"));
    }
    if (lease_id.has_value() && lease_id.value() <= 0) {
        return std::unexpected(EtcdError(EtcdErrorType::InvalidParam, "lease id must be positive"));
    }

    const std::string encoded_key = encodeBase64(key);
    const std::string encoded_value = encodeBase64(value);
    std::string body;
    body.reserve(40 + encoded_key.size() + encoded_value.size() + (lease_id.has_value() ? 32 : 0));
    body += "{\"key\":\"";
    body += encoded_key;
    body += "\",\"value\":\"";
    body += encoded_value;
    body += "\"";
    if (lease_id.has_value()) {
        body += ",\"lease\":\"";
        body += std::to_string(lease_id.value());
        body += "\"";
    }
    body += "}";
    return body;
}

inline std::expected<std::string, EtcdError> buildGetRequestBody(
    std::string_view key,
    bool prefix = false,
    std::optional<int64_t> limit = std::nullopt)
{
    if (key.empty()) {
        return std::unexpected(EtcdError(EtcdErrorType::InvalidParam, "key must not be empty"));
    }
    if (limit.has_value() && limit.value() <= 0) {
        return std::unexpected(EtcdError(EtcdErrorType::InvalidParam, "limit must be positive"));
    }

    const std::string encoded_key = encodeBase64(key);
    std::string encoded_range_end;
    if (prefix) {
        encoded_range_end = encodeBase64(makePrefixRangeEnd(std::string(key)));
    }

    std::string body;
    body.reserve(32 + encoded_key.size() + encoded_range_end.size() + (limit.has_value() ? 24 : 0));
    body += "{\"key\":\"";
    body += encoded_key;
    body += "\"";
    if (prefix) {
        body += ",\"range_end\":\"";
        body += encoded_range_end;
        body += "\"";
    }
    if (limit.has_value()) {
        body += ",\"limit\":";
        body += std::to_string(limit.value());
    }
    body += "}";
    return body;
}

inline std::expected<std::string, EtcdError> buildDeleteRequestBody(
    std::string_view key,
    bool prefix = false)
{
    if (key.empty()) {
        return std::unexpected(EtcdError(EtcdErrorType::InvalidParam, "key must not be empty"));
    }

    const std::string encoded_key = encodeBase64(key);
    std::string encoded_range_end;
    if (prefix) {
        encoded_range_end = encodeBase64(makePrefixRangeEnd(std::string(key)));
    }

    std::string body;
    body.reserve(32 + encoded_key.size() + encoded_range_end.size());
    body += "{\"key\":\"";
    body += encoded_key;
    body += "\"";
    if (prefix) {
        body += ",\"range_end\":\"";
        body += encoded_range_end;
        body += "\"";
    }
    body += "}";
    return body;
}

inline std::expected<std::string, EtcdError> buildLeaseGrantRequestBody(int64_t ttl_seconds)
{
    if (ttl_seconds <= 0) {
        return std::unexpected(EtcdError(EtcdErrorType::InvalidParam, "ttl must be positive"));
    }
    return std::string("{\"TTL\":") + std::to_string(ttl_seconds) + "}";
}

inline std::expected<std::string, EtcdError> buildLeaseKeepAliveRequestBody(int64_t lease_id)
{
    if (lease_id <= 0) {
        return std::unexpected(EtcdError(EtcdErrorType::InvalidParam, "lease id must be positive"));
    }
    return std::string("{\"ID\":\"") + std::to_string(lease_id) + "\"}";
}

inline std::expected<std::string, EtcdError> buildTxnBody(std::span<const PipelineOp> operations)
{
    if (operations.empty()) {
        return std::unexpected(EtcdError(EtcdErrorType::InvalidParam, "pipeline operations must not be empty"));
    }

    std::string body = "{\"compare\":[],\"success\":[";
    body.reserve(operations.size() * 96 + 32);
    bool first = true;
    for (const auto& op : operations) {
        if (op.key.empty()) {
            return std::unexpected(EtcdError(EtcdErrorType::InvalidParam, "pipeline op key must not be empty"));
        }
        if (op.limit.has_value() && op.limit.value() <= 0) {
            return std::unexpected(EtcdError(EtcdErrorType::InvalidParam, "pipeline op limit must be positive"));
        }
        if (op.lease_id.has_value() && op.lease_id.value() <= 0) {
            return std::unexpected(EtcdError(EtcdErrorType::InvalidParam, "pipeline op lease id must be positive"));
        }

        if (!first) {
            body += ",";
        }
        first = false;

        switch (op.type) {
        case PipelineOpType::Put: {
            const std::string encoded_key = encodeBase64(op.key);
            const std::string encoded_value = encodeBase64(op.value);
            body += "{\"request_put\":{\"key\":\"";
            body += encoded_key;
            body += "\",\"value\":\"";
            body += encoded_value;
            body += "\"";
            if (op.lease_id.has_value()) {
                body += ",\"lease\":\"" + std::to_string(op.lease_id.value()) + "\"";
            }
            body += "}}";
            break;
        }
        case PipelineOpType::Get: {
            const std::string encoded_key = encodeBase64(op.key);
            body += "{\"request_range\":{\"key\":\"";
            body += encoded_key;
            body += "\"";
            if (op.prefix) {
                body += ",\"range_end\":\"";
                body += encodeBase64(makePrefixRangeEnd(op.key));
                body += "\"";
            }
            if (op.limit.has_value()) {
                body += ",\"limit\":" + std::to_string(op.limit.value());
            }
            body += "}}";
            break;
        }
        case PipelineOpType::Delete: {
            const std::string encoded_key = encodeBase64(op.key);
            body += "{\"request_delete_range\":{\"key\":\"";
            body += encoded_key;
            body += "\"";
            if (op.prefix) {
                body += ",\"range_end\":\"";
                body += encodeBase64(makePrefixRangeEnd(op.key));
                body += "\"";
            }
            body += "}}";
            break;
        }
        }
    }

    body += "],\"failure\":[]}";
    return body;
}

inline std::expected<std::string, EtcdError> buildTxnBody(const std::vector<PipelineOp>& operations)
{
    return buildTxnBody(std::span<const PipelineOp>(operations.data(), operations.size()));
}

inline std::expected<std::vector<PipelineItemResult>, EtcdError> parsePipelineResponses(
    const simdjson::dom::object& root,
    std::span<const PipelineOpType> operation_types)
{
    auto succeeded_field = root["succeeded"];
    if (!succeeded_field.error()) {
        auto succeeded_result = succeeded_field.value_unsafe().get_bool();
        if (!succeeded_result.error() && !succeeded_result.value_unsafe()) {
            return std::unexpected(EtcdError(EtcdErrorType::Server, "pipeline txn returned succeeded=false"));
        }
    }

    auto responses_field = root["responses"];
    if (responses_field.error()) {
        return std::unexpected(EtcdError(EtcdErrorType::Parse, "pipeline txn response missing responses field"));
    }

    auto responses_array_result = responses_field.value_unsafe().get_array();
    if (responses_array_result.error()) {
        return std::unexpected(makeJsonParseError("parse pipeline responses as array", responses_array_result.error()));
    }

    const auto responses = responses_array_result.value_unsafe();
    if (responses.size() != operation_types.size()) {
        return std::unexpected(EtcdError(
            EtcdErrorType::Parse,
            "pipeline responses size mismatch, expected=" + std::to_string(operation_types.size()) +
                ", actual=" + std::to_string(responses.size())));
    }

    std::vector<PipelineItemResult> pipeline_results;
    pipeline_results.reserve(operation_types.size());

    for (size_t i = 0; i < operation_types.size(); ++i) {
        auto item_object_result = responses.at(i).get_object();
        if (item_object_result.error()) {
            return std::unexpected(makeJsonParseError(
                "parse pipeline response item as object",
                item_object_result.error()));
        }

        const auto item_object = item_object_result.value_unsafe();
        PipelineItemResult item;
        item.type = operation_types[i];

        switch (operation_types[i]) {
        case PipelineOpType::Put: {
            auto put_field = item_object["response_put"];
            if (put_field.error()) {
                return std::unexpected(EtcdError(
                    EtcdErrorType::Parse,
                    "pipeline put response missing response_put"));
            }
            auto put_object_result = put_field.value_unsafe().get_object();
            if (put_object_result.error()) {
                return std::unexpected(makeJsonParseError(
                    "parse pipeline response_put as object",
                    put_object_result.error()));
            }
            item.ok = true;
            break;
        }
        case PipelineOpType::Get: {
            auto range_field = item_object["response_range"];
            if (range_field.error()) {
                return std::unexpected(EtcdError(
                    EtcdErrorType::Parse,
                    "pipeline get response missing response_range"));
            }
            auto range_object_result = range_field.value_unsafe().get_object();
            if (range_object_result.error()) {
                return std::unexpected(makeJsonParseError(
                    "parse pipeline response_range as object",
                    range_object_result.error()));
            }

            auto kvs_result = parseKvsFromObject(
                range_object_result.value_unsafe(),
                "parse pipeline response_range");
            if (!kvs_result.has_value()) {
                return std::unexpected(kvs_result.error());
            }
            item.kvs = std::move(kvs_result.value());
            item.ok = true;
            break;
        }
        case PipelineOpType::Delete: {
            auto del_field = item_object["response_delete_range"];
            if (del_field.error()) {
                return std::unexpected(EtcdError(
                    EtcdErrorType::Parse,
                    "pipeline delete response missing response_delete_range"));
            }
            auto del_object_result = del_field.value_unsafe().get_object();
            if (del_object_result.error()) {
                return std::unexpected(makeJsonParseError(
                    "parse pipeline response_delete_range as object",
                    del_object_result.error()));
            }
            item.deleted_count = findIntField(del_object_result.value_unsafe(), "deleted").value_or(0);
            item.ok = true;
            break;
        }
        }

        pipeline_results.push_back(std::move(item));
    }

    return pipeline_results;
}

inline std::expected<std::vector<PipelineItemResult>, EtcdError> parsePipelineResponses(
    const simdjson::dom::object& root,
    std::span<const PipelineOp> operations)
{
    std::vector<PipelineOpType> operation_types;
    operation_types.reserve(operations.size());
    for (const auto& op : operations) {
        operation_types.push_back(op.type);
    }
    return parsePipelineResponses(root, std::span<const PipelineOpType>(operation_types.data(), operation_types.size()));
}

inline std::expected<void, EtcdError> parsePutResponse(const std::string& body)
{
    if (!maybeContainsEtcdErrorFields(body)) {
        return {};
    }

    auto root = parseEtcdSuccessObject(body, "parse put response");
    if (!root.has_value()) {
        return std::unexpected(root.error());
    }
    return {};
}

inline std::expected<std::vector<EtcdKeyValue>, EtcdError> parseGetResponseKvs(const std::string& body)
{
    auto root = parseEtcdSuccessObject(body, "parse get response");
    if (!root.has_value()) {
        return std::unexpected(root.error());
    }
    return parseKvsFromObject(root.value(), "parse get response");
}

inline std::expected<int64_t, EtcdError> parseDeleteResponseDeletedCount(const std::string& body)
{
    auto root = parseEtcdSuccessObject(body, "parse delete response");
    if (!root.has_value()) {
        return std::unexpected(root.error());
    }
    return findIntField(root.value(), "deleted").value_or(0);
}

inline std::expected<int64_t, EtcdError> parseLeaseGrantResponseId(const std::string& body)
{
    auto root = parseEtcdSuccessObject(body, "parse lease grant response");
    if (!root.has_value()) {
        return std::unexpected(root.error());
    }

    const auto lease_id = findIntField(root.value(), "ID");
    if (!lease_id.has_value()) {
        return std::unexpected(EtcdError(EtcdErrorType::Parse, "lease grant response missing ID"));
    }
    return lease_id.value();
}

inline std::expected<int64_t, EtcdError> parseLeaseKeepAliveResponseId(
    const std::string& body,
    int64_t expected_lease_id)
{
    auto root = parseEtcdSuccessObject(body, "parse lease keepalive response");
    if (!root.has_value()) {
        return std::unexpected(root.error());
    }

    const auto response_id = findIntField(root.value(), "ID");
    if (response_id.has_value() && response_id.value() != expected_lease_id) {
        return std::unexpected(EtcdError(EtcdErrorType::Parse, "lease keepalive response id mismatch"));
    }
    return expected_lease_id;
}

inline std::expected<std::vector<PipelineItemResult>, EtcdError> parsePipelineTxnResponse(
    const std::string& body,
    std::span<const PipelineOpType> operation_types)
{
    auto root = parseEtcdSuccessObject(body, "parse pipeline txn response");
    if (!root.has_value()) {
        return std::unexpected(root.error());
    }
    return parsePipelineResponses(root.value(), operation_types);
}

inline std::expected<std::vector<PipelineItemResult>, EtcdError> parsePipelineTxnResponse(
    const std::string& body,
    std::span<const PipelineOp> operations)
{
    auto root = parseEtcdSuccessObject(body, "parse pipeline txn response");
    if (!root.has_value()) {
        return std::unexpected(root.error());
    }
    return parsePipelineResponses(root.value(), operations);
}

} // namespace galay::etcd::internal

#endif // GALAY_ETCD_INTERNAL_H
