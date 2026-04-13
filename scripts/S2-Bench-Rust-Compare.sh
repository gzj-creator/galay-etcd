#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${1:-${PROJECT_DIR}/build}"
BENCH_DIR="${BUILD_DIR}/benchmark"
RUST_DIR="${PROJECT_DIR}/benchmark/compare/rust"
RUST_TARGET_DIR="${RUST_DIR}/target"
CARGO_EXTRA_ARGS="${CARGO_EXTRA_ARGS:-}"

CPP_B1="${BENCH_DIR}/B1-EtcdKvBenchmark"
CPP_B2="${BENCH_DIR}/B2-AsyncEtcdKvBenchmark"
RUST_B1="${RUST_TARGET_DIR}/release/rust_sync_etcd_bench"
RUST_B2="${RUST_TARGET_DIR}/release/rust_async_etcd_bench"

ENDPOINT="${GALAY_ETCD_ENDPOINT:-http://127.0.0.1:2379}"
WORKERS="${GALAY_ETCD_BENCH_WORKERS:-4}"
OPS_PER_WORKER="${GALAY_ETCD_BENCH_OPS_PER_WORKER:-125}"
VALUE_SIZE="${GALAY_ETCD_BENCH_VALUE_SIZE:-32}"
MODE="${GALAY_ETCD_BENCH_MODE:-put}"
IO_SCHEDULERS="${GALAY_ETCD_BENCH_IO_SCHEDULERS:-2}"

extract_metric() {
    local key="$1"
    awk -F': ' -v k="$key" '{
        lhs=$1
        sub(/[[:space:]]+$/, "", lhs)
        if (lhs==k) {
            print $2
            exit
        }
    }'
}

require_binary() {
    local path="$1"
    if [ ! -x "$path" ]; then
        echo "missing executable: $path" >&2
        exit 1
    fi
}

timestamp() {
    date -u +"%Y-%m-%dT%H:%M:%SZ UTC"
}

print_language_metrics() {
    local label="$1"
    local throughput="$2"
    local p50="$3"
    local p95="$4"
    local p99="$5"
    printf "  %-6s throughput=%s  p50=%s  p95=%s  p99=%s\n" "$label" "$throughput" "$p50" "$p95" "$p99"
}

print_scenario_summary() {
    local scenario_label="$1"
    local start_time="$2"
    local end_time="$3"
    local cpp_tp="$4"
    local cpp_p50="$5"
    local cpp_p95="$6"
    local cpp_p99="$7"
    local rust_tp="$8"
    local rust_p50="$9"
    local rust_p95="${10}"
    local rust_p99="${11}"

    echo
    echo "== ${scenario_label} (start_time: ${start_time}, end_time: ${end_time}) =="
    print_language_metrics "C++" "$cpp_tp" "$cpp_p50" "$cpp_p95" "$cpp_p99"
    print_language_metrics "Rust" "$rust_tp" "$rust_p50" "$rust_p95" "$rust_p99"
    echo "resource metrics: not automated; collect manually if needed"
}

if [ "${MODE}" != "put" ] && [ "${MODE}" != "mixed" ]; then
    echo "invalid mode: ${MODE}, expected put or mixed" >&2
    exit 1
fi

echo "== Build Rust benchmark counterparts =="
if [ -n "${CARGO_EXTRA_ARGS}" ]; then
    # Allow local callers to override registry/mirror behavior without editing global cargo config.
    # shellcheck disable=SC2086
    cargo ${CARGO_EXTRA_ARGS} build --manifest-path "${RUST_DIR}/Cargo.toml" --release --target-dir "${RUST_TARGET_DIR}"
else
    cargo build --manifest-path "${RUST_DIR}/Cargo.toml" --release --target-dir "${RUST_TARGET_DIR}"
fi

require_binary "${CPP_B1}"
require_binary "${CPP_B2}"
require_binary "${RUST_B1}"
require_binary "${RUST_B2}"

echo
echo "== Scenario A: Sync benchmark (C++ B1 vs Rust blocking reqwest) =="
SCENARIO_A_START="$(timestamp)"
CPP_B1_OUT="$("${CPP_B1}" "${ENDPOINT}" "${WORKERS}" "${OPS_PER_WORKER}" "${VALUE_SIZE}" "${MODE}")"
printf '%s\n' "${CPP_B1_OUT}"
CPP_B1_TP="$(printf '%s\n' "${CPP_B1_OUT}" | extract_metric "Throughput")"
CPP_B1_P50="$(printf '%s\n' "${CPP_B1_OUT}" | extract_metric "Latency p50")"
CPP_B1_P95="$(printf '%s\n' "${CPP_B1_OUT}" | extract_metric "Latency p95")"
CPP_B1_P99="$(printf '%s\n' "${CPP_B1_OUT}" | extract_metric "Latency p99")"
CPP_B1_FAIL="$(printf '%s\n' "${CPP_B1_OUT}" | extract_metric "Failure")"

RUST_B1_OUT="$("${RUST_B1}" "${ENDPOINT}" "${WORKERS}" "${OPS_PER_WORKER}" "${VALUE_SIZE}" "${MODE}")"
printf '%s\n' "${RUST_B1_OUT}"
RUST_B1_TP="$(printf '%s\n' "${RUST_B1_OUT}" | extract_metric "Throughput")"
RUST_B1_P50="$(printf '%s\n' "${RUST_B1_OUT}" | extract_metric "Latency p50")"
RUST_B1_P95="$(printf '%s\n' "${RUST_B1_OUT}" | extract_metric "Latency p95")"
RUST_B1_P99="$(printf '%s\n' "${RUST_B1_OUT}" | extract_metric "Latency p99")"
RUST_B1_FAIL="$(printf '%s\n' "${RUST_B1_OUT}" | extract_metric "Failure")"
SCENARIO_A_END="$(timestamp)"

echo
echo "== Scenario B: Async benchmark (C++ B2 vs Rust async reqwest/tokio) =="
SCENARIO_B_START="$(timestamp)"
CPP_B2_OUT="$("${CPP_B2}" "${ENDPOINT}" "${WORKERS}" "${OPS_PER_WORKER}" "${VALUE_SIZE}" "${MODE}" "${IO_SCHEDULERS}")"
printf '%s\n' "${CPP_B2_OUT}"
CPP_B2_TP="$(printf '%s\n' "${CPP_B2_OUT}" | extract_metric "Throughput")"
CPP_B2_P50="$(printf '%s\n' "${CPP_B2_OUT}" | extract_metric "Latency p50")"
CPP_B2_P95="$(printf '%s\n' "${CPP_B2_OUT}" | extract_metric "Latency p95")"
CPP_B2_P99="$(printf '%s\n' "${CPP_B2_OUT}" | extract_metric "Latency p99")"
CPP_B2_FAIL="$(printf '%s\n' "${CPP_B2_OUT}" | extract_metric "Failure")"

RUST_B2_OUT="$("${RUST_B2}" "${ENDPOINT}" "${WORKERS}" "${OPS_PER_WORKER}" "${VALUE_SIZE}" "${MODE}" "${IO_SCHEDULERS}")"
printf '%s\n' "${RUST_B2_OUT}"
RUST_B2_TP="$(printf '%s\n' "${RUST_B2_OUT}" | extract_metric "Throughput")"
RUST_B2_P50="$(printf '%s\n' "${RUST_B2_OUT}" | extract_metric "Latency p50")"
RUST_B2_P95="$(printf '%s\n' "${RUST_B2_OUT}" | extract_metric "Latency p95")"
RUST_B2_P99="$(printf '%s\n' "${RUST_B2_OUT}" | extract_metric "Latency p99")"
RUST_B2_FAIL="$(printf '%s\n' "${RUST_B2_OUT}" | extract_metric "Failure")"
SCENARIO_B_END="$(timestamp)"

if [ -z "${CPP_B1_TP}" ] || [ -z "${RUST_B1_TP}" ] || [ -z "${CPP_B2_TP}" ] || [ -z "${RUST_B2_TP}" ]; then
    echo "failed to parse throughput from one or more benchmark outputs" >&2
    exit 1
fi

if [ "${CPP_B1_FAIL:-}" != "0" ] || [ "${RUST_B1_FAIL:-}" != "0" ] || [ "${CPP_B2_FAIL:-}" != "0" ] || [ "${RUST_B2_FAIL:-}" != "0" ]; then
    echo "one or more benchmark runs reported non-zero failures" >&2
    exit 1
fi

print_scenario_summary "Scenario A: Sync benchmark (C++ B1 vs Rust blocking reqwest)" "${SCENARIO_A_START}" "${SCENARIO_A_END}" \
    "${CPP_B1_TP}" "${CPP_B1_P50:-n/a}" "${CPP_B1_P95:-n/a}" "${CPP_B1_P99:-n/a}" \
    "${RUST_B1_TP}" "${RUST_B1_P50:-n/a}" "${RUST_B1_P95:-n/a}" "${RUST_B1_P99:-n/a}"

print_scenario_summary "Scenario B: Async benchmark (C++ B2 vs Rust async reqwest/tokio)" "${SCENARIO_B_START}" "${SCENARIO_B_END}" \
    "${CPP_B2_TP}" "${CPP_B2_P50:-n/a}" "${CPP_B2_P95:-n/a}" "${CPP_B2_P99:-n/a}" \
    "${RUST_B2_TP}" "${RUST_B2_P50:-n/a}" "${RUST_B2_P95:-n/a}" "${RUST_B2_P99:-n/a}"

echo
echo "== Summary =="
printf "sync:  C++ throughput=%s p50=%s p95=%s p99=%s | Rust throughput=%s p50=%s p95=%s p99=%s\n" \
    "${CPP_B1_TP}" "${CPP_B1_P50:-n/a}" "${CPP_B1_P95:-n/a}" "${CPP_B1_P99:-n/a}" \
    "${RUST_B1_TP}" "${RUST_B1_P50:-n/a}" "${RUST_B1_P95:-n/a}" "${RUST_B1_P99:-n/a}"
printf "async: C++ throughput=%s p50=%s p95=%s p99=%s | Rust throughput=%s p50=%s p95=%s p99=%s\n" \
    "${CPP_B2_TP}" "${CPP_B2_P50:-n/a}" "${CPP_B2_P95:-n/a}" "${CPP_B2_P99:-n/a}" \
    "${RUST_B2_TP}" "${RUST_B2_P50:-n/a}" "${RUST_B2_P95:-n/a}" "${RUST_B2_P99:-n/a}"
