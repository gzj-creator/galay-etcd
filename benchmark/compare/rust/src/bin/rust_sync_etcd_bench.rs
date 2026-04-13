use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use reqwest::blocking::Client;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
enum Mode {
    Put,
    Mixed,
}

#[derive(Clone)]
struct Options {
    endpoint: String,
    workers: usize,
    ops_per_worker: usize,
    value_size: usize,
    mode: Mode,
}

struct WorkerResult {
    success: i64,
    failure: i64,
    latency_us: Vec<i64>,
}

struct SharedState {
    ready_workers: AtomicUsize,
    startup_failures: AtomicUsize,
    finished_workers: AtomicUsize,
    benchmark_started: AtomicBool,
    benchmark_aborted: AtomicBool,
}

fn parse_mode(value: &str) -> Result<Mode, String> {
    match value {
        "put" => Ok(Mode::Put),
        "mixed" => Ok(Mode::Mixed),
        other => Err(format!("invalid mode: {other}, expected put or mixed")),
    }
}

fn parse_args() -> Result<Options, String> {
    let args: Vec<String> = std::env::args().collect();
    let endpoint = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "http://127.0.0.1:2379".to_string());
    let workers = args
        .get(2)
        .map(|v| {
            v.parse::<usize>()
                .map_err(|_| format!("invalid workers: {v}"))
        })
        .transpose()?
        .unwrap_or(8)
        .max(1);
    let ops_per_worker = args
        .get(3)
        .map(|v| {
            v.parse::<usize>()
                .map_err(|_| format!("invalid ops_per_worker: {v}"))
        })
        .transpose()?
        .unwrap_or(500)
        .max(1);
    let value_size = args
        .get(4)
        .map(|v| {
            v.parse::<usize>()
                .map_err(|_| format!("invalid value_size: {v}"))
        })
        .transpose()?
        .unwrap_or(64)
        .max(1);
    let mode = parse_mode(args.get(5).map(String::as_str).unwrap_or("put"))?;

    Ok(Options {
        endpoint,
        workers,
        ops_per_worker,
        value_size,
        mode,
    })
}

fn percentile(sorted: &[i64], p: f64) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = p * (sorted.len().saturating_sub(1)) as f64;
    let idx = rank.floor() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::Put => "put",
        Mode::Mixed => "mixed(put+get)",
    }
}

fn request_json(key: &str, value: &str) -> serde_json::Value {
    json!({
        "key": STANDARD.encode(key),
        "value": STANDARD.encode(value),
    })
}

fn range_json(key: &str) -> serde_json::Value {
    json!({
        "key": STANDARD.encode(key),
    })
}

fn delete_prefix_json(prefix: &str) -> serde_json::Value {
    let range_end = prefix_range_end(prefix.as_bytes());
    json!({
        "key": STANDARD.encode(prefix),
        "range_end": STANDARD.encode(range_end),
    })
}

fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
    let mut bytes = prefix.to_vec();
    for i in (0..bytes.len()).rev() {
        if bytes[i] != 0xFF {
            bytes[i] += 1;
            bytes.truncate(i + 1);
            return bytes;
        }
    }
    vec![0]
}

fn post_json(
    client: &Client,
    url: &str,
    body: &serde_json::Value,
) -> Result<serde_json::Value, ()> {
    let response = client.post(url).json(body).send().map_err(|_| ())?;
    if !response.status().is_success() {
        return Err(());
    }
    response.json::<serde_json::Value>().map_err(|_| ())
}

fn warmup_connection(client: &Client, range_url: &str, warmup_key: &str) -> bool {
    match post_json(client, range_url, &range_json(warmup_key)) {
        Ok(resp) => resp
            .get("kvs")
            .and_then(|kvs| kvs.as_array())
            .map(|kvs| kvs.is_empty())
            .unwrap_or(true),
        Err(_) => false,
    }
}

fn cleanup_prefix(endpoint: &str, prefix: &str) {
    let client = match Client::builder().timeout(Duration::from_secs(30)).build() {
        Ok(client) => client,
        Err(_) => return,
    };
    let delete_url = format!("{}/v3/kv/deleterange", endpoint.trim_end_matches('/'));
    let _ = post_json(&client, &delete_url, &delete_prefix_json(prefix));
}

fn run_worker(
    options: Arc<Options>,
    state: Arc<SharedState>,
    worker_id: usize,
    prefix: String,
    value: Arc<String>,
) -> WorkerResult {
    let client = match Client::builder().timeout(Duration::from_secs(30)).build() {
        Ok(client) => client,
        Err(_) => {
            return WorkerResult {
                success: 0,
                failure: options.ops_per_worker as i64,
                latency_us: Vec::new(),
            }
        }
    };
    let put_url = format!("{}/v3/kv/put", options.endpoint.trim_end_matches('/'));
    let range_url = format!("{}/v3/kv/range", options.endpoint.trim_end_matches('/'));

    let warmup_key = format!("{prefix}warmup/{worker_id}");
    if !warmup_connection(&client, &range_url, &warmup_key) {
        state.startup_failures.fetch_add(1, Ordering::Release);
        return WorkerResult {
            success: 0,
            failure: options.ops_per_worker as i64,
            latency_us: Vec::new(),
        };
    }

    state.ready_workers.fetch_add(1, Ordering::Release);
    while !state.benchmark_started.load(Ordering::Acquire)
        && !state.benchmark_aborted.load(Ordering::Acquire)
    {
        thread::sleep(Duration::from_millis(1));
    }

    if state.benchmark_aborted.load(Ordering::Acquire) {
        return WorkerResult {
            success: 0,
            failure: 0,
            latency_us: Vec::new(),
        };
    }

    let mut success = 0_i64;
    let mut failure = 0_i64;
    let mut latency_us = Vec::with_capacity(options.ops_per_worker);

    for i in 0..options.ops_per_worker {
        let key = format!("{prefix}{worker_id}/{i}");
        let begin = Instant::now();
        let ok = match options.mode {
            Mode::Put => post_json(&client, &put_url, &request_json(&key, value.as_str())).is_ok(),
            Mode::Mixed => {
                if post_json(&client, &put_url, &request_json(&key, value.as_str())).is_ok() {
                    match post_json(&client, &range_url, &range_json(&key)) {
                        Ok(resp) => resp
                            .get("kvs")
                            .and_then(|kvs| kvs.as_array())
                            .map(|kvs| !kvs.is_empty())
                            .unwrap_or(false),
                        Err(_) => false,
                    }
                } else {
                    false
                }
            }
        };
        latency_us.push(begin.elapsed().as_micros() as i64);
        if ok {
            success += 1;
        } else {
            failure += 1;
        }
    }

    state.finished_workers.fetch_add(1, Ordering::Release);
    WorkerResult {
        success,
        failure,
        latency_us,
    }
}

fn main() {
    let options = match parse_args() {
        Ok(options) => Arc::new(options),
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    let value = Arc::new("x".repeat(options.value_size));
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    let prefix = format!("/galay-etcd/rust-bench/sync/{micros}/");
    let state = Arc::new(SharedState {
        ready_workers: AtomicUsize::new(0),
        startup_failures: AtomicUsize::new(0),
        finished_workers: AtomicUsize::new(0),
        benchmark_started: AtomicBool::new(false),
        benchmark_aborted: AtomicBool::new(false),
    });

    let mut handles = Vec::with_capacity(options.workers);
    for worker_id in 0..options.workers {
        let options = Arc::clone(&options);
        let state = Arc::clone(&state);
        let prefix = prefix.clone();
        let value = Arc::clone(&value);
        handles.push(thread::spawn(move || {
            run_worker(options, state, worker_id, prefix, value)
        }));
    }

    while state.ready_workers.load(Ordering::Acquire) + state.startup_failures.load(Ordering::Acquire)
        < options.workers
    {
        thread::sleep(Duration::from_millis(1));
    }

    if state.startup_failures.load(Ordering::Acquire) != 0
        || state.ready_workers.load(Ordering::Acquire) != options.workers
    {
        state.benchmark_aborted.store(true, Ordering::Release);
        for handle in handles {
            let _ = handle.join();
        }
        eprintln!("benchmark startup failed");
        std::process::exit(1);
    }

    state.benchmark_started.store(true, Ordering::Release);
    let begin = Instant::now();
    while state.finished_workers.load(Ordering::Acquire) < options.workers {
        thread::sleep(Duration::from_millis(1));
    }
    let duration = begin.elapsed();

    let mut success = 0_i64;
    let mut failure = 0_i64;
    let mut latencies = Vec::with_capacity(options.workers * options.ops_per_worker);
    for handle in handles {
        match handle.join() {
            Ok(result) => {
                success += result.success;
                failure += result.failure;
                latencies.extend(result.latency_us);
            }
            Err(_) => {
                failure += options.ops_per_worker as i64;
            }
        }
    }
    cleanup_prefix(&options.endpoint, &prefix);
    latencies.sort_unstable();
    let throughput = if duration.as_secs_f64() > 0.0 {
        success as f64 / duration.as_secs_f64()
    } else {
        0.0
    };

    println!("Endpoint      : {}", options.endpoint);
    println!("Mode          : {}", mode_name(options.mode));
    println!("Workers       : {}", options.workers);
    println!("Ops/worker    : {}", options.ops_per_worker);
    println!("Value size    : {} bytes", options.value_size);
    println!("Total ops     : {}", success + failure);
    println!("Success       : {}", success);
    println!("Failure       : {}", failure);
    println!("Duration      : {:.6} s", duration.as_secs_f64());
    println!("Throughput    : {} ops/s", throughput);
    println!("Latency p50   : {} us", percentile(&latencies, 0.50));
    println!("Latency p95   : {} us", percentile(&latencies, 0.95));
    println!("Latency p99   : {} us", percentile(&latencies, 0.99));
    println!(
        "Latency max   : {} us",
        latencies.last().copied().unwrap_or(0)
    );

    if failure != 0 {
        std::process::exit(2);
    }
}
