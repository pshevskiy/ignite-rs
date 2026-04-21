//! Post-audit perf investigation — concurrent put/get bench.
//!
//! N tokio tasks share one `Client` and hammer `put` / `get`. Used to
//! decide whether single-op latency is I/O-bound (throughput scales with
//! concurrency), CPU-bound (plateaus at per-op CPU budget × cores), or
//! lock-contention-bound (plateaus earlier than that).
//!
//! Standalone binary, not a criterion harness: criterion's `async_tokio`
//! executor runs iterations strictly sequentially, so we can't observe
//! in-flight parallelism over a single client. Criterion is kept for
//! calibrated single-op microbenches (`benches/hot_path.rs`).
//!
//! Run:
//!   IGNITE_ADDR=127.0.0.1:10800 \
//!     cargo bench --bench concurrent_put
//!
//! Env overrides:
//!   PAYLOAD_BYTES   (default 16)
//!   OPS_PER_TASK    (default 2000)
//!   CONCURRENCIES   (default "1,2,8,32,128")
//!   WARMUP_SECS     (default 1)
//!   MODE            ("put" | "get" | "both"; default "put")
//!   TCP_NODELAY     (optional: "true"/"false" — default: system default)

use std::sync::Arc;
use std::time::{Duration, Instant};

use ignite_rs::cache::Cache;
use ignite_rs::{new_client, ClientConfig};
use tokio::task::JoinSet;

fn addr() -> String {
    std::env::var("IGNITE_ADDR").unwrap_or_else(|_| "127.0.0.1:10800".to_string())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_concurrencies() -> Vec<usize> {
    std::env::var("CONCURRENCIES")
        .unwrap_or_else(|_| "1,2,8,32,128".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect()
}

fn percentile(latencies_ns: &mut [u128], p: f64) -> u128 {
    if latencies_ns.is_empty() {
        return 0;
    }
    latencies_ns.sort_unstable();
    let idx = ((latencies_ns.len() - 1) as f64 * p).round() as usize;
    latencies_ns[idx]
}

async fn run_put_phase(
    cache: Arc<Cache<i64, Vec<u8>>>,
    concurrency: usize,
    ops_per_task: usize,
    payload: Vec<u8>,
    key_base: i64,
) -> (Duration, Vec<u128>) {
    let payload = Arc::new(payload);
    let mut set: JoinSet<Vec<u128>> = JoinSet::new();
    let started = Instant::now();

    for t in 0..concurrency {
        let cache = cache.clone();
        let payload = payload.clone();
        let task_key_base = key_base + (t as i64) * 1_000_000;
        set.spawn(async move {
            let mut lat = Vec::with_capacity(ops_per_task);
            for i in 0..ops_per_task {
                let k = task_key_base + i as i64;
                let t0 = Instant::now();
                cache.put(&k, &*payload).await.expect("put");
                lat.push(t0.elapsed().as_nanos());
            }
            lat
        });
    }

    let mut all_latencies = Vec::with_capacity(concurrency * ops_per_task);
    while let Some(r) = set.join_next().await {
        all_latencies.extend(r.expect("task panic"));
    }
    let elapsed = started.elapsed();
    (elapsed, all_latencies)
}

async fn run_get_phase(
    cache: Arc<Cache<i64, Vec<u8>>>,
    concurrency: usize,
    ops_per_task: usize,
    key_base: i64,
    key_range: i64,
) -> (Duration, Vec<u128>) {
    let mut set: JoinSet<Vec<u128>> = JoinSet::new();
    let started = Instant::now();

    for t in 0..concurrency {
        let cache = cache.clone();
        set.spawn(async move {
            let mut lat = Vec::with_capacity(ops_per_task);
            let mut ctr: i64 = (t as i64).wrapping_mul(7919);
            for _ in 0..ops_per_task {
                ctr = ctr.wrapping_add(1);
                let k = key_base + ctr.rem_euclid(key_range);
                let t0 = Instant::now();
                let _v = cache.get(&k).await.expect("get");
                lat.push(t0.elapsed().as_nanos());
            }
            lat
        });
    }

    let mut all_latencies = Vec::with_capacity(concurrency * ops_per_task);
    while let Some(r) = set.join_next().await {
        all_latencies.extend(r.expect("task panic"));
    }
    let elapsed = started.elapsed();
    (elapsed, all_latencies)
}

fn report(label: &str, concurrency: usize, elapsed: Duration, mut lat_ns: Vec<u128>) {
    let total = lat_ns.len();
    let tput = total as f64 / elapsed.as_secs_f64();
    let mean_ns: u128 = lat_ns.iter().sum::<u128>() / total as u128;
    let p50 = percentile(&mut lat_ns, 0.50);
    let p95 = percentile(&mut lat_ns, 0.95);
    let p99 = percentile(&mut lat_ns, 0.99);
    let max = *lat_ns.last().unwrap();
    println!(
        "{label:6} concurrency={concurrency:<4} ops={total:<7} elapsed={:?} tput={tput:>9.0} ops/s mean={:>7.1}µs p50={:>7.1}µs p95={:>7.1}µs p99={:>7.1}µs max={:>8.1}µs",
        elapsed,
        mean_ns as f64 / 1000.0,
        p50 as f64 / 1000.0,
        p95 as f64 / 1000.0,
        p99 as f64 / 1000.0,
        max as f64 / 1000.0,
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let payload_bytes = env_usize("PAYLOAD_BYTES", 16);
    let ops_per_task = env_usize("OPS_PER_TASK", 2000);
    let warmup_secs = env_usize("WARMUP_SECS", 1) as u64;
    let concurrencies = env_concurrencies();
    let mode = std::env::var("MODE").unwrap_or_else(|_| "put".to_string());

    println!(
        "concurrent_put: addr={} payload={}B ops_per_task={} concurrencies={:?} mode={}",
        addr(),
        payload_bytes,
        ops_per_task,
        concurrencies,
        mode
    );

    let mut cfg = ClientConfig::new(&addr());
    if let Ok(v) = std::env::var("TCP_NODELAY") {
        let nodelay = matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on");
        cfg.tcp_nodelay = Some(nodelay);
        println!("tcp_nodelay = {}", nodelay);
    }
    let client = new_client(cfg).await.expect("connect ignite");
    let cache = Arc::new(
        client
            .get_or_create_cache::<i64, Vec<u8>>("CONC_PUT_BENCH")
            .await
            .expect("cache"),
    );

    let payload = vec![0u8; payload_bytes];

    // Warmup: populate keys 0..10_000 so GETs have data.
    println!("warmup ({}s)...", warmup_secs);
    let warmup_end = Instant::now() + Duration::from_secs(warmup_secs);
    let mut k: i64 = 0;
    while Instant::now() < warmup_end {
        cache.put(&k, &payload).await.expect("warmup put");
        k = (k + 1) % 10_000;
    }
    println!("warmup done, wrote keys 0..{}", k.max(1));

    let seed_keys = 10_000i64;

    for &concurrency in &concurrencies {
        if mode == "put" || mode == "both" {
            let key_base = 1_000_000_000 + (concurrency as i64) * 100_000_000;
            let (elapsed, lat) = run_put_phase(
                cache.clone(),
                concurrency,
                ops_per_task,
                payload.clone(),
                key_base,
            )
            .await;
            report("PUT", concurrency, elapsed, lat);
        }
        if mode == "get" || mode == "both" {
            let (elapsed, lat) =
                run_get_phase(cache.clone(), concurrency, ops_per_task, 0, seed_keys).await;
            report("GET", concurrency, elapsed, lat);
        }
    }
}
