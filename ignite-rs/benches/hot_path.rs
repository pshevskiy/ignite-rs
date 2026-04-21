//! Phase 6 — hot-path benchmarks for ignite-rs.
//!
//! Covers `put_single`, `get_single`, and `put_all` at sizes [10, 100, 1000].
//! Uses criterion's async_tokio harness against an Ignite server reachable at
//! `IGNITE_ADDR` (defaults to 127.0.0.1:10800).
//!
//! Run:
//!   IGNITE_ADDR=127.0.0.1:10800 cargo bench --bench hot_path -- --save-baseline phase6_baseline

use std::time::Duration;

use criterion::async_executor::AsyncExecutor;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::runtime::Runtime;

use ignite_rs::cache::Cache;
use ignite_rs::{new_client, Client, ClientConfig};

fn addr() -> String {
    std::env::var("IGNITE_ADDR").unwrap_or_else(|_| "127.0.0.1:10800".to_string())
}

/// Thin wrapper so criterion's async_tokio harness can reuse a single runtime
/// across the whole bench process (tokio::runtime is owned outside the closure).
struct TokioExec<'a>(&'a Runtime);
impl<'a> AsyncExecutor for TokioExec<'a> {
    fn block_on<T>(&self, fut: impl std::future::Future<Output = T>) -> T {
        self.0.block_on(fut)
    }
}

async fn make_client() -> Client {
    new_client(ClientConfig::new(&addr()))
        .await
        .expect("connect ignite")
}

async fn make_cache(client: &Client, name: &str) -> Cache<i32, Vec<u8>> {
    client
        .get_or_create_cache::<i32, Vec<u8>>(name)
        .await
        .expect("get_or_create_cache")
}

/// Seed the cache with `n` entries so `get_single` has something to read.
async fn seed(cache: &Cache<i32, Vec<u8>>, n: i32, payload_len: usize) {
    let v = vec![0u8; payload_len];
    for k in 0..n {
        cache.put(&k, &v).await.expect("seed put");
    }
}

fn bench_put_single(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio rt");
    let client = rt.block_on(make_client());
    let cache = rt.block_on(make_cache(&client, "HOTPATH_PUT_SINGLE"));

    let mut group = c.benchmark_group("put_single");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(50);

    let payload = vec![0u8; 16];
    let mut counter: i32 = 0;
    group.bench_function("1_entry_16b", |b| {
        b.to_async(TokioExec(&rt)).iter(|| {
            counter = counter.wrapping_add(1);
            let key = counter;
            let cache = &cache;
            let payload = &payload;
            async move {
                cache.put(&key, payload).await.expect("put");
                black_box(());
            }
        });
    });
    group.finish();
}

fn bench_get_single(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio rt");
    let client = rt.block_on(make_client());
    let cache = rt.block_on(make_cache(&client, "HOTPATH_GET_SINGLE"));
    // seed deterministic keys [0..1024)
    rt.block_on(seed(&cache, 1024, 16));

    let mut group = c.benchmark_group("get_single");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(50);

    let mut counter: i32 = 0;
    group.bench_function("1_entry_16b", |b| {
        b.to_async(TokioExec(&rt)).iter(|| {
            counter = counter.wrapping_add(1);
            let key = counter & 0x3FF; // 0..1024
            let cache = &cache;
            async move {
                let v = cache.get(&key).await.expect("get");
                black_box(v);
            }
        });
    });
    group.finish();
}

fn bench_put_all(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio rt");
    let client = rt.block_on(make_client());
    let cache = rt.block_on(make_cache(&client, "HOTPATH_PUT_ALL"));

    let mut group = c.benchmark_group("put_all");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(30);

    for &batch in &[10usize, 100, 1000] {
        // pre-build the payloads so we only measure the put_all call itself
        let payload = vec![0u8; 16];
        let pairs: Vec<(i32, Vec<u8>)> = (0..batch as i32).map(|k| (k, payload.clone())).collect();
        let mut batch_ctr: i32 = 0;

        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &_sz| {
            b.to_async(TokioExec(&rt)).iter(|| {
                batch_ctr = batch_ctr.wrapping_add(1);
                let offset = batch_ctr.wrapping_mul(batch as i32);
                // re-key pairs to avoid contention on the same keys across samples
                let pairs_shifted: Vec<(i32, Vec<u8>)> = pairs
                    .iter()
                    .map(|(k, v)| (k.wrapping_add(offset), v.clone()))
                    .collect();
                let cache = &cache;
                async move {
                    cache.put_all(&pairs_shifted).await.expect("put_all");
                    black_box(());
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_put_single, bench_get_single, bench_put_all);
criterion_main!(benches);
