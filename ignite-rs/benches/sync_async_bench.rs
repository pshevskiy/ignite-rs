use std::future::Future;
use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write as IoWrite;
use std::path::PathBuf;

use ignite_rs::{new_client, ClientConfig};

fn read_env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn parse_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn report_path() -> PathBuf {
    if let Ok(p) = std::env::var("IGNITE_BENCH_REPORT") {
        return PathBuf::from(p);
    }
    let mut p = PathBuf::from("target/criterion");
    p.push("ignite_summary.csv");
    p
}

fn write_sample(group: &str, conc: usize, ops: u64, dur: Duration) {
    let path = report_path();
    if let Some(dir) = path.parent() {
        let _ = create_dir_all(dir);
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("open report");
    if file.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
        let _ = writeln!(file, "group,concurrency,ops,duration_ns,ops_per_sec");
    }
    let ops_per_sec = (ops as f64) / dur.as_secs_f64();
    let _ = writeln!(
        file,
        "{},{},{},{},{}",
        group,
        conc,
        ops,
        dur.as_nanos(),
        ops_per_sec
    );
}

fn make_config() -> ClientConfig {
    #[cfg(feature = "ssl")]
    {
        let ca = std::env::var("IGNITE_TLS_CA_PEM").ok();
        let sni = std::env::var("IGNITE_TLS_SERVER_NAME").ok();
        let addr = std::env::var("IGNITE_TLS_ADDR").ok();
        if let (Some(ca), Some(sni), Some(addr)) = (ca, sni, addr) {
            return ignite_rs::client_config_from_ca_pem(&addr, &ca, &sni).expect("tls conf");
        }
    }
    let addr = read_env("IGNITE_ADDR", "127.0.0.1:10800");
    ClientConfig::new(&addr)
}

fn parse_sizes_env() -> Vec<usize> {
    if let Ok(s) = std::env::var("IGNITE_BENCH_SIZES") {
        let parts = s.split(',').map(|p| p.trim()).filter(|p| !p.is_empty());
        let mut v = Vec::new();
        for p in parts {
            if let Ok(n) = p.parse::<usize>() {
                v.push(n);
            }
        }
        if !v.is_empty() {
            return v;
        }
    }
    vec![16, 128, 1024, 8192]
}

fn run_local<F: Future>(fut: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, fut)
}

fn bench_get_cache_names(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_cache_names_async");
    let conc_levels = [1usize, 2, 4, 8];
    let iters = parse_env_usize("IGNITE_BENCH_ITERS", 100);

    for &conc in &conc_levels {
        group.throughput(Throughput::Elements((conc * iters) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(conc),
            &conc,
            |b, &concurrency| {
                b.iter_custom(|_| {
                    run_local(async move {
                        let start = Instant::now();
                        let mut tasks = Vec::with_capacity(concurrency);
                        for _ in 0..concurrency {
                            let conf = make_config();
                            tasks.push(tokio::task::spawn_local(async move {
                                let client = new_client(conf).await.expect("client");
                                for _ in 0..iters {
                                    let names = client.get_cache_names().await.expect("names");
                                    black_box(&names);
                                }
                            }));
                        }
                        for t in tasks {
                            t.await.expect("task");
                        }
                        let dur = start.elapsed();
                        write_sample(
                            "get_cache_names_async",
                            concurrency,
                            (concurrency * iters) as u64,
                            dur,
                        );
                        dur
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_put_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_get_async");
    let conc_levels = [1usize, 2, 4, 8];
    let iters = parse_env_usize("IGNITE_BENCH_ITERS", 100);
    let cache_name = read_env("IGNITE_BENCH_CACHE", "BENCH_CACHE");

    run_local(async {
        let client = new_client(make_config()).await.expect("client");
        let _ = client
            .get_or_create_cache::<i32, i32>(&cache_name)
            .await
            .expect("cache");
    });

    for &conc in &conc_levels {
        let name_outer = cache_name.clone();
        group.throughput(Throughput::Elements((conc * iters * 2) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(conc),
            &conc,
            move |b, &concurrency| {
                let name_outer = name_outer.clone();
                b.iter_custom(|_| {
                    let name_outer = name_outer.clone();
                    run_local(async move {
                        let start = Instant::now();
                        let mut tasks = Vec::with_capacity(concurrency);
                        for w in 0..concurrency {
                            let conf = make_config();
                            let name = name_outer.clone();
                            tasks.push(tokio::task::spawn_local(async move {
                                let client = new_client(conf).await.expect("client");
                                let cache = client
                                    .get_or_create_cache::<i32, i32>(&name)
                                    .await
                                    .expect("cache");
                                let base: i32 = (w * iters) as i32;
                                for i in 0..iters {
                                    let k = base + (i as i32);
                                    cache.put(&k, &k).await.expect("put");
                                    let v = cache.get(&k).await.expect("get");
                                    assert_eq!(v, Some(k));
                                    black_box(v);
                                }
                            }));
                        }
                        for t in tasks {
                            t.await.expect("task");
                        }
                        let dur = start.elapsed();
                        write_sample(
                            "put_get_async",
                            concurrency,
                            (concurrency * iters * 2) as u64,
                            dur,
                        );
                        dur
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_put_get_bytes(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_get_async_bytes");
    let conc_levels = [1usize, 2, 4, 8];
    let iters = parse_env_usize("IGNITE_BENCH_ITERS", 100);
    let sizes = parse_sizes_env();

    for &sz in &sizes {
        let cache_name = format!("BENCH_CACHE_BYTES_{}", sz);
        run_local(async {
            let client = new_client(make_config()).await.expect("client");
            let _ = client
                .get_or_create_cache::<i32, Vec<u8>>(&cache_name)
                .await
                .expect("cache");
        });

        for &conc in &conc_levels {
            let name_outer = cache_name.clone();
            let bench_id = BenchmarkId::new(format!("sz{}", sz), conc);
            group.throughput(Throughput::Elements((conc * iters * 2) as u64));
            group.bench_with_input(bench_id, &conc, move |b, &concurrency| {
                let name_outer = name_outer.clone();
                b.iter_custom(|_| {
                    let name_outer = name_outer.clone();
                    run_local(async move {
                        let start = Instant::now();
                        let mut tasks = Vec::with_capacity(concurrency);
                        for w in 0..concurrency {
                            let conf = make_config();
                            let name = name_outer.clone();
                            tasks.push(tokio::task::spawn_local(async move {
                                let client = new_client(conf).await.expect("client");
                                let cache = client
                                    .get_or_create_cache::<i32, Vec<u8>>(&name)
                                    .await
                                    .expect("cache");
                                let mut payload = vec![0u8; sz];
                                let base: i32 = (w * iters) as i32;
                                for i in 0..iters {
                                    let k = base + (i as i32);
                                    payload[0] = (k & 0xFF) as u8;
                                    cache.put(&k, &payload).await.expect("put");
                                    let v = cache.get(&k).await.expect("get");
                                    assert_eq!(v.as_ref().map(|vv| vv.len()), Some(sz));
                                    black_box(v);
                                }
                            }));
                        }
                        for t in tasks {
                            t.await.expect("task");
                        }
                        let dur = start.elapsed();
                        write_sample(
                            &format!("put_get_async_bytes_{}", sz),
                            concurrency,
                            (concurrency * iters * 2) as u64,
                            dur,
                        );
                        dur
                    })
                });
            });
        }
    }
    group.finish();
}

fn bench_put_all_get_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_all_get_all_async");
    let conc_levels = [1usize, 2, 4, 8];
    let iters = parse_env_usize("IGNITE_BENCH_ITERS", 100);
    let batch = parse_env_usize("IGNITE_BENCH_BATCH", 100);
    let cache_name = read_env("IGNITE_BENCH_CACHE", "BENCH_CACHE_PAIRS");

    run_local(async {
        let client = new_client(make_config()).await.expect("client");
        let _ = client
            .get_or_create_cache::<i32, i32>(&cache_name)
            .await
            .expect("cache");
    });

    for &conc in &conc_levels {
        let name_outer = cache_name.clone();
        group.throughput(Throughput::Elements((conc * iters * batch * 2) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(conc),
            &conc,
            move |b, &concurrency| {
                let name_outer = name_outer.clone();
                b.iter_custom(|_| {
                    let name_outer = name_outer.clone();
                    run_local(async move {
                        let start = Instant::now();
                        let mut tasks = Vec::with_capacity(concurrency);
                        for w in 0..concurrency {
                            let conf = make_config();
                            let name = name_outer.clone();
                            tasks.push(tokio::task::spawn_local(async move {
                                let client = new_client(conf).await.expect("client");
                                let cache = client
                                    .get_or_create_cache::<i32, i32>(&name)
                                    .await
                                    .expect("cache");
                                let base: i32 = (w * iters * batch) as i32;
                                let mut pairs = Vec::with_capacity(batch);
                                let mut keys = Vec::with_capacity(batch);
                                for i in 0..iters {
                                    pairs.clear();
                                    keys.clear();
                                    let off = base + (i as i32) * (batch as i32);
                                    for j in 0..batch {
                                        let k = off + (j as i32);
                                        pairs.push((k, k));
                                        keys.push(k);
                                    }
                                    cache.put_all(&pairs).await.expect("put_all");
                                    let got = cache.get_all(&keys).await.expect("get_all");
                                    black_box(&got);
                                }
                            }));
                        }
                        for t in tasks {
                            t.await.expect("task");
                        }
                        let dur = start.elapsed();
                        write_sample(
                            "put_all_get_all_async",
                            concurrency,
                            (concurrency * iters * batch * 2) as u64,
                            dur,
                        );
                        dur
                    })
                });
            },
        );
    }
    group.finish();
}

pub fn criterion_benches(c: &mut Criterion) {
    bench_get_cache_names(c);
    bench_put_get(c);
    bench_put_all_get_all(c);
    bench_put_get_bytes(c);
}

criterion_group!(benches, criterion_benches);
criterion_main!(benches);
