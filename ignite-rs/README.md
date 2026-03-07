# ignite-rs — Apache Ignite thin client (sync + async)

Rust thin client for Apache Ignite with high-performance sync and Tokio-based async APIs. Optional TLS is supported for both sync (rustls) and async (tokio-rustls). This crate also includes helpers, tests, and benchmarks to compare sync vs async performance.

## Features

- Sync and async clients with shared encoding/decoding for zero duplication
- Optional TLS (server-auth and mutual TLS)
- Helper constructors to build `ClientConfig` from PEM files (no direct rustls usage needed)
- Integration tests including async TLS
- Criterion benchmarks for concurrent sync vs async, with byte-size parameterization and CSV reporting

## Crate Feature Flags

- `rt-tokio`: enables async client (`AsyncClient`) built on Tokio
- `ssl`: enables TLS (rustls 0.22, tokio-rustls 0.25 for async)

Enable features in your build:

```
cargo build --manifest-path crates/ignite-rs/Cargo.toml --features rt-tokio
cargo build --manifest-path crates/ignite-rs/Cargo.toml --features "rt-tokio,ssl"
```

## Usage

### Synchronous client

```rust
use ignite_rs::{new_client, ClientConfig, Ignite};

fn main() -> ignite_rs::error::IgniteResult<()> {
    let conf = ClientConfig::new("127.0.0.1:10800");
    let mut client = new_client(conf)?;
    let names = client.get_cache_names()?;
    println!("{:?}", names);
    Ok(())
}
```

### Asynchronous client (Tokio)

```rust
use ignite_rs::{AsyncClient, ClientConfig};

#[tokio::main]
async fn main() -> ignite_rs::error::IgniteResult<()> {
    let conf = ClientConfig::new("127.0.0.1:10800");
    let client = AsyncClient::new_async(conf).await?;
    let names = client.get_cache_names().await?;
    println!("{:?}", names);
    Ok(())
}
```

## TLS Helpers (no rustls in your code)

When built with `ssl`, the crate exposes helpers to build `ClientConfig` from PEM files.

- Server-auth only (CA PEM + SNI):

```rust
use ignite_rs::client_config_from_ca_pem;

let conf = client_config_from_ca_pem(
    "127.0.0.1:15443",
    "./ca.pem",
    "localhost",
)?;
```

- Mutual TLS (CA PEM + client cert PEM + client key PEM + SNI):

```rust
use ignite_rs::client_config_from_ca_and_client_pem;

let conf = client_config_from_ca_and_client_pem(
    "127.0.0.1:15443",
    "./ca.pem",
    "./client.crt.pem",
    "./client.key.pem",
    "localhost",
)?;
```

These helpers work for both the sync and async clients.

## Async TLS Integration Tests

Tests fail if required env vars are missing. Run with features and env set:

```
export IGNITE_TLS_ADDR=127.0.0.1:15443
export IGNITE_TLS_SERVER_NAME=localhost
export IGNITE_TLS_CA_PEM=/path/to/ca.pem
# Optional strict assertion
export IGNITE_EXPECTED_CACHE_NAMES=SQL_PUBLIC_RAINBOW

# Server-auth only
cargo test --manifest-path crates/ignite-rs/Cargo.toml --features "rt-tokio,ssl"

# Mutual TLS
export IGNITE_TLS_CLIENT_CERT_PEM=/path/to/client.crt.pem
export IGNITE_TLS_CLIENT_KEY_PEM=/path/to/client.key.pem
cargo test --manifest-path crates/ignite-rs/Cargo.toml --features "rt-tokio,ssl"
```

Tests live in `crates/ignite-rs/tests/async_tls.rs` and use the TLS helper to avoid rustls imports.

## Benchmarks (concurrent, sync vs async)

Criterion benchmarks live at `crates/ignite-rs/benches/sync_async_bench.rs`. They measure ops/sec under concurrent load for:

- get_cache_names (sync/async)
- put_get (sync/async)
- put_all_get_all (sync/async)
- put_get with `Vec<u8>` values (size-param: sync/async)

Environment variables:

- `IGNITE_ADDR` (default `127.0.0.1:10800`)
- `IGNITE_BENCH_ITERS` (per-worker iterations, default `100`)
- `IGNITE_BENCH_BATCH` (batch size for put_all/get_all, default `100`)
- `IGNITE_BENCH_SIZES` (comma-separated payload sizes for bytes benches, default `16,128,1024,8192`)
- `IGNITE_BENCH_CACHE` (base cache name; caches auto-created)
- Reporting: `IGNITE_BENCH_REPORT` for per-sample CSV path (default `target/criterion/ignite_summary.csv`)

Run benches (sync + async without TLS):

```
IGNITE_ADDR=127.0.0.1:10800 \
  cargo bench --manifest-path crates/ignite-rs/Cargo.toml \
  --features rt-tokio --bench sync_async_bench
```

### Summary tool (conclusion: async vs sync)

The bench writes per-sample CSV rows and an aggregate summary tool reads them to compute mean/median/p90/p95 and compare async vs sync.

- Binary: `bench_summary`
- Inputs: per-sample CSV (default `target/criterion/ignite_summary.csv`)
- Outputs: aggregate CSV at `target/criterion/ignite_summary_agg.csv` and a human-readable comparison printed to stdout

Run:

```
(cd crates/ignite-rs && cargo run --bin bench_summary)
```

Example conclusion (from a local run without TLS):

- get_cache_names: async/sync median ratio ≈ 1.04x (comparable)
- put_get: async/sync median ratio ≈ 1.01x (comparable)
- put_all_get_all: async/sync median ratio ≈ 0.99x (comparable)
- put_get_bytes (16/256B): async/sync median ratio ≈ 1.03x/1.02x (comparable)

Overall, async and sync throughput are comparable across tested operations and concurrencies. Async is slightly ahead on some single-key operations while sync can match or edge out on some batch workloads.

## Notes

- The async client uses a single connection guarded by a Tokio mutex to mirror the sync client’s single-connection semantics. For higher true parallelism across requests, use multiple clients/connections.
- Encoding/decoding paths are shared between sync and async to avoid duplication and dynamic dispatch.

## Dev Tips

- Format: `cargo fmt`
- Lint: `cargo clippy --all-targets -D warnings`
- Tests: `cargo test --manifest-path crates/ignite-rs/Cargo.toml`
- Example: `cargo run --manifest-path crates/example/Cargo.toml`
