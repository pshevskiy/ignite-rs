# ignite-rs — Apache Ignite thin client (async-first)

Rust thin client for Apache Ignite with an async-first Tokio API. Optional TLS is supported via `rustls` and `tokio-rustls`.

## Features

- Async-first client surface for cache, query, transactions, binary/data structures, cluster, services, and compute
- TLS helpers for server-auth and mutual TLS setups
- Continuous query listeners and transport/lifecycle event subscriptions
- Conflict replication cache operations for dump/DR-style restore flows
- Suite-shaped integration tests aligned with Apache Ignite thin-client coverage
- Benchmarks for transport and cache hot paths

## Feature Flags

- `ssl`: enables TLS support

## Usage

```rust
use ignite_rs::{AsyncClient, ClientConfig};

#[tokio::main]
async fn main() -> ignite_rs::error::IgniteResult<()> {
    let conf = ClientConfig::new("127.0.0.1:10800");
    let client = AsyncClient::new(conf).await?;
    let names = client.get_cache_names().await?;
    println!("{:?}", names);
    Ok(())
}
```

## TLS Helpers

```rust
use ignite_rs::client_config_from_ca_pem;

#[tokio::main]
async fn main() -> ignite_rs::error::IgniteResult<()> {
    let conf = client_config_from_ca_pem(
        "127.0.0.1:15443",
        "./ca.pem",
        "localhost",
    )?;
    let client = ignite_rs::new_client(conf).await?;
    println!("{:?}", client.get_cache_names().await?);
    Ok(())
}
```

For mutual TLS, use `client_config_from_ca_and_client_pem(...)`.

## Tests

- Canonical full-matrix runner: `cargo run --manifest-path Cargo.toml -p xtask -- test-matrix`
- One bucket only: `cargo run --manifest-path Cargo.toml -p xtask -- test-matrix --bucket <pure|single_node|cluster3|cluster3_churn|auth|ssl>`
- Targeted debugging remains available through direct `cargo test ...` commands.
- TLS integration files: `ignite-rs/ignite-rs/tests/ssl_parameters_test.rs` and `ignite-rs/ignite-rs/tests/security_test.rs`
- Live fixture profiles and overrides:
  - Prefer `ignite_scope(IgniteProfile::...)` or `connect_profile(...)` in integration tests; those helpers select the correct live fixture profile and reuse shared managed environments within a process.
  - `IGNITE_ADDR`: external plain single-node endpoint.
  - `IGNITE_3NODE_ADDRS`: external comma-separated 3-node endpoint list.
  - `IGNITE_AUTH_ADDR`, `IGNITE_AUTH_USERNAME`, `IGNITE_AUTH_PASSWORD`: external auth-enabled endpoint and credentials.
  - `IGNITE_TLS_ADDR`, `IGNITE_TLS_SERVER_NAME`, `IGNITE_TLS_CA_PEM`: external TLS endpoint and trust material.
  - `IGNITE_TLS_CLIENT_CERT_PEM`, `IGNITE_TLS_CLIENT_KEY_PEM`: external mTLS client material.
  - `IGNITE_DELAYED_HANDSHAKE_ADDR`: external endpoint used by the live delayed-handshake proxy test.
  - `IGNITE_TEST_IMAGE` and `IGNITE_TEST_TAG`: override the Ignite image name/tag used by the fixture.
  - `IGNITE_TEST_CONTAINER_NAME`: override the shared fixture container base name.
  - `IGNITE_TEST_START_RETRIES` and `IGNITE_TEST_START_DELAY_MS`: widen startup polling for slower CI runners.
  - `DOCKER_HOST`: point managed fixtures at a Docker-compatible remote API endpoint.
  - `TESTCONTAINERS_HOST_OVERRIDE`: override the host part of published fixture addresses when the Docker API is remote.
  - Managed fixtures use the Docker API directly. If `DOCKER_HOST` is unset, the fixture falls back to the default local socket for the current platform.
- Auto-provisioned profiles:
  - plain single-node
  - plain 3-node cluster
  - auth-enabled single-node
  - TLS single-node
  - mutual-TLS single-node
  - delayed-handshake proxy over a live single-node fixture

## Notes

- `Client` and `AsyncClient` are aliases of the same Tokio-backed client type.
- The transport currently multiplexes requests over one active connection. Use multiple clients when you need parallel request throughput.
- Event subscriptions are available through `client.events().subscribe()`.
- Continuous query listeners are available through `cache.continuous_query(...)`.
- Conflict replication helpers live in `ignite_rs::replication` and are sent through `cache.put_all_conflict(...)` / `cache.remove_all_conflict(...)`.
- There is no separate blocking/sync wrapper in the parity surface.
- TLS parity is `rustls`-equivalent. Legacy Java TLS 1.1 / legacy-cipher exact parity is not targeted.

## Dev Tips

- Build: `cargo build --manifest-path ignite-rs/ignite-rs/Cargo.toml`
- Format: `cargo fmt --manifest-path ignite-rs/Cargo.toml --all`
- Bench compile: `cargo bench --manifest-path ignite-rs/ignite-rs/Cargo.toml --no-run`
- Example: `cargo run --manifest-path ignite-rs/example/Cargo.toml`
