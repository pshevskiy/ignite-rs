# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Async Rust thin client for Apache Ignite, implementing the binary thin-client protocol. Used as a dependency by `rsc-cache-rs` (a high-performance Rust replacement for the Java `rsc-cache` service). Part of the `ignite_all` monorepo alongside the Apache Ignite Java server.

## Build & Test Commands

```bash
# Build
cargo build --manifest-path ignite-rs/Cargo.toml

# Format
cargo fmt --manifest-path ignite-rs/Cargo.toml --all

# Run full test matrix (canonical CI/local runner — use this, not bare cargo test)
cargo run -p xtask -- test-matrix

# Run a single test matrix bucket
cargo run -p xtask -- test-matrix --bucket <pure|single_node|cluster3|cluster3_churn|auth|ssl>

# Run a specific test (for targeted debugging only)
cargo test --manifest-path ignite-rs/ignite-rs/Cargo.toml <test_name>

# Benchmarks (compile-check only)
cargo bench --manifest-path ignite-rs/ignite-rs/Cargo.toml --no-run

# Run example (needs Ignite on localhost:10800)
cargo run --manifest-path ignite-rs/example/Cargo.toml
```

## Workspace Structure

Four crates in the Cargo workspace:
- **`ignite-rs/`** — main library crate (the client)
- **`ignite-rs_derive/`** — proc-macro crate (`#[derive(IgniteObj)]` for auto-serialization to/from Ignite binary format)
- **`example/`** — usage demo
- **`xtask/`** — test matrix orchestration (provisions Docker fixtures, runs buckets sequentially)

## Architecture

### Client Lifecycle

`new_client(ClientConfig)` → creates `ReliableChannel` (transport layer) → establishes `AsyncConnection` (TCP/optional TLS) → performs handshake (protocol v1.7.0) → returns `Client`. The client is fully async (Tokio); `Client` and `AsyncClient` are aliases for the same type.

### Transport Layer (`transport.rs`)

`ReliableChannel` is the core — manages connection pool, discovery, heartbeats, retry logic, and affinity routing. Key behaviors:
- **Connection pool**: configurable size (default 1), auto-reconnect with throttled backoff
- **Partition awareness**: routes requests to the partition-primary node to avoid network hops
- **DC-aware routing**: optional `data_center_id` attribute for multi-datacenter deployments
- **Retry policies**: `Default` (retry on connection errors), `Never`, `ReadOnly` (retry only reads), `Custom(Arc<handler>)`
- **Heartbeat**: configurable interval to keep idle connections alive
- **Request batching**: `write_request_batch` for bulk operations

### Affinity Routing (`affinity.rs`)

Uses `ArcSwap` for lock-free reads on the hot path — no mutex/RwLock contention for affinity lookups. `AffinityCache` maps cache partitions → node IDs. Key function `marshal_key()` hashes a key to its partition ID for routing.

### Binary Protocol (`protocol/`, `binary.rs`, `binary_registry.rs`)

Implements Apache Ignite binary thin-client protocol exactly. Core traits:
- `WritableType` — serialize to Ignite byte sequence
- `ReadableType` — deserialize from Ignite byte sequence
- `IgniteObj` — marker trait combining both (used by derive macro)

Complex object schemas are registered globally via `register_complex_object_schema()`. Type IDs are Java-compatible hashcodes (`utils.rs`: `string_to_java_hashcode`).

### Derive Macro (`ignite-rs_derive/`)

`#[derive(IgniteObj)]` generates `WritableType` + `ReadableType` impls. Uses `#[ignite_type_name]` attribute to override type name for hashcode. Output matches Java binary format exactly (40-byte complex object header).

### Connection (`connection_async.rs`)

`AsyncConnection` wraps `AsyncStream` (enum: `Plain(TcpStream)` | `Tls(TlsStream)`). Handshake negotiates feature flags: partition_awareness, node_endpoints, heartbeat, dc_aware, query_partitions_batch_size.

### Cache Operations (`cache.rs`)

`Cache<K, V>` provides typed CRUD: put/get/put_all/get_all/remove/contains_key/size/clear/destroy. Also: SQL queries, scan queries, continuous queries, conflict replication (`put_all_conflict`/`remove_all_conflict`), and entry listeners.

### Event System (`events.rs`)

`EventBus` with broadcast channels and 256-event circular history buffer. Subscribers get replayed history on subscribe. Event kinds: `ConnectionEvent`, `RequestEvent`, `LifecycleEvent`.

### Error Handling (`error.rs`)

`IgniteError` with `ErrorKind` enum: `Other`, `Connection`, `Handshake`, `Authentication`, `Server`, `Tls`. Connection errors are detected via string matching on descriptions (timeout, connection, reset, etc.) to drive retry decisions.

## Test Infrastructure

### Test Matrix (`xtask`)

The canonical test runner. `xtask test-matrix` loads `ignite-rs/tests/test_matrix.toml`, provisions Docker fixtures per bucket, and runs tests sequentially. Six buckets:

| Bucket | Fixtures | Tests |
|--------|----------|-------|
| `pure` | None (unit/protocol) | ~19 |
| `single_node` | 1 Ignite node | ~18 |
| `cluster3` | 3-node cluster | ~4 |
| `cluster3_churn` | 3-node cluster + restarts | ~13 |
| `auth` | 1 node with auth | ~2 |
| `ssl` | 1 node with TLS/mTLS | ~4 |

### Fixtures (`tests/common/fixtures.rs`)

Auto-provisions Docker containers via `bollard` when env vars are unset. Key profiles: `DefaultSingleNode`, `ThreeNodeCluster`, `ThreeNodeClusterChurn`, `AuthSingleNode`, `TlsSingleNode`, `MtlsSingleNode`.

Use `ignite_scope(IgniteProfile::...)` and `connect_profile(...)` in new tests.

### Environment Variables for External Environments

- `IGNITE_ADDR` — single-node address
- `IGNITE_3NODE_ADDRS` — comma-separated cluster addresses
- `IGNITE_AUTH_ADDR` / `IGNITE_AUTH_USERNAME` / `IGNITE_AUTH_PASSWORD` — auth node
- `IGNITE_TLS_ADDR` / `IGNITE_TLS_SERVER_NAME` / `IGNITE_TLS_CA_PEM` — TLS node
- `IGNITE_TEST_IMAGE` / `IGNITE_TEST_TAG` — override fixture Docker image (default: `apacheignite/ignite:2.17.0-arm64`)

## Key Design Decisions

- **No dynamic dispatch**: performance-critical paths avoid `dyn Trait`; the crate is designed for use in the latency-sensitive `rsc-cache-rs` proxy.
- **Lock-free affinity**: `ArcSwap` instead of `RwLock` for the affinity hot path — recent optimization for throughput.
- **`Arc<str>` node IDs**: node identifiers use `Arc<str>` to eliminate per-request `String` clones.
- **Single async runtime**: Tokio only. The `rt-tokio` feature flag is a placeholder; there is no alternative runtime support.
- **Feature flag `ssl`**: enables `rustls`/`tokio-rustls`/`rustls-pemfile`. Not enabled by default.
