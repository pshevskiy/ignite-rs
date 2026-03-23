Apache Ignite thin client
====

## Usage

```rust
use ignite_rs::cache::Cache;
use ignite_rs::ClientConfig;
use ignite_rs_derive::IgniteObj;

#[tokio::main]
async fn main() -> ignite_rs::error::IgniteResult<()> {
    let client_config = ClientConfig::new("localhost:10800");
    let ignite = ignite_rs::new_client(client_config).await?;

    if let Ok(names) = ignite.get_cache_names().await {
        println!("ALL caches: {:?}", names);
    }

    let hello_cache: Cache<MyType, MyOtherType> = ignite
        .get_or_create_cache::<MyType, MyOtherType>("test")
        .await?;

    let key = MyType {
        bar: "AAAAA".into(),
        foo: 999,
    };
    let val = MyOtherType {
        list: vec![Some(FooBar {})],
        arr: vec![-23423423i64, -2342343242315i64],
    };

    hello_cache.put(&key, &val).await?;
    println!("{:?}", hello_cache.get(&key).await?);
    Ok(())
}

#[derive(IgniteObj, Clone, Debug)]
struct MyType {
    bar: String,
    foo: i32,
}

#[derive(IgniteObj, Clone, Debug)]
struct MyOtherType {
    list: Vec<Option<FooBar>>,
    arr: Vec<i64>,
}

#[derive(IgniteObj, Clone, Debug)]
struct FooBar {}
```

## TLS/TCP Helpers

```rust
use ignite_rs::client_config_from_ca_pem;

#[tokio::main]
async fn main() -> ignite_rs::error::IgniteResult<()> {
    let client_config = client_config_from_ca_pem(
        "localhost:10800",
        "./ca.pem",
        "mydomain.com",
    )?;

    let ignite = ignite_rs::new_client(client_config).await?;
    println!("{:?}", ignite.get_cache_names().await?);
    Ok(())
}
```

## Tests

- Run the full deterministic test matrix with `cargo run --manifest-path Cargo.toml -p xtask -- test-matrix`.
- Run one matrix bucket with `cargo run --manifest-path Cargo.toml -p xtask -- test-matrix --bucket <pure|single_node|cluster3|cluster3_churn|auth|ssl>`.
- `cargo test --manifest-path ignite-rs/Cargo.toml` remains useful for targeted debugging, but `xtask test-matrix` is the canonical runner for local full-suite and CI execution.
- Reuse existing environments with:
  - `IGNITE_ADDR` for plain single-node
  - `IGNITE_3NODE_ADDRS` for plain 3-node cluster
  - `IGNITE_AUTH_ADDR`, `IGNITE_AUTH_USERNAME`, and `IGNITE_AUTH_PASSWORD` for auth-enabled single-node
  - `IGNITE_TLS_ADDR`, `IGNITE_TLS_SERVER_NAME`, `IGNITE_TLS_CA_PEM`, and for mTLS also `IGNITE_TLS_CLIENT_CERT_PEM` plus `IGNITE_TLS_CLIENT_KEY_PEM`
  - `IGNITE_DELAYED_HANDSHAKE_ADDR` for the live delayed-handshake proxy test
- When those env vars are unset, the fixture can auto-provision plain, auth, TLS, and mTLS single-node containers plus the plain 3-node cluster.
- Prefer the profile-based fixture helpers in `ignite-rs/ignite-rs/tests/common/fixtures.rs`, such as `ignite_scope(IgniteProfile::...)` and `connect_profile(...)`, for any new live integration test.
- Override the managed fixture with `IGNITE_TEST_IMAGE`, `IGNITE_TEST_TAG`, `IGNITE_TEST_CONTAINER_NAME`, `IGNITE_TEST_START_RETRIES`, and `IGNITE_TEST_START_DELAY_MS` when CI needs different image sourcing or slower startup polling.
- Managed fixtures use a Docker-compatible API directly. Set `DOCKER_HOST` to point at a remote API endpoint, and `TESTCONTAINERS_HOST_OVERRIDE` if published ports should be reached through a different host than the Docker API address.

## Notes

- `Client` and `AsyncClient` are aliases of the same Tokio-backed client type.
- The public surface is async-first; there is no separate blocking/sync wrapper in the parity API.
- Event subscriptions are available through `client.events().subscribe()`.
- Continuous query listeners are available through `cache.continuous_query(...)`.
- Conflict replication helpers live in `ignite_rs::replication` and are sent through `cache.put_all_conflict(...)` / `cache.remove_all_conflict(...)`.

## Dev Tips

- Build: `cargo build --manifest-path ignite-rs/ignite-rs/Cargo.toml`
- Format: `cargo fmt --manifest-path ignite-rs/Cargo.toml --all`
- Bench compile: `cargo bench --manifest-path ignite-rs/ignite-rs/Cargo.toml --no-run`
- Example: `cargo run --manifest-path ignite-rs/example/Cargo.toml`

## Type Mapping

Here is the list of supported rust types with corresponding Ignite types and type codes
(https://apacheignite.readme.io/docs/binary-client-protocol-data-format)

Rust type|Ignite type|Ignite type code
---|---|---
u8|Byte|1
u16|Char|7
i16|Short|2
i32|Int|3
i64|Long|4
f32|Float|5
f64|Double|6
bool|Bool|8
ignite_rs::Enum|Enum|28
String|String|9
Vec\<u8>|ArrByte|12
Vec\<u16>|ArrChar|18
Vec\<i16>|ArrShort|13
Vec\<i32>|ArrInt|14
Vec\<i64>|ArrLong|15
Vec\<f32>|ArrFloat|16
Vec\<f64>|ArrDouble|17
Vec\<bool>|ArrBool|19
Vec\<Option\<T>> where T: WritableType + ReadableType|Ser => ArrObj; Deser => ArrObj or Collection|Ser => 23; Deser => 23 or 24
Option\<T> where T: WritableType + ReadableType|None => Null; Some => inner type|None => 101
User-defined struct|ComplexObj|103
