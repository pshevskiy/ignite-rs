//! Phase 6 — dhat heap profiling on the put hot path.
//!
//! Run:
//!   IGNITE_ADDR=127.0.0.1:10800 \
//!     cargo run --release --manifest-path ignite-rs/Cargo.toml \
//!     --example dhat_hot_path
//!
//! Produces `dhat-heap.json` in the cwd. Open at https://nnethercote.github.io/dh_view/

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use ignite_rs::{new_client, ClientConfig};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let _prof = dhat::Profiler::new_heap();

    let addr = std::env::var("IGNITE_ADDR").unwrap_or_else(|_| "127.0.0.1:10800".to_string());
    let client = new_client(ClientConfig::new(&addr))
        .await
        .expect("connect ignite");
    let cache = client
        .get_or_create_cache::<String, String>("dhat_test")
        .await
        .expect("cache");

    for i in 0..10_000 {
        cache
            .put(&format!("k{i}"), &format!("v{i}"))
            .await
            .expect("put");
    }

    eprintln!("dhat_hot_path: 10_000 puts complete, profile written on drop");
}
