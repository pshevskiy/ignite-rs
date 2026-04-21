//! Tier-2 cross-client parity tests — entry file.
//!
//! Each submodule below adds `#[tokio::test]` functions that exercise one
//! audit area. The actual infra (driver subprocess client + shared helpers)
//! lives in `parity/driver_client.rs` + `parity/mod.rs`.

#[path = "parity/driver_client.rs"]
mod driver_client;
#[path = "parity/mod.rs"]
mod parity;

#[path = "common/mod.rs"]
mod common;

// Per-area cases — each file adds `#[tokio::test]` functions.
#[path = "parity/wire_format.rs"]
mod wire_format;
#[path = "parity/cache_ops.rs"]
mod cache_ops;
#[path = "parity/transactions.rs"]
mod transactions;
#[path = "parity/queries.rs"]
mod queries;
#[path = "parity/compute.rs"]
mod compute;
#[path = "parity/services.rs"]
mod services;
#[path = "parity/errors.rs"]
mod errors;
