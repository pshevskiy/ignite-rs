//! Tier-3 byte-fixture round-trip — entry file.
//!
//! `read.rs` iterates the fixtures directory and asserts each `.bin`
//! decodes successfully; `write.rs` asserts that re-encoding the decoded
//! value produces the same bytes (round-trip). Both run in the `pure`
//! bucket — no live Ignite needed.

#[path = "byte_fixtures/read.rs"]
mod read;
#[path = "byte_fixtures/write.rs"]
mod write;
