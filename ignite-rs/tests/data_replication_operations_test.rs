#![cfg(not(feature = "ssl"))]

mod common;

use ignite_rs::replication::{CacheVersion, ConflictEntry, EXPIRE_TIME_ETERNAL};

/// Related Apache Ignite replication version packing coverage:
/// org.apache.ignite.internal.client.thin.DataReplicationOperationsTest
#[test]
fn should_pack_node_order_and_data_center_id_into_cache_version() {
    let version = CacheVersion::new(4, 5, 6, 1);

    assert_eq!(version.topology_version(), 4);
    assert_eq!(version.order(), 5);
    assert_eq!(version.node_order(), 6);
    assert_eq!(version.data_center_id(), 1);
    assert_eq!(version.node_order_dr_id(), 134_217_734);
}

/// Related Apache Ignite conflict-entry expiry coverage:
/// org.apache.ignite.internal.client.thin.DataReplicationOperationsTest
#[test]
fn should_default_conflict_entry_expiry_to_eternal_and_allow_override() {
    let version = CacheVersion::new(1, 2, 3, 0);
    let default_entry = ConflictEntry::new(10, version.clone());
    let overridden = ConflictEntry::new(20, version).with_expire_time(123_456);

    assert_eq!(default_entry.expire_time, EXPIRE_TIME_ETERNAL);
    assert_eq!(overridden.expire_time, 123_456);
}
