pub(crate) mod cache_config;
pub(crate) mod key_value;

/// Opcodes defined in Java 2.17.0's `ClientOperation` table. Ops outside
/// this set are either Gridgain-downstream extensions (e.g. `DataStreamer*`
/// at 8000-8001, `ClusterGetDataCenterNodes` at 5103) or Rust-side
/// synthetic ops — stock 2.17.0 servers respond to them with
/// `INVALID_OP_CODE(2)`. Used by FND-003 / FND-004 parity guards to
/// distinguish extension ops from pure Java 2.17.0 ops.
#[cfg(test)]
pub(crate) const JAVA_2_17_0_OPCODES: &[i16] = &[
    0, // RESOURCE_CLOSE
    1, // HEARTBEAT
    2, // GET_IDLE_TIMEOUT
    1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, // cache kv
    1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020, // cache kv cont.
    1022, 1023, 1024, 1025, 1050, 1051, 1052, 1053, 1054, 1055, 1056, // cache ops
    1101, // CACHE_PARTITIONS
    2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, // queries
    3000, 3001, 3002, 3003, 3004, // binary type
    4000, 4001, // tx
    5000, 5001, 5002, 5003, 5100, 5101, 5102, // cluster (5103 is Gridgain-downstream)
    6000, 6001, // compute
    7000, 7001, 7002, 7003, // services
    9000, 9001, 9002, 9003, 9004, 9005, 9006, 9007, // atomic long
    9010, 9011, 9012, 9013, 9014, 9015, 9016, 9017, 9018, 9019, 9020, 9021, 9022, 9023, // set
    10000, // OP_STOP_WARMUP
];

#[derive(Debug, Copy, Clone)]
#[allow(dead_code)] // Some variants (e.g. OpStopWarmup) exist for Java 2.17.0 opcode-table parity.
pub(crate) enum OpCode {
    Heartbeat = 1,
    GetIdleTimeout = 2,
    //cache configuration
    CacheGetNames = 1050,
    CacheCreateWithName = 1051,
    CacheGetOrCreateWithName = 1052,
    CacheCreateWithConfiguration = 1053,
    CacheGetOrCreateWithConfiguration = 1054,
    CacheGetConfiguration = 1055,
    CacheDestroy = 1056,
    // key-value
    CacheGet = 1000,
    CachePut = 1001,
    CachePutIfAbsent = 1002,
    CacheGetAll = 1003,
    CachePutAll = 1004,
    CacheGetAndPut = 1005,
    CacheGetAndReplace = 1006,
    CacheGetAndRemove = 1007,
    CacheGetAndPutIfAbsent = 1008,
    CacheReplace = 1009,
    CacheReplaceIfEquals = 1010,
    CacheContainsKey = 1011,
    CacheContainsKeys = 1012,
    CacheClear = 1013,
    CacheClearKey = 1014,
    CacheClearKeys = 1015,
    CacheRemoveKey = 1016,
    CacheRemoveIfEquals = 1017,
    CacheRemoveKeys = 1018,
    CacheRemoveAll = 1019,
    CacheGetSize = 1020,
    CachePutAllConflict = 1022,
    CacheRemoveAllConflict = 1023,
    CacheInvoke = 1024,
    CacheInvokeAll = 1025,
    CachePartitions = 1101,
    // sql & scan queries - https://ignite.apache.org/docs/latest/binary-client-protocol/sql-and-scan-queries
    QueryScan = 2000,
    QueryScanCursorGetPage = 2001,
    QuerySql = 2002,
    QuerySqlCursorGetPage = 2003,
    /// Java `ClientOperation.RESOURCE_CLOSE=0@2.17.0` — closes any resource
    /// by id (cursor, compute task, continuous query, set iterator).
    ResourceClose = 0,
    // SQL fields query
    QuerySqlFields = 2004,
    QuerySqlFieldsCursorGetPage = 2005,
    QueryContinuous = 2006,
    QueryContinuousEvent = 2007,
    QueryIndex = 2008,
    QueryIndexCursorGetPage = 2009,
    TxStart = 4000,
    TxEnd = 4001,
    GetBinaryTypeName = 3000,
    RegisterBinaryTypeName = 3001,
    GetBinaryType = 3002,
    PutBinaryType = 3003,
    GetBinaryConfiguration = 3004,
    ClusterGetState = 5000,
    ClusterChangeState = 5001,
    ClusterChangeWalState = 5002,
    ClusterGetWalState = 5003,
    ClusterGroupGetNodeIds = 5100,
    ClusterGroupGetNodeInfo = 5101,
    ClusterGroupGetNodeEndpoints = 5102,
    /// Gridgain-downstream extension (not in Java 2.17.0's cluster-group op
    /// range 5100-5102). Issued only when the DC-aware feature bit (22) was
    /// negotiated in the handshake — see FEATURE_DC_AWARE gating in
    /// `transport.rs::refresh_dc_nodes` and `connection_async.rs` FND-005.
    /// A stock 2.17.0 server responds with `INVALID_OP_CODE(2)`.
    ClusterGetDataCenterNodes = 5103,
    ComputeTaskExecute = 6000,
    ComputeTaskFinished = 6001,
    ServiceInvoke = 7000,
    ServiceGetDescriptors = 7001,
    ServiceGetDescriptor = 7002,
    ServiceGetTopology = 7003,
    /// Gridgain-downstream extension (not in Java 2.17.0's ClientOperation
    /// table which stops at OP_STOP_WARMUP=10000). A stock 2.17.0 server
    /// responds with `INVALID_OP_CODE(2)`; only downstream server builds
    /// with DataStreamer support accept this op.
    DataStreamerStart = 8000,
    /// Gridgain-downstream extension (see `DataStreamerStart`).
    DataStreamerAddData = 8001,
    AtomicLongCreate = 9000,
    AtomicLongRemove = 9001,
    AtomicLongExists = 9002,
    AtomicLongValueGet = 9003,
    AtomicLongValueAddAndGet = 9004,
    AtomicLongValueGetAndSet = 9005,
    AtomicLongValueCompareAndSet = 9006,
    AtomicLongValueCompareAndSetAndGet = 9007,
    SetGetOrCreate = 9010,
    SetClose = 9011,
    SetExists = 9012,
    SetValueAdd = 9013,
    SetValueAddAll = 9014,
    SetValueRemove = 9015,
    SetValueRemoveAll = 9016,
    SetValueContains = 9017,
    SetValueContainsAll = 9018,
    SetValueRetainAll = 9019,
    SetSize = 9020,
    SetClear = 9021,
    SetIteratorStart = 9022,
    SetIteratorGetPage = 9023,
    /// Java `ClientOperation.OP_STOP_WARMUP=10000@2.17.0` — cluster warmup stop op.
    OpStopWarmup = 10000,
}

impl Into<i16> for OpCode {
    fn into(self) -> i16 {
        self as i16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FND-001: Op 0 is `RESOURCE_CLOSE` in Java 2.17.0
    /// (`ClientOperation.RESOURCE_CLOSE=0`), not a query-specific close.
    /// The Rust name must match Java's `RESOURCE_CLOSE` to prevent downstream
    /// mis-usage (cursor close, compute task cancel, continuous query close,
    /// set iterator close all use this op).
    #[test]
    fn opcode_resource_close_matches_java_2_17_0() {
        let code: i16 = OpCode::ResourceClose.into();
        assert_eq!(code, 0, "Java RESOURCE_CLOSE=0");
    }

    /// FND-002: Java 2.17.0 defines `OP_STOP_WARMUP = 10000`
    /// (`ClientOperation.OP_STOP_WARMUP(10000)`). Rust must expose the
    /// same op for parity with the Java opcode table.
    #[test]
    fn opcode_stop_warmup_matches_java_2_17_0() {
        let code: i16 = OpCode::OpStopWarmup.into();
        assert_eq!(code, 10000, "Java OP_STOP_WARMUP=10000");
    }

    /// FND-003: `DataStreamerStart` (8000) and `DataStreamerAddData` (8001)
    /// are Gridgain-downstream extensions, not in Java 2.17.0. Stock 2.17.0
    /// servers respond with `INVALID_OP_CODE(2)`. Pin these opcodes out of
    /// the Java 2.17.0 set so future contributors see they're non-standard.
    #[test]
    fn opcode_data_streamer_is_not_java_2_17_0() {
        let start: i16 = OpCode::DataStreamerStart.into();
        let add_data: i16 = OpCode::DataStreamerAddData.into();
        assert!(
            !JAVA_2_17_0_OPCODES.contains(&start),
            "DataStreamerStart (8000) is a Gridgain-downstream extension, not Java 2.17.0"
        );
        assert!(
            !JAVA_2_17_0_OPCODES.contains(&add_data),
            "DataStreamerAddData (8001) is a Gridgain-downstream extension, not Java 2.17.0"
        );
    }

    /// FND-004: `ClusterGetDataCenterNodes` (5103) is a Gridgain-downstream
    /// extension, not in Java 2.17.0 (cluster-group ops stop at 5102).
    #[test]
    fn opcode_cluster_get_dc_nodes_is_not_java_2_17_0() {
        let code: i16 = OpCode::ClusterGetDataCenterNodes.into();
        assert!(
            !JAVA_2_17_0_OPCODES.contains(&code),
            "ClusterGetDataCenterNodes (5103) is a Gridgain-downstream extension, not Java 2.17.0"
        );
    }

    /// FND-002 continued: OP_STOP_WARMUP=10000 IS in the Java 2.17.0 set.
    #[test]
    fn opcode_stop_warmup_is_in_java_2_17_0_set() {
        let code: i16 = OpCode::OpStopWarmup.into();
        assert!(
            JAVA_2_17_0_OPCODES.contains(&code),
            "OP_STOP_WARMUP=10000 is in Java 2.17.0"
        );
    }
}
