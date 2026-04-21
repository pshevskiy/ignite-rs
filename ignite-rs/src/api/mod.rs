pub(crate) mod cache_config;
pub(crate) mod key_value;

#[derive(Debug, Copy, Clone)]
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
    ClusterGetDataCenterNodes = 5103,
    ComputeTaskExecute = 6000,
    ComputeTaskFinished = 6001,
    ServiceInvoke = 7000,
    ServiceGetDescriptors = 7001,
    ServiceGetDescriptor = 7002,
    ServiceGetTopology = 7003,
    DataStreamerStart = 8000,
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
}
