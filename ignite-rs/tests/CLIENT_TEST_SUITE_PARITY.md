# ClientTestSuite Parity

Status values:
- `migrated`: Rust suite file is live-Ignite-backed and matches the Java suite item directly.
- `partial`: Some behavior is covered, but the Java suite item is not matched feature-for-feature or still depends on mock-backed coverage.
- `blocked`: `ignite-rs` does not yet expose the subsystem/API needed to port the Java test.

Current status:
- `ClientConfigurationTest`: migrated
- `ClientCacheConfigurationTest`: migrated
- `ClientOrderedCollectionWarnTest`: partial
- `FunctionalTest`: partial
- `IgniteBinaryTest`: partial
- `LoadTest`: migrated
- `ReliabilityTest`: partial
- `SecurityTest`: partial
- `FunctionalQueryTest`: partial
- `IgniteBinaryQueryTest`: partial
- `SslParametersTest`: migrated
- `ConnectionTest`: migrated
- `ConnectToStartingNodeTest`: migrated
- `AsyncChannelTest`: migrated
- `ComputeTaskTest`: partial
- `ClusterApiTest`: partial
- `ClusterGroupTest`: partial
- `ServicesTest`: partial
- `ServicesBinaryArraysTests`: partial
- `ServiceAwarenessTest`: partial
- `CacheEntryListenersTest`: partial
- `ThinClientPartitionAwarenessStableTopologyTest`: partial
- `ThinClientPartitionAwarenessUnstableTopologyTest`: migrated
- `ThinClientPartitionAwarenessResourceReleaseTest`: partial
- `ThinClientPartitionAwarenessDiscoveryTest`: partial
- `ThinClientPartitionAwarenessBalancingTest`: partial
- `ThinClientPartitionAwarenessMultiDcTest`: partial
- `ThinClientNonTransactionalOperationsInTxTest`: partial
- `ReliableChannelTest`: partial
- `CacheAsyncTest`: migrated
- `TimeoutTest`: migrated
- `OptimizedMarshallerClassesCachedTest`: partial
- `AtomicLongTest`: partial
- `BinaryConfigurationTest`: partial
- `IgniteSetTest`: partial
- `DataReplicationOperationsTest`: partial
- `MetadataRegistrationTest`: partial
- `IgniteClientConnectionEventListenerTest`: partial
- `IgniteClientRequestEventListenerTest`: partial
- `IgniteClientLifecycleEventListenerTest`: migrated
- `ThinClientEnpointsDiscoveryTest`: partial
- `InactiveClusterCacheRequestTest`: partial
- `AffinityMetricsTest`: partial
- `ClusterGroupClusterRestartTest`: partial
- `BlockingTxOpsTest`: partial
- `InvokeTest`: partial
- `ExtraColumnInH2RowsTest`: migrated
- `RecoveryModeTest`: partial
- `ReliableChannelDuplicationTest`: partial
- `CacheExceptionsTest`: migrated

Layout notes:
- Legacy catch-all coverage in `tests/int-test.rs` has been redistributed into suite-shaped files.
- The fast non-Ignite smoke remains available as `tests/sanity_test.rs`.
- Non-TLS integration tests can auto-start one shared `apacheignite/ignite:2.15.0` container when `IGNITE_ADDR` is unset.
- The live fixture matrix supports plain single-node, plain 3-node, auth-enabled, TLS, mTLS, delayed-handshake, and cluster-churn profiles. It can be tuned with `IGNITE_TEST_IMAGE`, `IGNITE_TEST_TAG`, `IGNITE_TEST_CONTAINER_NAME`, `IGNITE_TEST_START_RETRIES`, and `IGNITE_TEST_START_DELAY_MS`.
- Under the live-only parity rule, suite files that still contain mock-backed Java-mapped tests are kept `partial` until those tests are moved to non-parity regression files or replaced with live-Ignite equivalents.

Notes on remaining `partial` entries:
- Many `partial` entries are temporarily downgraded because the suite file still mixes live-Ignite coverage with mock-backed Java-mapped tests. Those suites need the mock regressions moved out or replaced with live fixture coverage before they can return to `migrated`.
- The currently live-only migrated suites are `AsyncChannelTest`, `CacheAsyncTest`, `CacheExceptionsTest`, `ClientCacheConfigurationTest`, `ClientConfigurationTest`, `ConnectToStartingNodeTest`, `ConnectionTest`, `ExtraColumnInH2RowsTest`, `IgniteClientLifecycleEventListenerTest`, `LoadTest`, `SslParametersTest`, `ThinClientPartitionAwarenessUnstableTopologyTest`, and `TimeoutTest`.
- `ClientConfigurationTest` remains `migrated` because its uncovered Java method, `testRebalanceThreadPoolSize`, exercises embedded-node client mode rather than thin-client behavior. That case is intentionally out of scope for `ignite-rs`.
- `ClientCacheConfigurationTest` remains `migrated`; the suite file now only maps `org.apache.ignite.client.ClientCacheConfigurationTest` methods. Extra live tests in that file are retained as supplemental coverage without Java parity tags.
- `CacheAsyncTest` is now `migrated`: all `17` Java methods are covered live. The future-state and cancellation cases are matched through Tokio task state and cancellation, which is the Rust async analogue to Java `IgniteClientFuture` semantics.
- `ConnectToStartingNodeTest` is now `migrated`: it is covered live through a delayed listener proxy over the real single-node Ignite fixture, which preserves the Java observable behavior of retrying until the target endpoint becomes available without depending on brittle container stop/start timing.
- `ConnectionTest` is now `migrated`: all live-feasible Java methods are covered against a real Ignite node, including the large-handshake cases. The upstream Java IPv6 case remains intentionally out of scope here because it is already marked `@Ignore` in `org.apache.ignite.client.ConnectionTest`.
- `AsyncChannelTest` is now `migrated`: all three Java methods are covered live against a real multi-node Ignite fixture. The async-blocking case uses a real transactional lock from a second thin client to block `put(0)` while later requests on the shared client continue to complete, which matches the observable behavior asserted by the Java suite.
- `TimeoutTest` is now `migrated`: the two handshake timeout cases use the same raw-socket dummy-server shape as the Java suite, and the three server/operation timeout cases now pass live against the managed single-node Ignite fixture with the expected connector handshake timeout applied.
- `FunctionalQueryTest` is now live-only and its `ScanQuery`, `SqlQuery`, and `SqlFieldsQuery` paths all run against a real Ignite node. It remains `partial` because `FunctionalQueryTest#testQueryInitiatorId` still does not match Java behavior live: the server observes the default `cli:<addr>` initiator instead of the user-supplied `query_initiator_id`.
- `SecurityTest` is now live-only on the auth path and its invalid-auth, async invalid-auth, valid-auth, and non-admin `CREATE USER` cases pass against the managed auth fixture. It remains `partial` only because the mTLS leg is still noisy on this machine's managed TLS fixture path, so that last live parity check is not yet reliable enough to promote.
- `CacheExceptionsTest` is now `migrated`: its live missing-cache and tx-scoped cache-exception assertions pass against a real Ignite node.
- `ReliabilityTest` remains `partial`, but it is now broader and fully live-backed for the methods currently ported. `testFailover` now runs against the managed `cluster_3_churn` fixture with real cache operations, scan-query churn, and the all-nodes-down failure case; the suite is still missing the Java service-failover, server-critical-error, tx-id-intersection, and retry-policy conversion cases.
- `ReliableChannelTest` is now live-only, but it remains `partial` because the Java suite still has uncovered default-channel balancing and reinitialization cases.
- `ThinClientPartitionAwarenessStableTopologyTest` is now live-only and the currently ported nine Java methods pass against the managed 3-node fixture: `testReplicatedCache`, `testPartitionedCachePrimitiveKey`, `testPartitionedCache0Backups`, `testPartitionedCache1Backups`, `testPartitionedCache3Backups`, `testScanQuery`, `testIgniteSet`, `testIgniteSetCollocated`, and `testAtomicLong`. It remains `partial` because the Java suite still has unported custom-affinity, complex-key, node-filter, and cache-group routing permutations.
- `ThinClientPartitionAwarenessUnstableTopologyTest` is now `migrated`: all seven Java methods pass live against the managed churn fixture, including lower/same topology-version restarts and the handshake-close startup failure path. The affinity parser now also rejects insane partition-map values instead of resizing unboundedly when restart-time responses are malformed.
- `ClusterApiTest` is now mock-free for its current coverage: live cluster-state behavior runs against the managed single-node fixture, and the WAL request-shape assertions now live as unit tests in [cluster.rs](/Users/coloner/work/ignite_all/ignite-rs/ignite-rs/src/cluster.rs). It remains `partial` because the persistence-backed WAL behavior from the Java suite is not yet covered live.
- `DataReplicationOperationsTest` is now mock-free for its current coverage: public `CacheVersion` / `ConflictEntry` behavior remains in the suite file, and the request-encoding assertions moved into unit tests in [replication.rs](/Users/coloner/work/ignite_all/ignite-rs/ignite-rs/src/replication.rs). It remains `partial` because there is still no live thin-client replication fixture story.
- `IgniteBinaryTest`, `IgniteBinaryQueryTest`, and `OptimizedMarshallerClassesCachedTest` are now mock-free and run on the real single-node fixture for the currently ported behaviors. They remain `partial` because the Java suites still have uncovered binary-query and metadata permutations beyond the currently ported cases.
- `FunctionalTest`, `ReliabilityTest`, and `CacheEntryListenersTest` remain `partial` for true behavioral reasons as well: each has live coverage already, but the Java suite is not yet matched one-for-one across all methods.
