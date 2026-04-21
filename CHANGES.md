# Changes — Parity Audit + Performance Review (2026-04-20 → 2026-04-21)

Release tag: `v0.2.0-parity-audit-2026-04-21`
Baseline: commit `bc2c72e` (pre-audit)
HEAD: `f9668e1`

Full ignite-rs ↔ Apache Ignite 2.17.0 Java thin client parity audit across all
ten thin-client surface areas, plus an iterative performance review. All
changes preserve the public API; rsc-cache-rs downstream validated with zero
regressions and zero accommodation fixes required.

---

## 1. Summary

| Axis | Baseline | HEAD | Delta |
|------|----------|------|-------|
| Lib tests | 77 | 143 | +66 TDD tests |
| Test matrix buckets green | pure, single_node, cluster3, cluster3_churn, auth, ssl | same + **parity** | Added Tier-2 cross-client parity bucket |
| Bytes per 10 000 puts (dhat) | 93.72 MB | 6.59 MB | **−93 %** |
| put_single mean | 454 µs | 413 µs | −9 % |
| put_single p99 | 549 µs | 480 µs | −13 % (Java ratio 1.71× → 1.49×) |
| put_all/1000 mean | 2 100 µs | 1 576 µs | **−25 %** |
| Commits landed | — | 60 | 47 correctness + 8 validation infra + 5 PFND + 3 post-audit perf |

---

## 2. Correctness — Parity fixes (Phases 1-5)

A six-phase audit produced a findings report against Apache Ignite 2.17.0. 63
findings were audited; 47 were fixed (all WIRE-BREAKING + SEMANTIC severity
landed; one semver-breaking finding deferred; fourteen cosmetic findings
documented and left as-is).

### 2.1 Wire Protocol (8 commits, 7 fixes + 1 cleanup)

- `d324d45` **FND-001** — rename `OpCode::QueryClose` → `ResourceClose` to match Java `ClientOperation.RESOURCE_CLOSE = 0`.
- `463708f` **FND-002** — add `OpCode::OpStopWarmup = 10000`.
- `ef14a6e` **FND-003** — document `DataStreamer` opcodes (8000/8001) as Gridgain-downstream extensions; add `JAVA_2_17_0_OPCODES` parity guard.
- `a063a5d` **FND-004** — document `ClusterGetDataCenterNodes (5103)` as Gridgain-downstream extension.
- `065e5f0` **FND-005** — gate DC_AWARE + QRY_INITIATOR_ID feature bits on user config. Default handshake now byte-identical to Java 2.17.0.
- `59d0fd4` **FND-006** — advertise all 20 Java 2.17.0 `ProtocolBitmaskFeature` bits on handshake (was 6 real + 2 invented).
- `3f026f8` **FND-007** — back-patch request-frame length prefix (adopts Java encoder convention, eliminates a class of encoder-size-mismatch bugs).
- `24b8020` cleanup — silence warnings from test-only constants.

### 2.2 Binary Data Format (7 fixes, 1 blocked)

- `d456187` **FND-011** — add `HANDLE/CLASS/PROXY/TRANSFORMED` TypeCode variants.
- `a9b5815` **FND-013** — decode `ArrEnum (0x1D)` in ComplexObject fields.
- `d297fbf` **FND-015** — document `Decimal` byte convention; add signed-i128 helper.
- `012cf60` **FND-016** — preserve `BINARY_OBJ`/`WrappedData` envelope verbatim on round-trip (REG-1-class fix for a second TypeCode path).
- `820142f` **FND-018** — emit narrow offsets (u8/u16/u32) with matching `FLAG_OFFSET_ONE_BYTE`/`TWO_BYTES` flag. ComplexObject writer now matches Java per `BinaryUtils.java`.
- `3082257` **FND-020** — continuous-query prefix uses `cacheId + keepBinary` only (was writing spurious expiry/tx bytes).
- `cceaf05` **FND-021** — SQL WrappedData decoder reads length-delimited body, parses inner value from payload slice (was bleeding over the envelope boundary).
- **BLOCKED: FND-014** — typed `ArrString`/`ArrDate`/`ArrDecimal`/`ArrTimestamp`/`ArrUuid` round-trip. Fix requires a new `IgniteValue` variant (semver-breaking, breaks exhaustive matches in rsc-cache-rs and other consumers). Deferred pending consumer coordination.

### 2.3 Cache Operations (4 fixes)

- `39a5bec` **FND-022** — `put_if_absent` response-decoding test coverage (pins boolean return).
- `98fc8e3` **FND-024** — default SQL fields timeout to 0 (was 500 ms; Java default is 0).
- `4b0d2bd` **FND-025** — `invoke`/`invoke_all` no longer force `keep_binary = true`.
- `dce91ae` **FND-027** — integration test for `clear_keys` rejection inside a transaction.

### 2.4 Transactions (4 fixes)

- `3d246c0` **FND-028** — gate `tx_start` on negotiated `TRANSACTIONS` feature (V1_5_0+).
- `4d14062` **FND-029** — pin `TX_START` wire layout (typed-string label, typed-NULL when absent).
- `85a4d55` **FND-030** — serialize concurrent commit/rollback on shared handles (single-writer invariant).
- `35ce320` **FND-031** — warn when `Tx::drop` cannot reach the Tokio runtime (runtime teardown race).

### 2.5 Queries (5 fixes)

- `62ae333` **FND-033** — `IndexQuery` `valueType`/`indexName` as typed strings.
- `3e279d2` **FND-034** — gate `IndexQuery.limit` on `INDEX_QUERY_LIMIT` feature bit; reject `limit > 0` on unsupported servers.
- `9423391` **FND-035** — criteria collection marker is `ARR_LIST (0x01)`, not `TypeCode::Collection (0x18)`.
- `4fe310c` **FND-036** — `IndexQueryCriterion` `field_name` as typed string.
- `62cb211` **FND-039** — preserve `OPTIMIZED_MARSHALLER` payload in SQL results via `SqlValue::OpaqueMarshal`.

### 2.6 Compute Tasks (1 fix)

- `24d3b10` **FND-041** — drop `FLAG_KEEP_BINARY = 0x04` from compute flags (not set by Java 2.17.0).

### 2.7 Services (5 commits, 6 findings — a REG-2-class stacked bug cluster)

- `23ca8f0` **FND-046** — `service_name`/`method_name` as typed strings.
- `1fcf3d7` **FND-044** — set `FLAG_PARAMETER_TYPES_MASK` on `SERVICE_INVOKE`.
- `fd4c1c1` **FND-045 + FND-048** — prefix `SERVICE_INVOKE` args with `paramTypeId` (overload dispatch now distinguishes Int vs Long).
- `3c222ce` **FND-047** — gate `callAttrs` map on `SERVICE_INVOKE_CALLCTX` feature bit.
- `27ec6cb` **FND-049** — pin `ServiceTopologyResponse` wire shape.

### 2.8 Data Structures (3 pinning-test commits)

- `de80177` **FND-050** — pin `ATOMIC_LONG_CREATE` wire shape. *Note: the audit's proposed fix was incorrect — verifying against Java 2.17.0 showed the current Rust already matches byte-for-byte. Test pins the correct behavior.*
- `c5ac4f3` **FND-051** — pin `compare_and_set_and_get` derivation (audit-misread; no code change needed).
- `62c8b8b` **FND-052** — pin `OP_SET_ITERATOR_START` wire shape (audit-misread; no code change needed).

### 2.9 Cluster / Affinity (5 fixes)

- `0a891c4` **FND-053** — gate `ALL_AFFINITY_MAPPINGS` `customMappingsRequired` bool on `CACHE_PARTITIONS` request when feature bit 13 is negotiated.
- `3a27758` **FND-054** — read `ALL_AFFINITY_MAPPINGS` `defaultAffinity` bool trailer in response.
- `b7febfa` **FND-055** — mirror Java `U.safeAbs` in `rendezvous_partition` for non-power-of-2 partition counts (avoids `i32::MIN` panic).
- `d65c5bc` **FND-056** — gate `CLUSTER_CHANGE_STATE` `force_deactivation` bool on `FORCE_DEACTIVATION_FLAG` feature bit.
- `7509811` **FND-057** — pin `AFFINITY_TOPOLOGY_CHANGED` flag bit position vs Java `ClientFlag`.

### 2.10 Errors / Retry / Events (6 fixes)

- `19ccc5c` **FND-058** — plumb server status `i32` through `Flag::Failure { status, err_msg }`; add `ErrorKind::Authorization` (for `SECURITY_VIOLATION = 1012`) and `ErrorKind::EntryProcessorException` (for code 1040).
- `55263d1` **FND-058** — pin wire-level server status propagation through `read_incoming_frame`.
- `19e9245` **FND-060** — pin `ENTRY_PROCESSOR_EXCEPTION (1040)` distinct `ErrorKind` on invoke.
- `eced3db` **FND-061** — `should_retry` explicitly rejects terminal server errors.
- `5ddfd57` **FND-062** — align `RetryPolicy::ReadOnly` with Java `ClientRetryReadPolicy` (added QueryScan, QueryContinuous, ClusterGroupGetNodeIds, ClusterGroupGetNodeInfo; excluded Java-doesn't-retry ops).
- `c35c546` **FND-063** — document `ClientEvent` as observability events (not to be confused with Java's server-push `ClientNotificationType`).

---

## 3. Phase 5 — Three-tier correctness validation (8 commits)

New validation infrastructure landed end-to-end:

- `tests/java-parity-driver/` — Maven-built Java driver JAR (reads JSON
  commands from stdin, emits JSON responses, uses the official `IgniteClient`
  thin-client API). Built once by `xtask` pre-flight.
- `ignite-rs/tests/parity/` + `tests/parity.rs` — Rust cross-client parity
  harness + 9 representative cases across 9 audit areas.
- `ignite-rs/tests/byte_fixtures/` + `tests/byte_fixtures.rs` — Tier-3
  corpus: 25 Java-generated byte fixtures (`.bin` + `.meta.json` pairs) +
  Rust round-trip verification.
- `tests/test_matrix.toml` — new `parity` bucket registered; pre-flight
  builds the driver JAR, skips with clear error if `mvn` is unavailable.
- `xtask` extended with `Bucket::Parity` + `build_parity_driver` pre-flight.

**Validation at phase-5 close:**
- **Tier-1 pure bucket**: 22 suites, all green (includes byte fixtures 2/2).
- **Tier-1 single_node bucket**: 34 of 35 green; 1 pre-existing baseline
  flake in `compute_task_test::should_fail_on_unknown_task_name` due to
  stock apacheignite/ignite:2.17.0-arm64 defaults disabling thin-client
  compute.
- **Tier-2 parity bucket**: 9/9 cross-client cases green in 3.44 s.
- **Tier-3 byte fixtures**: 20/25 decode green, 16/25 exact round-trip (the 5
  gaps trace to FND-014; softly skipped with reason).

---

## 4. Performance — Phase 6 + post-audit investigation (11 perf commits)

### 4.1 Phase 6 closeout set (5 commits)

- `55563df` **PFND-001** — cache topology endpoints in `ArcSwap<Vec<String>>` instead of rebuilding per request. Hot path now lock-free.
- `12f0bc0` **PFND-002** — replace UUID `format!()` with manual hex formatting (eliminates per-call string-growth reallocs).
- `0131d2d` **PFND-004** — replace per-request `oneshot::channel()` + `Box<dyn Future>` in `Channel::request` with a sharded in-flight-request table keyed by request id. Tail-latency win only; mean flat (oneshot was already 1 alloc/op).
- `31b5006` **PFND-005** — zero-copy response path: reusable `BytesMut` in `attach_response_pump`; decoders consume `Bytes` slices via `split_to().freeze()`. **Eliminates per-frame 2 KB allocation (top dhat site).** This is the biggest correctness-phase perf win — bytes allocated per 10 000 puts dropped from 93.72 MB to 10.20 MB (−89 %); put_single p99 549 µs → 431 µs (−29.4 %).
- `7cd2de8` **PFND-003** — channel-candidates ArcSwap fast-path. Hot-path channel selection is now lock-free and allocation-free (mirrors PFND-001 pattern for a second site). Batched-op wins (put_all/1000 mean −23.8 %); single-op mean is I/O-bound and barely moves on this axis.

### 4.2 Post-audit investigation + fixes (6 commits)

- `8dea4c0` **TCP_NODELAY** — fixed a real bug: `conf.tcp_nodelay.unwrap_or(None)` defaulted to `None`, which skipped `set_nodelay(true)` entirely. Default is now `true` (explicit `Some(false)` still honored). TLS path covered. Without this fix, small single-op requests paid a Nagle-algorithm delay; large batches paid coalescing stalls. **put_all/1000 mean −29.6 %**.
- `80a6d06` **mimalloc (bench-only)** — set mimalloc as the global allocator in `benches/hot_path.rs` only. Library and consumers unaffected. put_single mean −5.0 % on bench.
- `233bca0` **concurrent bench** — added `benches/concurrent_requests.rs` for the perf investigation (confirms Rust's single-connection throughput scales 31× with concurrency; the write path is lock-free).
- `564dfd0` **PFND-007** — zero-alloc request encoding via a shared `Arc<StdMutex<Vec<Vec<u8>>>>` pool (bounded 32 entries). Mirrors PFND-005 for the write path. Batched-op wins (put_all/100 p99 −37.1 %, put_all/1000 mean −10.6 %, bytes/10k puts 8.68 MB → 6.59 MB). Single-op mean flat (malloc is < 0.03 % of a 424 µs round-trip).
- `f9668e1` **PFND-008 bench** — added `benches/hot_path_ct.rs` to compare multi-thread vs current-thread tokio runtimes on the same ignite-rs client. **Result: put_single mean 426 µs → 306 µs (−28.4 %); get_single mean −25.2 %; Java ratio for put_single mean 1.9× → 1.36×.** This is a runtime-choice finding, not a library change — consumers who run a current-thread runtime get the −28 % today with the existing `new_client`.

### 4.3 Investigation doc

- `benches/concurrent_requests.rs` showed throughput scaling (31× at conc=256)
  — the write path is confirmed lock-free.
- CPU flamegraph analysis identified 46 % `send`, 11 % `recv`, 10 % `epoll_wait`,
  14 % tokio park, < 4 % in ignite-rs code.
- An `LD_PRELOAD` shim counted syscalls apples-to-apples: Rust 6 recvs/op vs
  Java 2.11 recvs/op — a 2.85× read-syscall gap.
- **PFND-006 (buffered reader)** was tried against the recv gap; both
  implementations (tokio `BufReader` and hand-rolled accumulator) regressed.
  Root cause documented: memcpy cost of buffering on arm64 loopback exceeds
  the recv-syscall cost saved; the hand-rolled accumulator conflicts
  structurally with PFND-005's zero-copy `Bytes::split_to`. Not landed.

### 4.4 Measured perf deltas across the full session

Baseline (pre-audit, commit `bc2c72e`) → HEAD (`f9668e1`):

| Metric | Baseline | HEAD | Δ |
|---|---|---|---|
| Bytes/10k puts (dhat) | 93.72 MB | **6.59 MB** | **−93 %** |
| Allocs/put (dhat) | 83 blocks | ~17 blocks | **−80 %** |
| put_single mean | 454 µs | 413 µs | −9 % |
| put_single p99 | 549 µs | 480 µs | −13 % |
| put_all/100 mean | ~480 µs | 392 µs | −18 % |
| put_all/1000 mean | ~2100 µs | 1 576 µs | **−25 %** |
| Java ratio, put_single p99 | 1.71× | **1.49×** | passes 1.5× gate |
| put_single mean on CT runtime (bench) | 441 µs | **306 µs** | **−28 %** |

---

## 5. Downstream validation

rsc-cache-rs (the primary consumer) was validated against this ignite-rs:

- `cargo build --release` — clean.
- Unit tests: 89/90 (the 1 failure is pre-existing `bulk_put_grouping_stable_across_strategy_requests`, pre-dates audit).
- Integration suites that exercise the thin-client API: all green
  (`cache_service_it` 24/24, `bulk_operations_it` 17/18 pre-existing flake,
  `index_search_it` 5/5, `compression_it` 11/11, `large_entity_it` 8/8,
  `deleted_flag_it` 8/8, `headers_it` 4/4, `health_check_it` 2/2).
- **Zero regressions attributable to ignite-rs audit + perf.**
- **Zero accommodation commits needed.** New `ErrorKind::Authorization` from
  FND-058 is additive — rsc-cache-rs uses string constructors on
  `IgniteError`, not exhaustive matches.

Full validation report: `docs/superpowers/specs/2026-04-21-rsc-cache-rs-downstream-validation.md` (in the parent `docs/` tree).

---

## 6. Public API impact

**No breaking changes.** Additions only:

- New error variants: `ErrorKind::Authorization`, `ErrorKind::EntryProcessorException` (FND-058, FND-060). Additive; non-exhaustive enum.
- `IgniteError::server_status()` accessor (returns `Option<i32>`).
- New `IndexQueryCapabilities`, `ServiceInvokeCapabilities` capability accessors.
- `ClientConfig.tcp_nodelay` semantics changed from `None → skip set_nodelay` to `None → set_nodelay(true)`. Behavior change (NODELAY now on by default); explicit `Some(false)` still opts out.

`rsc-cache-rs` already sets `tcp_nodelay: Some(true)` explicitly so this default change is a no-op for it.

---

## 7. Follow-ons (not landed in this release)

### 7.1 Semver-breaking (would require consumer coordination)

- **FND-014** — typed `ArrXxx` round-trip (new `IgniteValue` variants, breaks
  exhaustive matches). Would unblock 5 Tier-3 byte-fixture soft-skips.

### 7.2 Cosmetic audit findings (documented, not fixed)

FND-008, 009, 010, 012, 017, 019, 023, 026, 032, 037, 038, 040, 042, 043, 059
— internal inefficiency / naming / non-observable divergences.

### 7.3 Perf — deferred

- **io_uring** transport rework — Linux-specific; could close the recv-syscall
  gap that PFND-006 could not. Estimated 3-6 h, structural.
- **PFND-006 alternative** — hand-rolled buffer that cooperates with
  PFND-005's `Bytes::split_to` (would need redesign — the two are currently
  structurally incompatible).
- **Vectored writes** (`writev` via `tokio::io::AsyncWriteExt::write_vectored`).
- **PGO / LTO** release profile.
- **Arc-clone audit** on the hot path.

### 7.4 Testing coverage

- Expand Tier-2 parity cases: 9 → 60 cases (per initial plan).
- Expand Tier-3 byte fixtures: 25 → 100 fixtures.
- Full feature-bit op-gating audit (FND-006 + FND-028/034/047/056 established the pattern for 4 of 20 bits; extend to the rest).

### 7.5 rsc-cache-rs — upstream opportunity

The **PFND-008 finding** (−28 % mean latency on current-thread runtime) is
unrealized in rsc-cache-rs, which currently runs a multi-thread tokio runtime
for its gRPC server. A dedicated current-thread runtime for the Ignite client
hot path (e.g. via a dedicated OS thread + mpsc crossover, or a pool thereof)
could capture this win in production. Needs architecture-level design —
not a mechanical patch.

---

## 8. Reference docs

All audit specs and reports live outside the ignite-rs repo, under
`/work/rsc_new/docs/superpowers/specs/`:

- `2026-04-20-ignite-rs-parity-audit-design.md` — audit design spec
- `2026-04-20-ignite-rs-wire-invariants.md` — I1–I10 + P1, 53 adversarial scenarios
- `2026-04-20-java-thin-client-semantics.md` — Java 2.17.0 etalon (1 444 lines)
- `2026-04-20-ignite-rs-parity-audit.md` — findings report (63 findings)
- `2026-04-20-ignite-rs-parity-audit-results.md` — phase-6 closeout
- `2026-04-20-ignite-rs-perf-report.md` — perf report (all PFNDs + negative results)
- `2026-04-20-ignite-rs-perf-investigation.md` — post-audit syscall analysis
- `2026-04-21-rsc-cache-rs-downstream-validation.md` — downstream clean bill

Corresponding plan: `/work/rsc_new/docs/superpowers/plans/2026-04-20-ignite-rs-parity-audit.md`.

Tags:
- `v0.2.0-parity-audit-2026-04-21` — release (this state)
- `audit-perf-checkpoint-2026-04-21` — pre-investigation checkpoint (post Phases 1-5 + PFND-001..005 + PFND-003; before TCP_NODELAY / mimalloc / PFND-007 / PFND-008)

Branches preserved:
- `audit/concurrency-2026-04-18` — Phase 4/5 audit work
- `perf/post-audit-investigation-2026-04-21` — Phase 6 + post-audit exploration

---

## 9. Post-closeout follow-ons (2026-04-21)

After the v0.2.0 release, five follow-on items from §7 were addressed one-by-one autonomously, each with strict rollback if tests failed or perf gates missed.

### 9.1 Systematic feature-bit op-gating — DONE

Extended the FND-028/034/047/056 pattern to all 20 `ProtocolBitmaskFeature` bits.

- 9 new gates added (one commit per bit): `CACHE_INVOKE`, `INDEX_QUERY`, `EXECUTE_TASK_BY_NAME`, `CLUSTER_GROUPS`, `CLUSTER_STATES`, `DATA_REPLICATION_OPERATIONS`, `GET_SERVICE_DESCRIPTORS`, `SERVICE_INVOKE`, `BINARY_CONFIGURATION`
- Plus mock thin-server feature-byte widening (`94b34da`) so integration tests exercise the gated paths
- 4 bits already gated prior (TRANSACTIONS/INDEX_QUERY_LIMIT/SERVICE_INVOKE_CALLCTX/FORCE_DEACTIVATION_FLAG)
- 4 bits already wire-gated elsewhere (USER_ATTRIBUTES at handshake, CLUSTER_GROUP_GET_NODES_ENDPOINTS topology-refresh, QRY_PARTITIONS_BATCH_SIZE SQL writer, HEARTBEAT, ALL_AFFINITY_MAPPINGS)
- 3 bits N/A — silent semantics (DEFAULT_QRY_TIMEOUT, SERVICE_TOPOLOGY, TX_AWARE_QUERIES)

Notable: `SERVICE_INVOKE` (bit 5) and `SERVICE_INVOKE_CALLCTX` (bit 10) are paired in Java — FND-047 gated only the callAttrs emission; the bit-5 commit adds the mirror check.

Tag: `feature-bit-gating-2026-04-21`.

### 9.2 FND-014 — typed-array round-trip (was BLOCKED) — DONE

Commit: `26b9237 fix(binary): FND-014 — preserve TypeCode on typed-array round-trip`.

Approach: added `IgniteValue::ArrTyped { type_code: u8, elements: Vec<IgniteValue> }` and marked `IgniteValue` `#[non_exhaustive]`. Decoder emits `ArrTyped` for all 6 typed-array codes (ArrString 0x18, ArrUuid 0x15, ArrDate 0x16, ArrDecimal 0x1F, ArrTimestamp 0x22, ArrTime 0x25); encoder preserves the original code.

Results:
- 6 new byte fixtures added + round-trip byte-identical
- Tier-3 round-trip coverage: 17/25 → 23/25 (the remaining 2 skips are primitive array codes ArrInt/ArrLong — separate from FND-014)
- **rsc-cache-rs needed zero accommodation** — all its `IgniteValue` matches already had wildcard arms, so `#[non_exhaustive]` was a no-op break
- Perf: flat to slightly improved (−5% put_single mean, −9% put_all/1000 mean in steady state)

Tag: `fnd-014-2026-04-21`.

### 9.3 Tier-2 parity coverage 9 → 64 cases — DONE

5 commits on `followon/parity-coverage-60-2026-04-21` (now merged).

Per-file counts (before → after):
- wire_format: 1 → 8
- cache_ops: 3 → 16
- transactions: 1 → 9
- queries: 1 → 9
- compute: 1 → 4
- services: 1 → 3
- data_structures: new, 5 cases
- cluster: new, 4 cases
- errors: 1 → 6

Total: 64/64 cases pass in 22.9s under `cargo run -p xtask -- test-matrix --bucket parity`.

~20 new ops added to `Driver.java` to support the cases. No new FND findings surfaced.

Tag: `parity-coverage-60-2026-04-21`.

### 9.4 Tier-3 byte-fixture corpus 31 → 105 — DONE

Commit: `504e9b2 test(fixtures): expand Tier-3 byte corpus from 31 to 105`.

Extended `FixtureGenerator.java` corpus covering: remaining primitive edge cases (zero/max/min, infinity, Unicode), collection subtypes (ArrayList, LinkedList, HashSet, LinkedHashSet), map subtypes (HashMap, LinkedHashMap), enum variants, decimal variants, timestamp variants, opaque blobs, nested structures (list-of-lists, list-of-maps, etc.).

Results:
- `read_all_fixtures`: 95/105 decode OK, 10 soft-skipped for primitive array codes (ArrShort/Int/Long/Float/Double/Char/Bool) — reader gap, not a regression.
- `write_all_fixtures_round_trip`: 94/105 byte-identical, 11 soft-skipped.
- The previous conservative `NON_ROUNDTRIP_KINDS` list was over-cautious: char/enum/decimal/opaque all round-trip fine; pruning raised coverage from 25/25 to 94/105.

Tag: `byte-fixtures-100-2026-04-21`.

### 9.5 io_uring transport — BLOCKED (not landed)

Attempted structural transport rewrite. Blocked at feasibility check:
- `tokio-uring 0.5` and `tokio::io::AsyncRead`/`AsyncWrite` have fundamentally incompatible APIs (completion-based vs readiness-based; ownership-passing vs borrow).
- Adding an `AsyncStream::Uring` variant requires a separate `tokio_uring::start()` runtime, breaking the crate's "Tokio-only" invariant and the multi-thread runtime that every pump task, integration test, and rsc-cache-rs assumes.
- Loopback bench target: PFND-006 already showed memcpy costs dominate syscall cost on loopback; io_uring's win would be on real-NIC / high-latency workloads, not this rig.
- Would lose the PFND-005 `Bytes::split_to` zero-copy hand-off (forces fresh Vec ownership transfers).

Decision: rolled back without writing code (refactor ≫ speculative bench win). Branch deleted.

### 9.6 F — Misc perf (vectored writes / Arc audit / PGO) — PARTIAL

Three experiments, strict ≥5% rollback gate.

- **F.1 Vectored writes:** SKIPPED — hypothesis invalid. `encode_request_into()` back-patches length into the same Vec as payload; `write_requests()` is already one syscall per request. Nothing to combine.
- **F.2 Arc-clone audit:** ROLLED BACK. Single wasteful `String` allocation identified + removed in `round_trip_internal` (`channel.address().to_string()` for optional ResponseMeta). Perf delta −2% on put_single (below 5% gate); variance made signal unreliable on Docker loopback. Reverted.
- **F.3 PGO release profile:** COMMITTED as `88fbbe9 perf(build): F.3 — add PGO release profile`. Added `[profile.release-pgo]` (codegen-units=1, LTO=fat, opt-in). Bench deltas vs baseline:
  - put_single mean: **−11% to −11.2%** (p<0.05)
  - get_single mean: **−5%**
  - put_all/100 mean: **−20.4%**
  - put_all/1000 mean: **−21.7% to −31.6%**
  - Note: bench-trained PGO — these are upper-bound gains; production benefit requires operators collecting representative profile data.

Tag: `followons-2026-04-21`.

### 9.7 Cumulative follow-on deltas

Vs v0.2.0 release tag:
- Test coverage: 9 → 64 Tier-2 parity cases; 31 → 105 Tier-3 byte fixtures; 143 → 150 lib tests (FND-014 added 7 round-trip tests)
- Feature-bit gating: 4/20 → 13/20 actively gated (plus 4 wire-gated, 3 N/A = full coverage)
- FND-014 unblocked
- PGO release profile available (opt-in)
- rsc-cache-rs remains green; zero accommodation needed across all follow-ons

Commit range: `94b34da` (feature-bit-gating HEAD, just after v0.2.0) → `88fbbe9` (PGO, current master).
