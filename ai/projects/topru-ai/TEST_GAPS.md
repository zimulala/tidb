# TEST_GAPS — TopRU (auto-generated)

## Snapshot
- project: topru-ai
- track: resource-observability-topru
- ssot_version: 2
- generated_at: 2026-02-03T14:01:27Z
- head_commit: 509dc996b32e6488b2c4709c99fa910f1ee1c855

## Coverage matrix (what exists vs what is missing)

### Functional (E_func)
- [x] In-flight sampling correctness (delta, no double count, reset/nil)
- [x] ExecCount begin-based semantics (pending-on-first-positive-delta)
- [x] Meta availability (TopSQL off + TopRU on)
- [x] Disabled=no-op (no RU output)
- [x] Query/TopN behaviors (sorting, share percent)  # if applicable in this repo scope
- [ ] Concurrency/edge cases: RU=0, user empty, <1s, session close vs tick

### Integration smoke (E_integ)
- [ ] End-to-end: generate TopRU records + meta in a realistic TiDB run (or harness)
- [ ] Verify records appear at sink (pubsub/single_target) and can be consumed
- [ ] Verify enable/disable behavior mid-exec in end-to-end environment

### Performance (E_perf)
- [ ] Sampling overhead under high cardinality (e.g., 1000 users × 500 active SQL)
- [ ] Aggregation/report overhead (CPU/alloc)
- [ ] Regression sanity for existing TopSQL paths

### Compatibility (E_compat)
- [ ] Resource Control on/off matrix
- [ ] Protobuf forward/backward compatibility expectations
- [ ] Coexist with existing TopSQL features; upgrade behavior unaffected

## Existing tests discovered

### Functional: stmtstats + enable/interval semantics
- pkg/util/topsql/state/state_test.go
  - TestTopRUEnableDisableAndResetInterval
  - TestTopRUReportIntervalSmallerPrevails
- pkg/util/topsql/stmtstats/stmtstats_test.go
  - TestOnExecutionBeginFinishRU
  - TestMergeRUIntoInFlightSamplingAndFinishDedup
  - TestMergeRUIntoHandlesRUResetAndNilRUDetails
  - TestExecCountBeginBased_LongRunningAcrossTicks
  - TestExecCountBeginBased_ToggleMidExecution
  - TestExecCountBeginBased_RUZeroNoNoise
  - TestExecCountBeginBased_BucketMergeSameTick
- pkg/util/topsql/stmtstats/aggregator_test.go
  - TestAggregatorDisableAggregateRU
  - TestAggregatorDisableAggregateRUNoEmit

### Functional: TopN / data model / sink toggles
- pkg/util/topsql/reporter/ru_datamodel_test.go
  - TestRURecordsTopN
  - TestUserRUCollectingTopNSQLs
  - TestUserRUCollectingPreTopNSQLCap
  - TestRUCollectingHybridTopN
  - TestRUCollectingPreTopNUserCap
  - TestRUCollectingSameBucketSameKeyAccumulates
- pkg/util/topsql/reporter/reporter_test.go
  - TestEffectiveReportIntervalSeconds_TopSQLIndependentFromTopRU
  - TestCollectAndTopN
- pkg/util/topsql/reporter/pubsub_test.go
  - TestPubSubDataSinkEnableTopRU
  - TestSubscribeRegisterFailDoesNotEnableTopRU
  - TestNormalizeTopRUReportIntervalInvalid

### Meta availability (executor)
- pkg/executor/adapter_internal_test.go
  - TestObserveStmtBeginForTopSQL_RegisterSQLPlanMeta_WhenTopRUEnabledAndTopSQLDisabled

### Performance-related (benchmarks exist, but not TopRU-specific)
- pkg/util/topsql/reporter/reporter_test.go
  - BenchmarkTopSQL_CollectAndIncrementFrequency
  - BenchmarkTopSQL_CollectAndEvict

## Missing / weak areas (ranked)
1. Integration smoke (true end-to-end TiDB run): prove TopRU record+meta produced and observable from sink (E_integ).
2. Perf sanity under high cardinality / high QPS: confirm overhead is acceptable and no regression in TopSQL path (E_perf).
3. Compat matrix: Resource Control on/off, enable/disable toggles, upgrade expectations (E_compat).

## Proposed next_actions (max 3)
1. verify/e_integ_smoke.sh — implement minimal end-to-end harness (produces: E_integ)
2. verify/e_perf_sanity.sh — implement high-cardinality/perf sanity recipe (produces: E_perf)
3. verify/e_compat_matrix.sh — implement compat matrix runner + checklist (produces: E_compat)

