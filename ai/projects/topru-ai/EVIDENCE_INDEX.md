# EVIDENCE_INDEX

rules:
- evidence is commit-bound and immutable for review purposes
- each E# MUST include: commit, time, env, command, artifact_path
- if commit != PROJECT_STATE.current_commit (when known), evidence is stale ⇒ COLLECT_EVIDENCE
- do not close findings with stale/missing evidence
- Go tests must include `-tags=intest`

<!-- NAVIGATOR:BEGIN AUTO_EVIDENCE -->
| ID | Type | What it proves | Commit | Time | Env | Command | Artifact path |
|----|------|----------------|--------|------|-----|---------|---------------|
| E_TBD | plan | Evidence plan placeholder (will be replaced by real test/bench artifacts). | e77f6de611501f8093e5df24ce58aa2c5a8be565 | 2026-01-30T07:21:16Z | local | N/A (plan) | ./artifacts/evidence/ |
| E4 | doc  | Semantic coupling accepted: interval min policy documented; disabled=no-op documented. | e77f6de611501f8093e5df24ce58aa2c5a8be565 | 2026-01-30T07:21:16Z | local | edit docs/spec | ./TOPRU_SEMANTIC_SPEC.md |
| E1 | test | In-flight RU sampling correctness and begin-based ExecCount semantics under ticks/finish/toggles (including late-enable and RU=0 noise prevention), SQL/Plan meta registration when TopSQL is off and TopRU is on, disabled=no-op RU output gating (housekeeping drain allowed, no emitted increments) in aggregator, and interval independence (TopRU cadence does not affect TopSQL cadence). | 509dc996b32e6488b2c4709c99fa910f1ee1c855 | 2026-02-03T05:58:57Z | local-macos (Go test) | `go test -tags=intest ./pkg/util/topsql/stmtstats -run 'TestOnExecutionBeginFinishRU|TestMergeRUIntoInFlightSamplingAndFinishDedup|TestMergeRUIntoHandlesRUResetAndNilRUDetails' -count=1`; `go test -tags=intest ./pkg/util/topsql/stmtstats -run 'TestExecCountBeginBased_LongRunningAcrossTicks|TestExecCountBeginBased_ToggleMidExecution|TestExecCountBeginBased_RUZeroNoNoise|TestExecCountBeginBased_BucketMergeSameTick' -count=1`; `go test -tags=intest ./pkg/util/topsql/stmtstats -run 'TestAggregatorDisableAggregateRUNoEmit|TestAggregatorDisableAggregateRU' -count=1`; `go test -tags=intest ./pkg/util/topsql/reporter -run TestEffectiveReportIntervalSeconds_TopSQLIndependentFromTopRU -count=1`; `go test -tags=intest ./pkg/executor -run TestObserveStmtBeginForTopSQL_RegisterSQLPlanMeta_WhenTopRUEnabledAndTopSQLDisabled -count=1`; `go test -tags=intest ./pkg/executor -run TestDoesNotExist -count=1` | ./artifacts/evidence/E1/stmtstats_inflight_sampling_test.log; ./artifacts/evidence/E1/f2_exec_count_semantics.log; ./artifacts/evidence/E1/f4_disabled_noop_aggregator.log; ./artifacts/evidence/E1/f5_interval_separate_tickers.log; ./artifacts/evidence/E1/f3_topru_meta_registration.log; ./artifacts/evidence/E1/executor_compile_check.log |
| E2 | test | Same bucket/tick accumulation for identical TopRU key is merged (not overwritten), with correct RU/ExecDuration sum and begin-based ExecCount semantics (no double-count). | 509dc996b32e6488b2c4709c99fa910f1ee1c855 | 2026-02-03T07:17:03Z | local-macos (Go test) | `go test -tags=intest ./pkg/util/topsql/reporter -run TestRUCollectingSameBucketSameKeyAccumulates -count=1` | ./artifacts/evidence/E2/e2_same_bucket_same_key.log |
| E3 | profile | Overhead evaluation: 1s sampling + meta registration overhead under representative workload. | 509dc996b32e6488b2c4709c99fa910f1ee1c855 | 2026-01-30T07:21:16Z | local | go test -tags=intest ./pkg/util/topsql/reporter -run TestEffectiveReportIntervalSeconds_TopSQLIndependentFromTopRU -count=50 | ./artifacts/evidence/E3/ |
<!-- NAVIGATOR:END AUTO_EVIDENCE -->

## Notes (human)
- TODO
