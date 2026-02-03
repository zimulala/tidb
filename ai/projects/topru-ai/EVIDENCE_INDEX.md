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
| E4 | doc | Semantic coupling accepted: interval min policy documented; disabled=no-op documented. | e77f6de611501f8093e5df24ce58aa2c5a8be565 | 2026-01-30T07:21:16Z | local | edit docs/spec | ./TOPRU_SEMANTIC_SPEC.md |
| E1 | test | In-flight RU sampling correctness and begin-based ExecCount semantics under ticks/finish/toggles (including late-enable and RU=0 noise prevention), SQL/Plan meta registration when TopSQL is off and TopRU is on, and disabled=no-op RU output gating (housekeeping drain allowed, no emitted increments) in aggregator. | 13d8e8f5171ec60a154f1bdbfe2602bc625dac57 | 2026-02-03T02:27:45Z | local-macos (Go test) | `go test -tags=intest ./pkg/util/topsql/stmtstats -run 'TestOnExecutionBeginFinishRU|TestMergeRUIntoInFlightSamplingAndFinishDedup|TestMergeRUIntoHandlesRUResetAndNilRUDetails' -count=1`; `go test -tags=intest ./pkg/util/topsql/stmtstats -run 'TestExecCountBeginBased_LongRunningAcrossTicks|TestExecCountBeginBased_ToggleMidExecution|TestExecCountBeginBased_RUZeroNoNoise|TestExecCountBeginBased_BucketMergeSameTick' -count=1`; `go test -tags=intest ./pkg/util/topsql/stmtstats -run 'TestAggregatorDisableAggregateRUNoEmit|TestAggregatorDisableAggregateRU' -count=1`; `go test -tags=intest ./pkg/executor -run TestObserveStmtBeginForTopSQL_RegisterSQLPlanMeta_WhenTopRUEnabledAndTopSQLDisabled -count=1`; `go test -tags=intest ./pkg/executor -run TestDoesNotExist -count=1` | ./artifacts/evidence/E1/stmtstats_inflight_sampling_test.log; ./artifacts/evidence/E1/f2_exec_count_semantics.log; ./artifacts/evidence/E1/f4_disabled_noop_aggregator.log; ./artifacts/evidence/E1/f3_topru_meta_registration.log; ./artifacts/evidence/E1/executor_compile_check.log |
| E2 | plan | (Planned) Same bucket accumulation correctness for same TopRU key; appears reliably in TopN output (no loss/overwrite/double-count). | e77f6de611501f8093e5df24ce58aa2c5a8be565 | 2026-01-30T07:21:16Z | local | TODO: go test ./... -run TestTopRUAccumulateSameBucketSameKey -count=1 | ./artifacts/evidence/E2/ |
| E3 | plan | (Planned) Overhead evaluation: 1s sampling + meta registration overhead under representative workload. | e77f6de611501f8093e5df24ce58aa2c5a8be565 | 2026-01-30T07:21:16Z | local | TODO: bench/profile command | ./artifacts/evidence/E3/ |
<!-- NAVIGATOR:END AUTO_EVIDENCE -->

## Notes (human)
- TODO
