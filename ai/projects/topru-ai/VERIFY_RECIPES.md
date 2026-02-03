# VERIFY_RECIPES

- E1 (functional correctness): intest suites
  - command: `go test -tags=intest ./pkg/util/topsql/stmtstats -run 'TestOnExecutionBeginFinishRU|TestMergeRUIntoInFlightSamplingAndFinishDedup|TestMergeRUIntoHandlesRUResetAndNilRUDetails' -count=1`; `go test -tags=intest ./pkg/util/topsql/stmtstats -run 'TestExecCountBeginBased_LongRunningAcrossTicks|TestExecCountBeginBased_ToggleMidExecution|TestExecCountBeginBased_RUZeroNoNoise|TestExecCountBeginBased_BucketMergeSameTick' -count=1`; `go test -tags=intest ./pkg/util/topsql/stmtstats -run 'TestAggregatorDisableAggregateRUNoEmit|TestAggregatorDisableAggregateRU' -count=1`; `go test -tags=intest ./pkg/util/topsql/reporter -run TestEffectiveReportIntervalSeconds_TopSQLIndependentFromTopRU -count=1`; `go test -tags=intest ./pkg/executor -run TestObserveStmtBeginForTopSQL_RegisterSQLPlanMeta_WhenTopRUEnabledAndTopSQLDisabled -count=1`
  - artifact: artifacts/evidence/E1/run.log

- E3 (perf sanity): bench/profile
  - command: `go test -tags=intest ./pkg/util/topsql/reporter -run TestEffectiveReportIntervalSeconds_TopSQLIndependentFromTopRU -count=50`
  - artifact: artifacts/evidence/E3/run.log

- E_integ (integration smoke): end-to-end record+meta
  - command: `./ai/projects/topru-ai/verify/e_integ_smoke.sh`
  - artifact: artifacts/evidence/E_integ/run.log

- E_perf (perf sanity): high-cardinality overhead
  - command: `./ai/projects/topru-ai/verify/e_perf_sanity.sh`
  - artifact: artifacts/evidence/E_perf/run.log

- E_compat (compat matrix): on/off combinations + upgrade expectations
  - command: `./ai/projects/topru-ai/verify/e_compat_matrix.sh`
  - artifact: artifacts/evidence/E_compat/run.log
