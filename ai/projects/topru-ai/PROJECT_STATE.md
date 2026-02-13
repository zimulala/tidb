# PROJECT_STATE

<!-- NAVIGATOR:BEGIN SSOT_V2 -->
version: 2
project: topru-ai
track: resource-observability-topru

policy:
  mode: safe
  proceed_required_for: [source_code_changes, destructive_fs, network]
  evidence:
    commit_bound: true
    record_patch_id: true
    artifact_root: ai/projects/topru-ai/artifacts/evidence

state:
  phase: IMPLEMENT
  current_commit: 509dc996b32e6488b2c4709c99fa910f1ee1c855
  last_updated: 2026-02-03T14:01:27Z

last_run: ai/ai-change-gates/runs/2026-02-13_run-123600Z-48ecf1a3df.json


claims:
  - id: C1
    legacy: [S1, A1]
    title: In-flight RU delta sampling (aggregator-tick cadence) with finish de-dup
    status: Verified
    evidence: [E1]
  - id: C2
    legacy: [S2, A2]
    title: Begin-based ExecCount with pending-on-first-positive-delta semantics
    status: Verified
    evidence: [E1]
  - id: C3
    legacy: [S3, A3]
    title: Register SQL/Plan meta when TopRU enabled even if TopSQL disabled
    status: Verified
    evidence: [E1]
  - id: C5
    revision: 2
    revision_note: "changed from coupled(min) -> separated tickers to avoid cross-feature cadence coupling"
    legacy: [S5, A4]
    title: TopSQL cadence independent from TopRU cadence (separate tickers)
    status: Accepted
    evidence: [E1, E3]


findings:
  - id: F1
    title: In-flight sampling missing / design mismatch
    status: Closed
    evidence: [E1]
  - id: F2
    title: ExecCount semantic decoupling risk
    status: Closed
    evidence: [E1]
  - id: F3
    title: Meta registration gated on TopSQL only
    status: Closed
    evidence: [E1]
  - id: F4
    title: Disabled behavior unclear
    status: Closed
    evidence: [E1]
  - id: F5
    title: Interval coupling semantics unclear
    status: Closed
    evidence: [E1]

evidence:
  # Track-level PR-ready evidence IDs (preferred)
  - id: E_func
    status: Captured
    type: test
    commit: "84e430afa290a8600ad135cddb53ca854ce096d8"
    patch_id: "2147c245005248f594f7a6318f50c244bdabc678"
    time: "2026-02-04T06:41:37Z"
    env: "Darwin/arm64 go1.25.6 darwin/arm64"
    command: "bash ai/projects/topru-ai/verify/e_func_smoke.sh (VERIFY_MODE=log, TIDB_LOG_PATH=/Users/xia/workspace/src/github.com/pingcap/tidb/tidb.log)"
    artifact: "ai/projects/topru-ai/artifacts/evidence/E_func/run.log"
  
  - id: E_integ
    status: Captured
    type: integ_smoke
    commit: "84e430afa290a8600ad135cddb53ca854ce096d8"
    patch_id: "2147c245005248f594f7a6318f50c244bdabc678"
    time: "2026-02-04T04:46:42Z"
    env: "Darwin/arm64 gogo1.25.6 darwin/arm64"
    command: "bash ai/projects/topru-ai/verify/e_integ_smoke.sh (VERIFY_MODE=log, TIDB_LOG_PATH=/Users/xia/workspace/src/github.com/pingcap/tidb/tidb.log, subscriber=127.0.0.1:10080)"
    artifact: "ai/projects/topru-ai/artifacts/evidence/E_integ/run.log"
  
  - id: E_perf
    status: Captured
    type: perf_sanity
    commit: "84e430afa290a8600ad135cddb53ca854ce096d8"
    patch_id: "2147c245005248f594f7a6318f50c244bdabc678"
    time: "2026-02-04T06:22:39Z"
    env: "Darwin/arm64 go1.25.6 darwin/arm64"
    command: "bash ai/projects/topru-ai/verify/e_perf_sanity.sh (PERF_N=50, PERF_MIN_QPS=0)"
    artifact: "ai/projects/topru-ai/artifacts/evidence/E_perf/run.log"
  
  - id: E_compat
    status: Planned
    type: compat_matrix
    commit: TODO_COMMIT
    patch_id: TODO_PATCH_ID
    time: TODO_TIME
    env: TODO_ENV
    command: TODO_CMD
    artifact: ai/projects/topru-ai/artifacts/evidence/E_compat/run.log

  - id: E_testgen_topru
    status: Captured
    type: go_test_gen
    commit: "2dafdd9e7d4b2b0dea212daeb1003b954fef10e6"
    patch_id: "cc231ab79812629ab285f3494e700791e0fc6386"
    time: "2026-02-05T03:19:17Z"
    env: "Darwin/arm64 go1.25.6 darwin/arm64"
    command: "BASE_REV=8c5df181ffc02e81133451433f7ebcbea0133a47; python3 ai/projects/topru-ai/testgen/generate_topru_cases.py --spec ai/projects/topru-ai/artifacts/evidence/E_testgen_topru/filtered_spec.yml --out pkg/util/topsql/reporter/topru_generated_cases_test.go; bash ai/projects/topru-ai/testgen/audit_goals.sh --spec ai/projects/topru-ai/artifacts/evidence/E_testgen_topru/filtered_spec.yml --gen pkg/util/topsql/reporter/topru_generated_cases_test.go --prefix TestTopRUGen --strict --no-extra; go test ./pkg/util/topsql/reporter -run 'TestTopRUGen' -count=1 -timeout 5m; flake_probe=0"
    artifact: "ai/projects/topru-ai/artifacts/evidence/E_testgen_topru/summary.md"
  
  # Legacy evidence IDs (optional; keep for continuity)
  - id: E1
    status: Captured
    type: test
    commit: TODO_COMMIT
    patch_id: TODO_PATCH_ID
    time: TODO_TIME
    env: TODO_ENV
    command: TODO_CMD
    artifact: ai/projects/topru-ai/artifacts/evidence/E1/run.log
  - id: E3
    status: Captured
    type: bench_or_profile
    commit: TODO_COMMIT
    patch_id: TODO_PATCH_ID
    time: TODO_TIME
    env: TODO_ENV
    command: TODO_CMD
    artifact: ai/projects/topru-ai/artifacts/evidence/E3/run.log

next_actions:
  - id: N1
    title: Harden integration smoke (end-to-end TopRU record+meta path)
    status: Proposed
    produces: [E_integ]
  - id: N2
    title: Add workload perf sanity (high cardinality sampling overhead)
    status: Proposed
    produces: [E_perf]
  - id: N3
    title: Add compat matrix smoke (Resource Control on/off + upgrade expectations)
    status: Proposed
    produces: [E_compat]

review:
  baseline_commit: "ba8b3d80cf7d5afecc821023a32e9e0363ad1faa"
  open: []
  fixed: []
  partially_fixed: []
  last_run:
    time: "2026-02-10T05:27:08Z"
    range: "ba8b3d80cf7d5afecc821023a32e9e0363ad1faa..ba8b3d80cf7d5afecc821023a32e9e0363ad1faa"
    artifacts_dir: "ai/projects/topru-ai/artifacts/review/20260210T052708Z_ba8b3d8_incr"
    review_md: "ai/projects/topru-ai/artifacts/review/20260210T052708Z_ba8b3d8_incr/review.md"
    findings_yaml: "ai/projects/topru-ai/artifacts/review/20260210T052708Z_ba8b3d8_incr/findings.yaml"
    next_actions: "ai/projects/topru-ai/artifacts/review/20260210T052708Z_ba8b3d8_incr/next_actions.md"
pr_ready:
  from_track: true
  status: true
  must: [E_func,E_integ]
  should: [E_perf]
  conditional: [E_compat]
  missing: []
  missing_must: []
  missing_should: []
  notes: "auto-updated by audit_ssot.sh"
<!-- NAVIGATOR:END SSOT_V2 -->
