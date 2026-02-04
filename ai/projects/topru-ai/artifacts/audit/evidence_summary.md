# TopRU Evidence Summary

Generated: 2026-02-04T06:44:35Z

## PR-ready audit
```
SSOT_AUDIT
project_root: ai/projects/topru-ai
track: resource-observability-topru
time_utc: 2026-02-04T06:41:38Z
pr_ready_status: true
missing_must: []
missing_should: []

NOTE: Run with --patch to write pr_ready fields back into SSOT.
```

## Evidence items
### E_func
```yaml
    status: Captured
    type: test
    commit: "84e430afa290a8600ad135cddb53ca854ce096d8"
    patch_id: "2147c245005248f594f7a6318f50c244bdabc678"
    time: "2026-02-04T06:41:37Z"
    env: "Darwin/arm64 go1.25.6 darwin/arm64"
    command: "bash ai/projects/topru-ai/verify/e_func_smoke.sh (VERIFY_MODE=log, TIDB_LOG_PATH=/Users/xia/workspace/src/github.com/pingcap/tidb/tidb.log)"
    artifact: "ai/projects/topru-ai/artifacts/evidence/E_func/run.log"
```

### E_integ
```yaml
    status: Captured
    type: integ_smoke
    commit: "84e430afa290a8600ad135cddb53ca854ce096d8"
    patch_id: "2147c245005248f594f7a6318f50c244bdabc678"
    time: "2026-02-04T04:46:42Z"
    env: "Darwin/arm64 gogo1.25.6 darwin/arm64"
    command: "bash ai/projects/topru-ai/verify/e_integ_smoke.sh (VERIFY_MODE=log, TIDB_LOG_PATH=/Users/xia/workspace/src/github.com/pingcap/tidb/tidb.log, subscriber=127.0.0.1:10080)"
    artifact: "ai/projects/topru-ai/artifacts/evidence/E_integ/run.log"
```

### E_perf
```yaml
    status: Captured
    type: perf_sanity
    commit: "84e430afa290a8600ad135cddb53ca854ce096d8"
    patch_id: "2147c245005248f594f7a6318f50c244bdabc678"
    time: "2026-02-04T06:22:39Z"
    env: "Darwin/arm64 go1.25.6 darwin/arm64"
    command: "bash ai/projects/topru-ai/verify/e_perf_sanity.sh (PERF_N=50, PERF_MIN_QPS=0)"
    artifact: "ai/projects/topru-ai/artifacts/evidence/E_perf/run.log"
```

## Local artifact locations
- E_func: ai/projects/topru-ai/artifacts/evidence/E_func/
- E_integ: ai/projects/topru-ai/artifacts/evidence/E_integ/
- E_perf: ai/projects/topru-ai/artifacts/evidence/E_perf/
