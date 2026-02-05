#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

source "ai/projects/topru-ai/verify/lib_oneclick.sh"
# This runner intentionally handles go test failures to still emit evidence,
# so disable lib_oneclick's ERR trap here.
trap - ERR

EID="E_testgen_topru"
ETYPE="go_test_gen"
PROJECT_ROOT="ai/projects/topru-ai"
STATE_FILE="${PROJECT_ROOT}/PROJECT_STATE.md"
ART_DIR="${PROJECT_ROOT}/artifacts/evidence/${EID}"
RUN_LOG="${ART_DIR}/run.log"
MANIFEST="${ART_DIR}/manifest.json"
SUMMARY="${ART_DIR}/summary.md"
PR_DIFF="${ART_DIR}/pr_diff.txt"
TESTS_LIST="${ART_DIR}/tests_list.txt"
GOALS_LIST="${ART_DIR}/goals_list.txt"
COVERAGE_MAP="${ART_DIR}/coverage_map.txt"
DEDUP_REPORT="${ART_DIR}/dedup_report.txt"
FILTERED_SPEC="${ART_DIR}/filtered_spec.yml"
INVENTORY_LIST="${ART_DIR}/inventory_tests_list.txt"
BUNDLE_ERR="${ART_DIR}/bundle.err"

SPEC_FILE="ai/projects/topru-ai/testgen/specs/topru_coverage_goals.yml"
MAPPING_FILE="ai/projects/topru-ai/testgen/specs/topru_goal_coverage.map"
GEN_SCRIPT="ai/projects/topru-ai/testgen/generate_topru_cases.py"
GEN_OUT="pkg/util/topsql/reporter/topru_generated_cases_test.go"
DEDUP_SCRIPT="ai/projects/topru-ai/testgen/dedup_filter.sh"
AUDIT_SCRIPT="ai/projects/topru-ai/testgen/audit_goals.sh"
BUNDLE_SCRIPT="ai/projects/topru-ai/testgen/audit_bundle.sh"

mkdir -p "$ART_DIR"
: > "$RUN_LOG"
: > "$BUNDLE_ERR"

[[ -f "$SPEC_FILE" ]] || die "spec file missing: $SPEC_FILE"
[[ -f "$GEN_SCRIPT" ]] || die "generator script missing: $GEN_SCRIPT"
[[ -f "$DEDUP_SCRIPT" ]] || die "dedup filter script missing: $DEDUP_SCRIPT"
[[ -f "$AUDIT_SCRIPT" ]] || die "audit script missing: $AUDIT_SCRIPT"
[[ -f "$BUNDLE_SCRIPT" ]] || die "bundle script missing: $BUNDLE_SCRIPT"

extract_run_prefix() {
  awk '
    /^go_test:[[:space:]]*$/ { in_go=1; next }
    in_go==1 && /^[^[:space:]]/ { in_go=0 }
    in_go==1 && /^[[:space:]]*run_prefix:[[:space:]]*/ {
      p=$0
      sub(/^[[:space:]]*run_prefix:[[:space:]]*/, "", p)
      gsub(/"/, "", p)
      sub(/[[:space:]]*$/, "", p)
      print p
      exit
    }
  ' "$SPEC_FILE"
}

BASE_REV="${BASE_REV:-}"
if [[ -z "$BASE_REV" ]]; then
  if git rev-parse --verify origin/master >/dev/null 2>&1; then
    BASE_REV="$(git merge-base HEAD origin/master || true)"
  fi
fi
if [[ -z "$BASE_REV" ]]; then
  if git rev-parse --verify origin/main >/dev/null 2>&1; then
    BASE_REV="$(git merge-base HEAD origin/main || true)"
  fi
fi
if [[ -z "$BASE_REV" ]]; then
  BASE_REV="$(git rev-parse HEAD~20 2>/dev/null || git rev-parse HEAD)"
fi

COUNT="${COUNT:-1}"
if [[ "${GATE_FLAKE_PROBE:-0}" == "1" ]]; then
  COUNT="10"
fi
RUN_PREFIX="$(extract_run_prefix)"
if [[ -z "$RUN_PREFIX" ]]; then
  RUN_PREFIX="TestTopRUGen"
fi

GEN_CMD="python3 ${GEN_SCRIPT} --spec ${FILTERED_SPEC} --out ${GEN_OUT}"
AUDIT_CMD="bash ${AUDIT_SCRIPT} --spec ${FILTERED_SPEC} --gen ${GEN_OUT} --prefix ${RUN_PREFIX} --strict --no-extra"
TEST_CMD_STR="go test ./pkg/util/topsql/reporter -run '${RUN_PREFIX}' -count=${COUNT} -timeout 5m"
ONECLICK_CMD_STR="BASE_REV=${BASE_REV} COUNT=${COUNT} GATE_FLAKE_PROBE=${GATE_FLAKE_PROBE:-0} bash ai/projects/topru-ai/testgen/run_testgen_oneclick.sh"
PIPELINE_CMD="BASE_REV=${BASE_REV}; ${GEN_CMD}; ${AUDIT_CMD}; ${TEST_CMD_STR}; flake_probe=${GATE_FLAKE_PROBE:-0}"

log "repo root: ${ROOT}"
log "base_rev: ${BASE_REV}"
log "run_prefix: ${RUN_PREFIX}"

# 1) Pre-gen dedup filter
bash "$DEDUP_SCRIPT" \
  --spec "$SPEC_FILE" \
  --gen "$GEN_OUT" \
  --pkg "pkg/util/topsql/reporter" \
  --base "$BASE_REV" \
  --out "$FILTERED_SPEC" \
  --out-dir "$ART_DIR" \
  --mapping "$MAPPING_FILE"

# 2) Generate tests from filtered spec
python3 "$GEN_SCRIPT" --spec "$FILTERED_SPEC" --out "$GEN_OUT"

# 3) Format generated file
if command -v goimports >/dev/null 2>&1; then
  goimports -w "$GEN_OUT"
else
  gofmt -w "$GEN_OUT"
fi

# 4) Strict audit gate
bash "$AUDIT_SCRIPT" \
  --spec "$FILTERED_SPEC" \
  --gen "$GEN_OUT" \
  --prefix "$RUN_PREFIX" \
  --strict \
  --no-extra

# 5) go test
TEST_CMD=(
  go test ./pkg/util/topsql/reporter
  -run "$RUN_PREFIX"
  -count="$COUNT"
  -timeout 5m
)

set +e
"${TEST_CMD[@]}" 2>&1 | tee "$RUN_LOG"
TEST_RC=${PIPESTATUS[0]}
set -e

# 6) Build audit bundle (best-effort even if go test fails)
set +e
bash "$BUNDLE_SCRIPT" \
  --base "$BASE_REV" \
  --spec "$FILTERED_SPEC" \
  --orig-spec "$SPEC_FILE" \
  --gen "$GEN_OUT" \
  --pkg "pkg/util/topsql/reporter" \
  --out-dir "$ART_DIR" \
  --prefix "$RUN_PREFIX" \
  --go-test-cmd "$TEST_CMD_STR" \
  --oneclick-cmd "$ONECLICK_CMD_STR" \
  >>"$RUN_LOG" 2>"$BUNDLE_ERR"
BUNDLE_RC=$?
set -e
if [[ -s "$BUNDLE_ERR" ]]; then
  {
    echo
    echo "[bundle.stderr]"
    cat "$BUNDLE_ERR"
    echo "[/bundle.stderr]"
    echo
  } >> "$RUN_LOG"
fi
if [[ "$BUNDLE_RC" != "0" ]]; then
  warn "audit bundle failed with rc=${BUNDLE_RC}; stderr=${BUNDLE_ERR}"
  STRICT_BUNDLE="${STRICT_BUNDLE:-1}"
  if [[ "${STRICT_BUNDLE}" == "1" ]]; then
    exit "$BUNDLE_RC"
  fi
fi

# 7) Manifest + SSOT patch
COMMIT="$(git rev-parse HEAD)"
PATCH_ID="$(patch_id_of_head)"
TIME="$(now_iso)"
ENVSTR="$(env_string)"

cat > "$MANIFEST" <<EOF
{
  "id": "${EID}",
  "status": "Captured",
  "type": "${ETYPE}",
  "commit": "${COMMIT}",
  "patch_id": "${PATCH_ID}",
  "time": "${TIME}",
  "env": "${ENVSTR}",
  "command": "${PIPELINE_CMD}",
  "result": {
    "go_test_rc": ${TEST_RC},
    "bundle_rc": ${BUNDLE_RC}
  },
  "artifacts": {
    "run_log": "${RUN_LOG}",
    "summary_md": "${SUMMARY}",
    "pr_diff": "${PR_DIFF}",
    "tests_list": "${TESTS_LIST}",
    "goals_list": "${GOALS_LIST}",
    "coverage_map": "${COVERAGE_MAP}",
    "dedup_report": "${DEDUP_REPORT}",
    "filtered_spec": "${FILTERED_SPEC}",
    "inventory_list": "${INVENTORY_LIST}",
    "bundle_err": "${BUNDLE_ERR}",
    "manifest": "${MANIFEST}"
  }
}
EOF

ssot_patch_evidence "$STATE_FILE" "$EID" "Captured" "$ETYPE" "$COMMIT" "$PATCH_ID" "$TIME" "$ENVSTR" "$PIPELINE_CMD" "$SUMMARY"

log "DONE (go_test_rc=${TEST_RC}, bundle_rc=${BUNDLE_RC})"
log "  - ${RUN_LOG}"
log "  - ${BUNDLE_ERR}"
log "  - ${SUMMARY}"
log "  - ${MANIFEST}"

if [[ "${TEST_RC}" != "0" ]]; then
  exit "${TEST_RC}"
fi
exit 0
