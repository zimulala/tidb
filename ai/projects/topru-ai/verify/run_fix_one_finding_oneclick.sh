#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "${ONECLICK_SUPPRESS_NOTICE:-0}" != "1" ]]; then
  echo "NOTICE: prefer oneclick.sh pr-ready|impl|fix" >&2
fi

usage() {
  cat <<'EOF'
RUN_FIX_ONE_FINDING_ONECLICK

Usage:
  bash ai/projects/topru-ai/verify/run_fix_one_finding_oneclick.sh [--finding Rn] [--impl-cmd "<command>"]

Behavior:
  - Pick one open finding (from SSOT review.open) or use --finding.
  - Optionally run one fix command (--impl-cmd).
  - Re-run strict review in targeted mode for that finding.
  - Write auditable artifacts under artifacts/fix/<RUN_ID>/:
    - patch.diff
    - manifest.json
    - trace.jsonl
    - result.json
EOF
}

FINDING_ID=""
IMPL_CMD=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --finding) FINDING_ID="$2"; shift 2;;
    --impl-cmd) IMPL_CMD="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

PROJECT_ROOT="ai/projects/topru-ai"
VERIFY_DIR="${PROJECT_ROOT}/verify"
SSOT_FILE="${PROJECT_ROOT}/PROJECT_STATE.md"
STRICT_REVIEW_SCRIPT="${VERIFY_DIR}/strict_review_fix_oneclick.sh"
LIB_ONECLICK="${VERIFY_DIR}/lib_oneclick.sh"

[[ -f "${SSOT_FILE}" ]] || { echo "ERROR: SSOT not found: ${SSOT_FILE}" >&2; exit 2; }
[[ -f "${STRICT_REVIEW_SCRIPT}" ]] || { echo "ERROR: strict review script missing: ${STRICT_REVIEW_SCRIPT}" >&2; exit 2; }

if [[ -f "${LIB_ONECLICK}" ]]; then
  ONECLICK_DISABLE_ERR_TRAP=1
  # shellcheck source=/dev/null
  source "${LIB_ONECLICK}"
  ONECLICK_DISABLE_ERR_TRAP=0
fi

if ! command -v now_iso >/dev/null 2>&1; then
  now_iso() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
fi
if ! declare -F trace >/dev/null 2>&1; then
  trace() {
    local st="$1"; local msg="$2"; local status="$3"; local details="${4:-{}}"
    local t; t="$(now_iso)"
    msg="${msg//\"/\' }"
    echo "{\"time\":\"${t}\",\"stage\":\"${st}\",\"status\":\"${status}\",\"msg\":\"${msg}\",\"details\":${details}}" >> "${TRACE_JSONL}" 2>/dev/null || true
  }
fi
if ! declare -F stage >/dev/null 2>&1; then
  stage() { echo "[fix][stage $1] $2"; trace "$1" "$2" "ok" "{}"; }
fi
if ! command -v patch_id_of_head >/dev/null 2>&1; then
  patch_id_of_head() { (git show HEAD | git patch-id --stable | awk '{print $1}') 2>/dev/null || echo "UNKNOWN"; }
fi

json_escape() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  s="${s//$'\r'/\\r}"
  s="${s//$'\t'/\\t}"
  printf '%s' "${s}"
}

json_quote() { printf '"%s"' "$(json_escape "$1")"; }

ssot_review_value() {
  local key="$1"
  awk -v k="${key}" '
    /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inssot=1; next}
    /<!-- NAVIGATOR:END SSOT_V2 -->/ {inssot=0}
    inssot && /^review:\s*$/ {inrev=1; next}
    inssot && inrev && /^[A-Za-z0-9_]+:\s*$/ {inrev=0}
    inssot && inrev && $0 ~ "^  "k":" {
      sub("^  "k":[[:space:]]*", "", $0)
      gsub(/"/, "", $0)
      print $0
      exit
    }
  ' "${SSOT_FILE}" 2>/dev/null || true
}

ssot_review_list_csv() {
  local key="$1"
  local line
  line="$(awk -v k="${key}" '
    /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inssot=1; next}
    /<!-- NAVIGATOR:END SSOT_V2 -->/ {inssot=0}
    inssot && /^review:\s*$/ {inrev=1; next}
    inssot && inrev && /^[A-Za-z0-9_]+:\s*$/ {inrev=0}
    inssot && inrev && $0 ~ "^  "k":" {print; exit}
  ' "${SSOT_FILE}" 2>/dev/null || true)"
  echo "${line}" | awk '
    match($0, /\[[^]]*\]/) {
      s=substr($0, RSTART+1, RLENGTH-2)
      gsub(/[[:space:]]/, "", s)
      print s
    }
  ' 2>/dev/null || true
}

ssot_review_last_run_value() {
  local key="$1"
  awk -v k="${key}" '
    /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inssot=1; next}
    /<!-- NAVIGATOR:END SSOT_V2 -->/ {inssot=0}
    inssot && /^review:\s*$/ {inrev=1; next}
    inssot && inrev && /^[A-Za-z0-9_]+:\s*$/ {inrev=0}
    inssot && inrev && /^  last_run:\s*$/ {inlast=1; next}
    inssot && inrev && inlast && /^[A-Za-z0-9_]+:\s*$/ {inlast=0}
    inssot && inrev && inlast && $0 ~ "^    "k":" {
      sub("^    "k":[[:space:]]*", "", $0)
      gsub(/"/, "", $0)
      print $0
      exit
    }
  ' "${SSOT_FILE}" 2>/dev/null || true
}

csv_first_item() {
  local csv="${1:-}"
  [[ -n "${csv}" ]] || { echo ""; return 0; }
  echo "${csv}" | tr ',' '\n' | sed '/^$/d' | head -n 1
}

csv_contains_item() {
  local csv="$1"
  local item="$2"
  [[ ",${csv}," == *",${item},"* ]]
}

TIME_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
HEAD_SHORT="$(git rev-parse --short HEAD 2>/dev/null || echo "UNKNOWN")"
OPEN_BEFORE_CSV="$(ssot_review_list_csv open)"
BASELINE_COMMIT="$(ssot_review_value baseline_commit)"

if [[ -z "${FINDING_ID}" ]]; then
  FINDING_ID="$(csv_first_item "${OPEN_BEFORE_CSV}")"
fi

if [[ -z "${FINDING_ID}" ]]; then
  FINDING_ID="NONE"
fi

RUN_ID="${TIME_UTC}_${HEAD_SHORT}_fix_${FINDING_ID}"
ART_DIR="${PROJECT_ROOT}/artifacts/fix/${RUN_ID}"
RUN_LOG="${ART_DIR}/run.log"
TRACE_JSONL="${ART_DIR}/trace.jsonl"
PATCH_DIFF="${ART_DIR}/patch.diff"
MANIFEST_JSON="${ART_DIR}/manifest.json"
RESULT_JSON="${ART_DIR}/result.json"
REVIEW_LOG="${ART_DIR}/review_run.log"

mkdir -p "${ART_DIR}"
: > "${RUN_LOG}"
: > "${TRACE_JSONL}"
: > "${PATCH_DIFF}"
: > "${MANIFEST_JSON}"
: > "${RESULT_JSON}"
: > "${REVIEW_LOG}"

RESULT_WORD="FAIL"
NEXT_CMD=""
FINAL_RC=1
IMPL_RC=0
REVIEW_RC=0
REVIEW_MD=""
REVIEW_FINDINGS_YAML=""
REVIEW_ART_DIR=""
PATCH_SOURCE="none"

BEFORE_HEAD="$(git rev-parse HEAD 2>/dev/null || true)"
BEFORE_PATCH_ID="$(patch_id_of_head)"

stage 0 "preflight"
trace 0 "fix run start" "ok" "{\"run_id\":\"${RUN_ID}\",\"finding_id\":\"${FINDING_ID}\",\"open_before\":\"${OPEN_BEFORE_CSV}\"}"

if [[ "${FINDING_ID}" == "NONE" ]]; then
  RESULT_WORD="FAIL"
  NEXT_CMD="No open finding found. Re-run review: bash ai/projects/topru-ai/verify/strict_review_fix_oneclick.sh --mode incr"
  FINAL_RC=1
else
  if [[ -n "${IMPL_CMD}" ]]; then
    stage 1 "execute impl-cmd"
    set +e
    bash -lc "${IMPL_CMD}" 2>&1 | tee -a "${RUN_LOG}"
    IMPL_RC=${PIPESTATUS[0]}
    set -e
    trace 1 "impl-cmd finished" "ok" "{\"rc\":${IMPL_RC}}"
  else
    stage 1 "no impl-cmd provided (skip)"
    trace 1 "impl-cmd skipped" "ok" "{}"
    IMPL_RC=0
  fi

  stage 2 "capture patch diff"
  if [[ -n "${BASELINE_COMMIT}" ]] && ! git diff --quiet "${BASELINE_COMMIT}..HEAD" >/dev/null 2>&1; then
    git diff --binary "${BASELINE_COMMIT}..HEAD" > "${PATCH_DIFF}" || true
    PATCH_SOURCE="${BASELINE_COMMIT}..HEAD"
  elif ! git diff --quiet HEAD >/dev/null 2>&1; then
    git diff --binary HEAD > "${PATCH_DIFF}" || true
    PATCH_SOURCE="HEAD(worktree)"
  else
    : > "${PATCH_DIFF}"
    PATCH_SOURCE="empty"
  fi
  PATCH_BYTES="$(wc -c < "${PATCH_DIFF}" | tr -d ' ')"
  trace 2 "patch captured" "ok" "{\"source\":\"${PATCH_SOURCE}\",\"bytes\":${PATCH_BYTES}}"

  stage 3 "targeted strict review + ssot update"
  set +e
  bash "${STRICT_REVIEW_SCRIPT}" --mode targeted --target "${FINDING_ID}" 2>&1 | tee -a "${REVIEW_LOG}"
  REVIEW_RC=${PIPESTATUS[0]}
  set -e
  trace 3 "targeted review finished" "ok" "{\"rc\":${REVIEW_RC}}"

  OPEN_AFTER_CSV="$(ssot_review_list_csv open)"
  REVIEW_ART_DIR="$(ssot_review_last_run_value artifacts_dir)"
  REVIEW_MD="$(ssot_review_last_run_value review_md)"
  REVIEW_FINDINGS_YAML="$(ssot_review_last_run_value findings_yaml)"

  CLOSED="false"
  if [[ -n "${OPEN_AFTER_CSV}" ]]; then
    if ! csv_contains_item "${OPEN_AFTER_CSV}" "${FINDING_ID}"; then
      CLOSED="true"
    fi
  else
    CLOSED="true"
  fi

  if [[ "${IMPL_RC}" == "0" && "${REVIEW_RC}" == "0" && "${CLOSED}" == "true" ]]; then
    RESULT_WORD="PASS"
    FINAL_RC=0
    NEXT_CMD="PR_READY_INCLUDE_REVIEW=1 bash ai/projects/topru-ai/verify/run_pr_ready_oneclick.sh"
  else
    RESULT_WORD="FAIL"
    FINAL_RC=1
    if [[ "${IMPL_RC}" != "0" ]]; then
      NEXT_CMD="Fix impl-cmd failure and rerun: bash ai/projects/topru-ai/verify/run_fix_one_finding_oneclick.sh --finding ${FINDING_ID} --impl-cmd \"<command>\""
    elif [[ "${CLOSED}" != "true" ]]; then
      NEXT_CMD="Finding still open. Apply code fix, commit if needed, then rerun: bash ai/projects/topru-ai/verify/run_fix_one_finding_oneclick.sh --finding ${FINDING_ID}"
    else
      NEXT_CMD="Re-run targeted review: bash ai/projects/topru-ai/verify/strict_review_fix_oneclick.sh --mode targeted --target ${FINDING_ID}"
    fi
  fi
fi

AFTER_HEAD="$(git rev-parse HEAD 2>/dev/null || true)"
AFTER_PATCH_ID="$(patch_id_of_head)"
OPEN_AFTER_CSV="${OPEN_AFTER_CSV:-$(ssot_review_list_csv open)}"

cat > "${MANIFEST_JSON}" <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${RUN_ID}"),
  "type": "fix_one_finding",
  "time": $(json_quote "$(now_iso)"),
  "finding_id": $(json_quote "${FINDING_ID}"),
  "impl_cmd": $(json_quote "${IMPL_CMD}"),
  "impl_rc": ${IMPL_RC},
  "review_rc": ${REVIEW_RC},
  "open_before": $(json_quote "${OPEN_BEFORE_CSV}"),
  "open_after": $(json_quote "${OPEN_AFTER_CSV}"),
  "baseline_commit": $(json_quote "${BASELINE_COMMIT:-}"),
  "before_head": $(json_quote "${BEFORE_HEAD:-}"),
  "after_head": $(json_quote "${AFTER_HEAD:-}"),
  "before_patch_id": $(json_quote "${BEFORE_PATCH_ID}"),
  "after_patch_id": $(json_quote "${AFTER_PATCH_ID}"),
  "patch_source": $(json_quote "${PATCH_SOURCE}"),
  "patch_bytes": $(wc -c < "${PATCH_DIFF}" | tr -d ' '),
  "review_artifacts_dir": $(json_quote "${REVIEW_ART_DIR}"),
  "artifacts": {
    "run_log": $(json_quote "${RUN_LOG}"),
    "review_log": $(json_quote "${REVIEW_LOG}"),
    "trace": $(json_quote "${TRACE_JSONL}"),
    "patch_diff": $(json_quote "${PATCH_DIFF}"),
    "manifest": $(json_quote "${MANIFEST_JSON}")
  }
}
EOF

cat > "${RESULT_JSON}" <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${RUN_ID}"),
  "result": $(json_quote "${RESULT_WORD}"),
  "art_dir": $(json_quote "${ART_DIR}"),
  "next": $(json_quote "${NEXT_CMD}"),
  "finding_id": $(json_quote "${FINDING_ID}"),
  "manifest": $(json_quote "${MANIFEST_JSON}"),
  "trace": $(json_quote "${TRACE_JSONL}"),
  "patch": $(json_quote "${PATCH_DIFF}"),
  "review_md": $(json_quote "${REVIEW_MD}"),
  "findings_yaml": $(json_quote "${REVIEW_FINDINGS_YAML}"),
  "final_rc": ${FINAL_RC}
}
EOF

echo "RESULT=${RESULT_WORD} RUN_ID=${RUN_ID} ART_DIR=${ART_DIR} NEXT=\"${NEXT_CMD}\""
echo "DETAIL trace.jsonl=${TRACE_JSONL}"
echo "DETAIL manifest.json=${MANIFEST_JSON}"
echo "DETAIL patch.diff=${PATCH_DIFF}"
if [[ -n "${REVIEW_MD}" ]]; then
  echo "DETAIL review.md=${REVIEW_MD}"
fi
if [[ -n "${REVIEW_FINDINGS_YAML}" ]]; then
  echo "DETAIL findings.yaml=${REVIEW_FINDINGS_YAML}"
fi
echo "DETAIL result.json=${RESULT_JSON}"

exit "${FINAL_RC}"
