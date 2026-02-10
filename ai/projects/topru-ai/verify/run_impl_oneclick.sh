#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "${ONECLICK_SUPPRESS_NOTICE:-0}" != "1" ]]; then
  echo "NOTICE: prefer oneclick.sh pr-ready|impl|fix" >&2
fi

usage() {
  cat <<'EOF'
RUN_IMPL_ONECLICK

Usage:
  bash ai/projects/topru-ai/verify/run_impl_oneclick.sh --cmd "<implementation command>" [--label <name>] [--allow-empty-patch]

Goal:
  - Execute one implementation/fix command in a controlled way.
  - Always write auditable artifacts:
    - patch.diff
    - manifest.json
    - trace.jsonl
    - result.json
EOF
}

LABEL="impl"
IMPL_CMD=""
ALLOW_EMPTY_PATCH="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --label) LABEL="$2"; shift 2;;
    --cmd) IMPL_CMD="$2"; shift 2;;
    --allow-empty-patch) ALLOW_EMPTY_PATCH="1"; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

if [[ -z "${IMPL_CMD}" ]]; then
  echo "ERROR: --cmd is required" >&2
  usage
  exit 2
fi

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

PROJECT_ROOT="ai/projects/topru-ai"
VERIFY_DIR="${PROJECT_ROOT}/verify"
LIB_ONECLICK="${VERIFY_DIR}/lib_oneclick.sh"

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
  stage() { echo "[impl][stage $1] $2"; trace "$1" "$2" "ok" "{}"; }
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

TIME_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
HEAD_SHORT="$(git rev-parse --short HEAD 2>/dev/null || echo "UNKNOWN")"
LABEL_SAFE="$(echo "${LABEL}" | tr -cs 'A-Za-z0-9._-' '_')"
RUN_ID="${TIME_UTC}_${HEAD_SHORT}_${LABEL_SAFE}"
ART_DIR="${PROJECT_ROOT}/artifacts/impl/${RUN_ID}"
RUN_LOG="${ART_DIR}/run.log"
TRACE_JSONL="${ART_DIR}/trace.jsonl"
PATCH_DIFF="${ART_DIR}/patch.diff"
MANIFEST_JSON="${ART_DIR}/manifest.json"
RESULT_JSON="${ART_DIR}/result.json"

mkdir -p "${ART_DIR}"
: > "${RUN_LOG}"
: > "${TRACE_JSONL}"
: > "${PATCH_DIFF}"
: > "${MANIFEST_JSON}"
: > "${RESULT_JSON}"

RESULT_WORD="FAIL"
NEXT_CMD=""
FINAL_RC=1

BEFORE_HEAD="$(git rev-parse HEAD 2>/dev/null || true)"
BASE_SHA="$(git merge-base HEAD @{upstream} 2>/dev/null || git rev-parse HEAD~1 2>/dev/null || true)"
IMPL_RC=0
PATCH_SOURCE="none"

stage 0 "preflight"
trace 0 "impl run start" "ok" "{\"run_id\":\"${RUN_ID}\",\"label\":\"${LABEL_SAFE}\"}"

stage 1 "execute impl command"
set +e
bash -lc "${IMPL_CMD}" 2>&1 | tee -a "${RUN_LOG}"
IMPL_RC=${PIPESTATUS[0]}
set -e
trace 1 "impl command finished" "ok" "{\"rc\":${IMPL_RC}}"

stage 2 "capture patch diff"
if [[ -n "${BASE_SHA}" ]] && ! git diff --quiet "${BASE_SHA}..HEAD" >/dev/null 2>&1; then
  git diff --binary "${BASE_SHA}..HEAD" > "${PATCH_DIFF}" || true
  PATCH_SOURCE="${BASE_SHA}..HEAD"
elif ! git diff --quiet HEAD >/dev/null 2>&1; then
  git diff --binary HEAD > "${PATCH_DIFF}" || true
  PATCH_SOURCE="HEAD(worktree)"
else
  : > "${PATCH_DIFF}"
  PATCH_SOURCE="empty"
fi
PATCH_BYTES="$(wc -c < "${PATCH_DIFF}" | tr -d ' ')"
trace 2 "patch captured" "ok" "{\"patch_source\":\"${PATCH_SOURCE}\",\"patch_bytes\":${PATCH_BYTES}}"

AFTER_HEAD="$(git rev-parse HEAD 2>/dev/null || true)"
PATCH_ID="$(patch_id_of_head)"

if [[ "${IMPL_RC}" == "0" ]]; then
  if [[ "${PATCH_BYTES}" != "0" || "${ALLOW_EMPTY_PATCH}" == "1" ]]; then
    RESULT_WORD="PASS"
    FINAL_RC=0
    NEXT_CMD="NONE"
  else
    RESULT_WORD="FAIL"
    FINAL_RC=1
    NEXT_CMD="No patch captured. Ensure your impl command changes code, then rerun."
  fi
else
  RESULT_WORD="FAIL"
  FINAL_RC=1
  NEXT_CMD="Fix impl command failure, then rerun: bash ai/projects/topru-ai/verify/run_impl_oneclick.sh --cmd \"${IMPL_CMD}\""
fi

cat > "${MANIFEST_JSON}" <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${RUN_ID}"),
  "type": "impl",
  "label": $(json_quote "${LABEL_SAFE}"),
  "time": $(json_quote "$(now_iso)"),
  "command": $(json_quote "${IMPL_CMD}"),
  "base": $(json_quote "${BASE_SHA:-}"),
  "before_head": $(json_quote "${BEFORE_HEAD:-}"),
  "after_head": $(json_quote "${AFTER_HEAD:-}"),
  "patch_id_after_head": $(json_quote "${PATCH_ID}"),
  "rc": ${IMPL_RC},
  "patch_source": $(json_quote "${PATCH_SOURCE}"),
  "patch_bytes": ${PATCH_BYTES},
  "artifacts": {
    "run_log": $(json_quote "${RUN_LOG}"),
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
  "manifest": $(json_quote "${MANIFEST_JSON}"),
  "trace": $(json_quote "${TRACE_JSONL}"),
  "patch": $(json_quote "${PATCH_DIFF}"),
  "command_rc": ${IMPL_RC},
  "final_rc": ${FINAL_RC}
}
EOF

echo "RESULT=${RESULT_WORD} RUN_ID=${RUN_ID} ART_DIR=${ART_DIR} NEXT=\"${NEXT_CMD}\""
echo "DETAIL trace.jsonl=${TRACE_JSONL}"
echo "DETAIL manifest.json=${MANIFEST_JSON}"
echo "DETAIL patch.diff=${PATCH_DIFF}"
echo "DETAIL result.json=${RESULT_JSON}"

exit "${FINAL_RC}"
