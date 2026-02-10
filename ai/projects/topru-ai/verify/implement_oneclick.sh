#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'EOF'
IMPLEMENT_ONECLICK

Usage:
  bash ai/projects/topru-ai/verify/implement_oneclick.sh --plan [--base <hash>] [--head <hash>]
  bash ai/projects/topru-ai/verify/implement_oneclick.sh --exec --cmd "<command>" [--base <hash>] [--head <hash>]
  bash ai/projects/topru-ai/verify/implement_oneclick.sh --exec --apply --patch-file <path> [--base <hash>] [--head <hash>]

Safety:
  - No auto push.
  - No git config mutation.
  - No writes outside repository through patch apply.
EOF
}

MODE=""
BASE=""
HEAD=""
EXEC_CMD=""
APPLY_PATCH="0"
PATCH_FILE=""
PROJECT_ROOT="ai/projects/topru-ai"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --plan)
      [[ -z "${MODE}" ]] || { echo "ERROR: choose only one mode: --plan or --exec" >&2; exit 2; }
      MODE="plan"
      shift
      ;;
    --exec)
      [[ -z "${MODE}" ]] || { echo "ERROR: choose only one mode: --plan or --exec" >&2; exit 2; }
      MODE="exec"
      shift
      ;;
    --base) BASE="$2"; shift 2;;
    --head) HEAD="$2"; shift 2;;
    --cmd) EXEC_CMD="$2"; shift 2;;
    --apply) APPLY_PATCH="1"; shift;;
    --patch-file) PATCH_FILE="$2"; shift 2;;
    --project-root) PROJECT_ROOT="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

[[ -n "${MODE}" ]] || { echo "ERROR: mode is required (--plan or --exec)" >&2; usage; exit 2; }

if [[ "${MODE}" == "exec" ]]; then
  if [[ -z "${EXEC_CMD}" && -z "${PATCH_FILE}" ]]; then
    echo "ERROR: --exec requires --cmd or --patch-file" >&2
    exit 2
  fi
  if [[ -n "${EXEC_CMD}" && -n "${PATCH_FILE}" ]]; then
    echo "ERROR: use one input source only: --cmd OR --patch-file" >&2
    exit 2
  fi
  if [[ -n "${PATCH_FILE}" && "${APPLY_PATCH}" != "1" ]]; then
    echo "ERROR: --patch-file requires --apply (default is no apply)" >&2
    exit 2
  fi
fi

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

VERIFY_DIR="${PROJECT_ROOT}/verify"
LIB_ONECLICK="${VERIFY_DIR}/lib_oneclick.sh"
[[ -d "${PROJECT_ROOT}" ]] || { echo "ERROR: project root not found: ${PROJECT_ROOT}" >&2; exit 2; }

if [[ -f "${LIB_ONECLICK}" ]]; then
  ONECLICK_DISABLE_ERR_TRAP=1
  # shellcheck source=/dev/null
  source "${LIB_ONECLICK}"
  ONECLICK_DISABLE_ERR_TRAP=0
fi

if ! command -v now_iso >/dev/null 2>&1; then
  now_iso() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
fi
if ! command -v env_string >/dev/null 2>&1; then
  env_string() { echo "$(uname -s)/$(uname -m)"; }
fi
if ! command -v patch_id_of_head >/dev/null 2>&1; then
  patch_id_of_head() { (git show HEAD | git patch-id --stable | awk '{print $1}') 2>/dev/null || echo "UNKNOWN"; }
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
  stage() { echo "[implement][stage $1] $2"; trace "$1" "$2" "ok" "{}"; }
fi
if ! declare -F init_trace >/dev/null 2>&1; then
  init_trace() { : > "${TRACE_JSONL}" 2>/dev/null || true; }
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

json_quote() {
  printf '"%s"' "$(json_escape "$1")"
}

lines_to_json_array() {
  local lines="${1:-}"
  local out="["
  local first=1
  while IFS= read -r line; do
    [[ -n "${line}" ]] || continue
    if [[ ${first} -eq 0 ]]; then
      out+=","
    fi
    out+="$(json_quote "${line}")"
    first=0
  done <<< "${lines}"
  out+="]"
  printf '%s' "${out}"
}

resolve_base_sha() {
  if [[ -n "${BASE}" ]]; then
    echo "${BASE}"
    return 0
  fi
  local upstream
  upstream="$(git rev-parse --abbrev-ref --symbolic-full-name @{upstream} 2>/dev/null || true)"
  if [[ -n "${upstream}" ]]; then
    git merge-base HEAD "${upstream}" 2>/dev/null || true
    return 0
  fi
  git rev-parse HEAD~1 2>/dev/null || git rev-parse HEAD 2>/dev/null || true
}

resolve_abs_path() {
  local p="$1"
  local abs_dir
  if [[ "${p}" == /* ]]; then
    abs_dir="$(cd "$(dirname "${p}")" 2>/dev/null && pwd -P)" || return 1
    echo "${abs_dir}/$(basename "${p}")"
    return 0
  fi
  abs_dir="$(cd "$(dirname "${ROOT}/${p}")" 2>/dev/null && pwd -P)" || return 1
  echo "${abs_dir}/$(basename "${p}")"
}

path_within_repo() {
  local p="$1"
  [[ "${p}" == "${ROOT}" || "${p}" == "${ROOT}/"* ]]
}

validate_cmd_safety() {
  local cmd="$1"
  if echo "${cmd}" | grep -Eq '(^|[[:space:]])git[[:space:]]+push([[:space:]]|$)'; then
    echo "ERROR: command contains forbidden operation: git push" >&2
    return 1
  fi
  if echo "${cmd}" | grep -Eq '(^|[[:space:]])git[[:space:]]+config([[:space:]]|$)'; then
    echo "ERROR: command contains forbidden operation: git config" >&2
    return 1
  fi
  if echo "${cmd}" | grep -Eq 'rm[[:space:]]+-rf[[:space:]]+/'; then
    echo "ERROR: command contains forbidden operation: rm -rf /" >&2
    return 1
  fi
  if echo "${cmd}" | grep -Eq 'curl[[:space:]].*\|[[:space:]]*(bash|sh)\b'; then
    echo "ERROR: command contains forbidden operation: curl | bash/sh" >&2
    return 1
  fi
  return 0
}

collect_changed_files_plan() {
  if [[ -n "${BASE_SHA}" && -n "${HEAD_SHA}" && "${BASE_SHA}" != "${HEAD_SHA}" ]]; then
    git diff --name-only "${BASE_SHA}..${HEAD_SHA}" > "${CHANGED_FILES}" || true
  else
    git diff --name-only > "${CHANGED_FILES}" || true
  fi
}

collect_changed_files_exec() {
  if [[ -s "${PATCH_DIFF}" ]]; then
    grep -E '^\+\+\+ b/' "${PATCH_DIFF}" | sed 's#^\+\+\+ b/##' | sort -u > "${CHANGED_FILES}" || true
  fi
  if [[ ! -s "${CHANGED_FILES}" ]]; then
    git diff --name-only > "${CHANGED_FILES}" || true
  fi
  if [[ ! -s "${CHANGED_FILES}" && -n "${BASE_SHA}" && -n "${HEAD_SHA}" && "${BASE_SHA}" != "${HEAD_SHA}" ]]; then
    git diff --name-only "${BASE_SHA}..${HEAD_SHA}" > "${CHANGED_FILES}" || true
  fi
}

HEAD_SHA="${HEAD:-$(git rev-parse HEAD 2>/dev/null || true)}"
BASE_SHA="$(resolve_base_sha)"
if [[ -z "${BASE_SHA}" ]]; then
  BASE_SHA="${HEAD_SHA}"
fi
RANGE="${BASE_SHA}..${HEAD_SHA}"

TIME_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
HEAD_SHORT="$(echo "${HEAD_SHA:-UNKNOWN}" | cut -c1-10)"
RUN_ID="${TIME_UTC}_${HEAD_SHORT}_${MODE}"
ART_DIR="${PROJECT_ROOT}/artifacts/impl/${RUN_ID}"
PLAN_MD="${ART_DIR}/IMPLEMENT_PLAN.md"
PATCH_DIFF="${ART_DIR}/patch.diff"
TRACE_JSONL="${ART_DIR}/trace.jsonl"
MANIFEST_JSON="${ART_DIR}/manifest.json"
CHANGED_FILES="${ART_DIR}/changed_files.txt"
RESULT_JSON="${ART_DIR}/result.json"
RUN_LOG="${ART_DIR}/run.log"

mkdir -p "${ART_DIR}"
: > "${PLAN_MD}"
: > "${PATCH_DIFF}"
: > "${TRACE_JSONL}"
: > "${MANIFEST_JSON}"
: > "${CHANGED_FILES}"
: > "${RESULT_JSON}"
: > "${RUN_LOG}"
init_trace "${ART_DIR}"

RESULT_WORD="FAIL"
NEXT_CMD="NONE"
FINAL_RC=1
PATCH_BYTES=0
PATCH_PATH_PRINT=""
TOUCHED_COUNT=0
EXEC_RC=0
APPLY_RC=0
PATCH_SOURCE="none"
CONFIG_SIG_BEFORE="$(cksum .git/config 2>/dev/null | awk '{print $1 ":" $2}' || true)"
BEFORE_HEAD="$(git rev-parse HEAD 2>/dev/null || true)"

stage 0 "preflight"
trace 0 "start" "ok" "{\"mode\":\"${MODE}\",\"run_id\":\"${RUN_ID}\"}"

if [[ "${MODE}" == "plan" ]]; then
  stage 1 "collect touched files for plan"
  collect_changed_files_plan
  TOUCHED_COUNT="$(sed '/^$/d' "${CHANGED_FILES}" | wc -l | tr -d ' ')"

  {
    echo "# IMPLEMENT PLAN"
    echo
    echo "- run_id: ${RUN_ID}"
    echo "- mode: ${MODE}"
    echo "- base: ${BASE_SHA}"
    echo "- head: ${HEAD_SHA}"
    echo "- range: ${RANGE}"
    echo "- apply: false"
    echo "- cmd: ${EXEC_CMD:-<none>}"
    echo "- patch_file: ${PATCH_FILE:-<none>}"
    echo "- touched_files_count: ${TOUCHED_COUNT}"
    echo
    echo "## Planned Steps"
    echo "1. Keep current tree unchanged (plan-only)."
    echo "2. Review touched files list."
    echo "3. Run --exec with an explicit command or patch apply."
    echo
    echo "## Touched Files"
    sed -n '1,200p' "${CHANGED_FILES}"
  } > "${PLAN_MD}"

  RESULT_WORD="PASS"
  FINAL_RC=0
  NEXT_CMD="bash ai/projects/topru-ai/verify/implement_oneclick.sh --exec --base ${BASE_SHA} --head ${HEAD_SHA} --cmd __YOUR_CMD__"
else
  stage 1 "execute implementation action"
  if [[ -n "${EXEC_CMD}" ]]; then
    validate_cmd_safety "${EXEC_CMD}" || {
      EXEC_RC=2
      APPLY_RC=0
    }
    if [[ "${EXEC_RC}" == "0" ]]; then
      set +e
      bash -lc "${EXEC_CMD}" 2>&1 | tee -a "${RUN_LOG}"
      EXEC_RC=${PIPESTATUS[0]}
      set -e
      PATCH_SOURCE="cmd"
    fi
  elif [[ -n "${PATCH_FILE}" ]]; then
    ABS_PATCH="$(resolve_abs_path "${PATCH_FILE}")" || {
      echo "ERROR: patch file not resolvable: ${PATCH_FILE}" >&2
      EXEC_RC=2
    }
    if [[ "${EXEC_RC}" == "0" ]]; then
      if ! path_within_repo "${ABS_PATCH}"; then
        echo "ERROR: patch file must be inside repository: ${ABS_PATCH}" >&2
        EXEC_RC=2
      fi
    fi
    if [[ "${EXEC_RC}" == "0" ]]; then
      set +e
      git apply "${ABS_PATCH}" 2>&1 | tee -a "${RUN_LOG}"
      APPLY_RC=${PIPESTATUS[0]}
      set -e
      EXEC_RC="${APPLY_RC}"
      PATCH_SOURCE="apply_patch_file"
    fi
  fi
  trace 1 "exec finished" "ok" "{\"exec_rc\":${EXEC_RC},\"patch_source\":\"${PATCH_SOURCE}\"}"

  stage 2 "capture patch diff"
  AFTER_HEAD="$(git rev-parse HEAD 2>/dev/null || true)"
  if ! git diff --quiet >/dev/null 2>&1; then
    git diff --binary > "${PATCH_DIFF}" || true
  elif [[ -n "${BEFORE_HEAD}" && -n "${AFTER_HEAD}" && "${BEFORE_HEAD}" != "${AFTER_HEAD}" ]]; then
    git diff --binary "${BEFORE_HEAD}..${AFTER_HEAD}" > "${PATCH_DIFF}" || true
  elif [[ -n "${BASE_SHA}" && -n "${HEAD_SHA}" && "${BASE_SHA}" != "${HEAD_SHA}" ]]; then
    git diff --binary "${BASE_SHA}..${HEAD_SHA}" > "${PATCH_DIFF}" || true
  else
    : > "${PATCH_DIFF}"
  fi
  PATCH_BYTES="$(wc -c < "${PATCH_DIFF}" | tr -d ' ')"
  if [[ "${PATCH_BYTES}" != "0" ]]; then
    PATCH_PATH_PRINT="${PATCH_DIFF}"
  fi
  collect_changed_files_exec
  TOUCHED_COUNT="$(sed '/^$/d' "${CHANGED_FILES}" | wc -l | tr -d ' ')"

  CONFIG_SIG_AFTER="$(cksum .git/config 2>/dev/null | awk '{print $1 ":" $2}' || true)"
  if [[ "${CONFIG_SIG_BEFORE}" != "${CONFIG_SIG_AFTER}" ]]; then
    EXEC_RC=4
    NEXT_CMD="Restore .git/config and rerun in safe mode."
    trace 2 "git config changed" "fail" "{\"before\":\"${CONFIG_SIG_BEFORE}\",\"after\":\"${CONFIG_SIG_AFTER}\"}"
  fi

  if [[ "${EXEC_RC}" == "0" && "${PATCH_BYTES}" != "0" ]]; then
    RESULT_WORD="PASS"
    FINAL_RC=0
    NEXT_CMD="bash ai/projects/topru-ai/verify/strict_review_fix_oneclick.sh --mode incr"
  else
    RESULT_WORD="FAIL"
    FINAL_RC=1
    if [[ "${EXEC_RC}" != "0" ]]; then
      NEXT_CMD="Fix command failure and rerun: bash ai/projects/topru-ai/verify/implement_oneclick.sh --exec --cmd \"<your_cmd>\""
    else
      NEXT_CMD="No patch generated. Provide a concrete modification command and rerun --exec."
    fi
  fi
fi

TOUCHED_JSON="$(lines_to_json_array "$(sed '/^$/d' "${CHANGED_FILES}" 2>/dev/null || true)")"
PATCH_ID="$(patch_id_of_head)"

cat > "${MANIFEST_JSON}" <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${RUN_ID}"),
  "mode": $(json_quote "${MODE}"),
  "base": $(json_quote "${BASE_SHA}"),
  "head": $(json_quote "${HEAD_SHA}"),
  "range": $(json_quote "${RANGE}"),
  "time": $(json_quote "$(now_iso)"),
  "env": $(json_quote "$(env_string)"),
  "command": $(json_quote "${EXEC_CMD}"),
  "apply": $(json_quote "${APPLY_PATCH}"),
  "patch_file": $(json_quote "${PATCH_FILE}"),
  "patch_id": $(json_quote "${PATCH_ID}"),
  "touched_files_count": ${TOUCHED_COUNT},
  "touched_files": ${TOUCHED_JSON},
  "artifacts": {
    "plan": $(json_quote "${PLAN_MD}"),
    "patch": $(json_quote "${PATCH_DIFF}"),
    "trace": $(json_quote "${TRACE_JSONL}"),
    "manifest": $(json_quote "${MANIFEST_JSON}"),
    "changed_files": $(json_quote "${CHANGED_FILES}"),
    "run_log": $(json_quote "${RUN_LOG}")
  }
}
EOF

cat > "${RESULT_JSON}" <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${RUN_ID}"),
  "mode": $(json_quote "${MODE}"),
  "result": $(json_quote "${RESULT_WORD}"),
  "base": $(json_quote "${BASE_SHA}"),
  "head": $(json_quote "${HEAD_SHA}"),
  "range": $(json_quote "${RANGE}"),
  "touched_files_count": ${TOUCHED_COUNT},
  "patch_bytes": ${PATCH_BYTES},
  "art_dir": $(json_quote "${ART_DIR}"),
  "next": $(json_quote "${NEXT_CMD}"),
  "paths": {
    "plan": $(json_quote "${PLAN_MD}"),
    "patch": $(json_quote "${PATCH_DIFF}"),
    "trace": $(json_quote "${TRACE_JSONL}"),
    "manifest": $(json_quote "${MANIFEST_JSON}"),
    "changed_files": $(json_quote "${CHANGED_FILES}"),
    "result_json": $(json_quote "${RESULT_JSON}")
  },
  "final_rc": ${FINAL_RC}
}
EOF

echo "base=${BASE_SHA}"
echo "head=${HEAD_SHA}"
echo "range=${RANGE}"
echo "touched_files_count=${TOUCHED_COUNT}"
if [[ "${PATCH_BYTES}" != "0" ]]; then
  echo "patch_diff=${PATCH_DIFF}"
fi
echo "next=${NEXT_CMD}"
echo "RESULT=${RESULT_WORD} RUN_ID=${RUN_ID} ART_DIR=${ART_DIR} NEXT=\"${NEXT_CMD}\""
echo "DETAIL trace.jsonl=${TRACE_JSONL}"
echo "DETAIL manifest.json=${MANIFEST_JSON}"
if [[ "${PATCH_BYTES}" != "0" ]]; then
  echo "DETAIL patch.diff=${PATCH_DIFF}"
fi
echo "DETAIL changed_files.txt=${CHANGED_FILES}"
echo "DETAIL result.json=${RESULT_JSON}"

exit "${FINAL_RC}"
