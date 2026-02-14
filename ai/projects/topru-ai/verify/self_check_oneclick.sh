#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="ai/projects/topru-ai"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-root)
      PROJECT_ROOT="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'USAGE'
SELF_CHECK_ONECLICK

Usage:
  bash ai/projects/topru-ai/verify/self_check_oneclick.sh [--project-root <path>]

Read-only checks:
  - repo worktree status
  - SSOT parseability
  - required governance scripts exist + bash -n
  - verify recipes can be expanded
USAGE
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
SSOT_FILE="${PROJECT_ROOT}/PROJECT_STATE.md"
EXPAND_TOOL="ai/ai-change-gates/tools/expand_verify_recipe.sh"

errors=()
warnings=()

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
  local out="["
  local first=1
  local line
  for line in "$@"; do
    [[ -n "${line}" ]] || continue
    if [[ ${first} -eq 0 ]]; then
      out+="," 
    fi
    out+="$(json_quote "${line}")"
    first=0
  done
  out+="]"
  printf '%s' "${out}"
}

add_error() {
  errors+=("$1")
}

add_warning() {
  warnings+=("$1")
}

# 1) repo state
DIRTY_COUNT="$(git status --porcelain | wc -l | tr -d ' ')"
if [[ "${DIRTY_COUNT}" != "0" ]]; then
  add_warning "worktree is dirty entries=${DIRTY_COUNT}"
fi

# 2) SSOT parseability
if [[ ! -f "${SSOT_FILE}" ]]; then
  add_error "missing SSOT: ${SSOT_FILE}"
else
  if ! grep -q "<!-- NAVIGATOR:BEGIN SSOT_V2 -->" "${SSOT_FILE}"; then
    add_error "SSOT_V2 begin marker missing in ${SSOT_FILE}"
  fi
  if ! grep -q "<!-- NAVIGATOR:END SSOT_V2 -->" "${SSOT_FILE}"; then
    add_error "SSOT_V2 end marker missing in ${SSOT_FILE}"
  fi
  SSOT_BLOCK_LINES="$(awk '
    /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inblk=1; next}
    /<!-- NAVIGATOR:END SSOT_V2 -->/ {inblk=0}
    inblk==1 {c++}
    END {print c+0}
  ' "${SSOT_FILE}" 2>/dev/null || echo 0)"
  if [[ "${SSOT_BLOCK_LINES}" == "0" ]]; then
    add_error "SSOT_V2 block is empty or unparsable"
  fi
fi

# 3) required scripts exist + bash -n
required_scripts=(
  "ai/ai-change-gates/gatecheck/quick.sh"
  "ai/ai-change-gates/gatecheck/review_pack.sh"
  "ai/ai-change-gates/tools/run_record.sh"
  "ai/ai-change-gates/tools/expand_verify_recipe.sh"
  "ai/projects/topru-ai/verify/strict_review_fix_oneclick.sh"
  "ai/projects/topru-ai/verify/run_fix_one_finding_oneclick.sh"
  "ai/projects/topru-ai/verify/run_pr_ready_oneclick.sh"
  "ai/projects/topru-ai/verify/oneclick.sh"
)

for script in "${required_scripts[@]}"; do
  if [[ ! -f "${script}" ]]; then
    add_error "missing script: ${script}"
    continue
  fi
  if ! bash -n "${script}" >/dev/null 2>&1; then
    add_error "bash -n failed: ${script}"
  fi
  if [[ ! -x "${script}" ]]; then
    add_warning "not executable: ${script}"
  fi
done

# 4) verify recipes expansion
if [[ ! -x "${EXPAND_TOOL}" ]]; then
  add_error "missing expand tool: ${EXPAND_TOOL}"
else
  recipe_ids=(
    "V_INTERVAL_DETERMINISM"
    "V_CONCURRENCY_CANCEL_EXIT"
    "V_BACKPRESSURE_SLOW_CONSUMER"
    "V_PERF_SANITY_AGG"
  )
  for rid in "${recipe_ids[@]}"; do
    out="$(bash "${EXPAND_TOOL}" --recipe-id "${rid}" --project-root "${ROOT}" 2>&1 || true)"
    if [[ -z "${out}" ]]; then
      add_error "recipe ${rid} expansion returned empty"
      continue
    fi
    if echo "${out}" | grep -q '^# TODO: recipe not found'; then
      add_error "recipe ${rid} not mapped"
    fi
  done
fi

RESULT_WORD="PASS"
SELF_CHECK_WORD="OK"
NEXT_CMD="NONE"
if [[ "${#errors[@]}" -gt 0 ]]; then
  RESULT_WORD="FAIL"
  SELF_CHECK_WORD="FAIL"
  NEXT_CMD="Fix self-check errors and rerun: bash ai/projects/topru-ai/verify/oneclick.sh self-check"
elif [[ "${#warnings[@]}" -gt 0 ]]; then
  NEXT_CMD="Optional: clean worktree before review runs"
fi

echo "SELF_CHECK=${SELF_CHECK_WORD}"
echo "NEXT: ${NEXT_CMD}"
echo "DETAILS: ssot=${SSOT_FILE} dirty_entries=${DIRTY_COUNT} errors=${#errors[@]} warnings=${#warnings[@]}"

for e in "${errors[@]-}"; do
  [[ -n "${e}" ]] || continue
  echo "DETAIL error=${e}"
done
for w in "${warnings[@]-}"; do
  [[ -n "${w}" ]] || continue
  echo "DETAIL warning=${w}"
done

echo "RESULT=${RESULT_WORD} RUN_ID=${RUN_ID} ART_DIR=NONE NEXT=\"${NEXT_CMD}\""

echo "===ONECLICK_JSON_BEGIN==="
cat <<JSON
{
  "schema_version": "v1",
  "run_id": $(json_quote "${RUN_ID}"),
  "result": $(json_quote "${RESULT_WORD}"),
  "self_check": $(json_quote "${SELF_CHECK_WORD}"),
  "project_root": $(json_quote "${PROJECT_ROOT}"),
  "ssot": $(json_quote "${SSOT_FILE}"),
  "dirty_entries": ${DIRTY_COUNT},
  "errors": $(lines_to_json_array "${errors[@]-}"),
  "warnings": $(lines_to_json_array "${warnings[@]-}"),
  "next": $(json_quote "${NEXT_CMD}")
}
JSON
echo "===ONECLICK_JSON_END==="

if [[ "${RESULT_WORD}" == "PASS" ]]; then
  exit 0
fi
exit 1
