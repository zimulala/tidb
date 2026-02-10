#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'EOF'
FIX_ONE_BY_ONE

Usage:
  bash ai/projects/topru-ai/verify/fix_one_by_one.sh --id R# [--base <commit>] [--head <commit>] [--apply] [--no-verify]

Behavior:
  - Resolve one finding from latest SSOT-linked findings.yaml.
  - Attempt minimal auto-fix (currently supports gofmt-style findings).
  - Run verify_min by default (unless --no-verify).
  - Update finding status and patch SSOT review section.
EOF
}

FINDING_ID=""
BASE_SHA=""
HEAD_SHA=""
APPLY_FIX="0"
NO_VERIFY="0"
PROJECT_ROOT="ai/projects/topru-ai"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --id) FINDING_ID="$2"; shift 2;;
    --base) BASE_SHA="$2"; shift 2;;
    --head) HEAD_SHA="$2"; shift 2;;
    --apply) APPLY_FIX="1"; shift;;
    --no-verify) NO_VERIFY="1"; shift;;
    --project-root) PROJECT_ROOT="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

[[ -n "${FINDING_ID}" ]] || { echo "ERROR: --id is required" >&2; usage; exit 2; }

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

VERIFY_DIR="${PROJECT_ROOT}/verify"
SSOT_FILE="${PROJECT_ROOT}/PROJECT_STATE.md"
SSOT_PATCH_REVIEW="${VERIFY_DIR}/ssot_patch_review.py"
LIB_ONECLICK="${VERIFY_DIR}/lib_oneclick.sh"

[[ -f "${SSOT_FILE}" ]] || { echo "ERROR: SSOT not found: ${SSOT_FILE}" >&2; exit 2; }
[[ -f "${SSOT_PATCH_REVIEW}" ]] || { echo "ERROR: ssot patch helper missing: ${SSOT_PATCH_REVIEW}" >&2; exit 2; }

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
  stage() { echo "[fix_one][stage $1] $2"; trace "$1" "$2" "ok" "{}"; }
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

json_quote() { printf '"%s"' "$(json_escape "$1")"; }

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

finding_field() {
  local file="$1"
  local id="$2"
  local key="$3"
  awk -v id="${id}" -v k="${key}" '
    $0 ~ "^  - id: "id"$" {inside=1; next}
    inside==1 && $0 ~ "^  - id: " && $0 !~ "^  - id: "id"$" {exit}
    inside==1 && $0 ~ "^    "k":" {
      sub("^    "k":[[:space:]]*", "", $0)
      gsub(/"/, "", $0)
      print $0
      exit
    }
  ' "${file}" 2>/dev/null || true
}

finding_list() {
  local file="$1"
  local id="$2"
  local key="$3"
  awk -v id="${id}" -v k="${key}" '
    $0 ~ "^  - id: "id"$" {inside=1; next}
    inside==1 && $0 ~ "^  - id: " && $0 !~ "^  - id: "id"$" {exit}
    inside==1 && $0 ~ "^    "k":\\s*$" {inlist=1; next}
    inside==1 && inlist==1 && $0 ~ "^    [A-Za-z0-9_]+:" {exit}
    inside==1 && inlist==1 && $0 ~ "^      - " {
      sub("^      -[[:space:]]*", "", $0)
      gsub(/"/, "", $0)
      print $0
    }
  ' "${file}" 2>/dev/null || true
}

update_finding_status() {
  local file="$1"
  local id="$2"
  local new_status="$3"
  local tmp
  tmp="$(mktemp)"
  awk -v id="${id}" -v st="${new_status}" '
    $0 ~ "^  - id: "id"$" {inside=1; print; next}
    inside==1 && $0 ~ "^  - id: " && $0 !~ "^  - id: "id"$" {inside=0}
    inside==1 && $0 ~ "^    status:" {
      print "    status: "st
      inside=0
      next
    }
    {print}
  ' "${file}" > "${tmp}"
  mv "${tmp}" "${file}"
}

contains_finding() {
  local file="$1"
  local id="$2"
  rg -q "^  - id: ${id}$" "${file}"
}

normalize_verify_cmd() {
  local raw="$1"
  raw="${raw#"${raw%%[![:space:]]*}"}"
  raw="${raw%"${raw##*[![:space:]]}"}"
  raw="${raw%\"}"
  raw="${raw#\"}"
  raw="$(echo "${raw}" | sed -E 's/[[:space:]]*\\(expect empty\\)[[:space:]]*$//')"
  if [[ -z "${raw}" ]]; then
    echo ""
    return 0
  fi
  if [[ "${raw}" == gofmt\ -l* ]]; then
    echo "test -z \"\$(${raw})\""
    return 0
  fi
  echo "${raw}"
}

extract_head_from_range() {
  local range="$1"
  if [[ "${range}" == *".."* ]]; then
    echo "${range##*..}"
    return 0
  fi
  echo ""
}

FINDINGS_YAML_RAW="$(ssot_review_last_run_value findings_yaml)"
REVIEW_MD_RAW="$(ssot_review_last_run_value review_md)"
NEXT_ACTIONS_RAW="$(ssot_review_last_run_value next_actions)"
REVIEW_ART_DIR_RAW="$(ssot_review_last_run_value artifacts_dir)"
BASELINE_RAW="$(ssot_review_value baseline_commit)"
RUN_RANGE_RAW="$(ssot_review_last_run_value range)"

[[ -n "${FINDINGS_YAML_RAW}" ]] || { echo "ERROR: SSOT.review.last_run.findings_yaml is empty" >&2; exit 2; }
FINDINGS_YAML_ABS="$(resolve_abs_path "${FINDINGS_YAML_RAW}")" || { echo "ERROR: findings_yaml path is invalid: ${FINDINGS_YAML_RAW}" >&2; exit 2; }
[[ -f "${FINDINGS_YAML_ABS}" ]] || { echo "ERROR: findings_yaml not found: ${FINDINGS_YAML_ABS}" >&2; exit 2; }

if [[ -z "${BASE_SHA}" ]]; then
  BASE_SHA="${BASELINE_RAW:-$(git rev-parse HEAD 2>/dev/null || true)}"
fi
if [[ -z "${HEAD_SHA}" ]]; then
  HEAD_SHA="$(extract_head_from_range "${RUN_RANGE_RAW}")"
fi
if [[ -z "${HEAD_SHA}" ]]; then
  HEAD_SHA="$(git rev-parse HEAD 2>/dev/null || true)"
fi
RANGE="${BASE_SHA}..${HEAD_SHA}"

if ! contains_finding "${FINDINGS_YAML_ABS}" "${FINDING_ID}"; then
  echo "ERROR: finding not found in latest findings.yaml: ${FINDING_ID}" >&2
  exit 2
fi

TIME_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
HEAD_SHORT="$(git rev-parse --short HEAD 2>/dev/null || echo "UNKNOWN")"
RUN_ID="${TIME_UTC}_${HEAD_SHORT}_fix_${FINDING_ID}"
ART_DIR="${PROJECT_ROOT}/artifacts/fix/${RUN_ID}"
FIX_PLAN_MD="${ART_DIR}/fix_plan.md"
PATCH_DIFF="${ART_DIR}/patch.diff"
VERIFY_LOG="${ART_DIR}/verify.log"
TRACE_JSONL="${ART_DIR}/trace.jsonl"
MANIFEST_JSON="${ART_DIR}/manifest.json"
RESULT_JSON="${ART_DIR}/result.json"

mkdir -p "${ART_DIR}"
: > "${FIX_PLAN_MD}"
: > "${PATCH_DIFF}"
: > "${VERIFY_LOG}"
: > "${TRACE_JSONL}"
: > "${MANIFEST_JSON}"
: > "${RESULT_JSON}"
init_trace "${ART_DIR}"

OLD_STATUS="$(finding_field "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "status")"
FINDING_TYPE="$(finding_field "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "type")"
FINDING_TITLE="$(finding_field "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "title")"
FINDING_LOCATION="$(finding_field "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "location")"
FINDING_ADVICE="$(finding_field "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "advice")"
VERIFY_MIN_LINES="$(finding_list "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "verify_min")"
CLEANUP_LINES="$(finding_list "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "cleanup")"

AUTO_FIX="0"
if [[ "${FINDING_TYPE}" == "Style" ]] || echo "${FINDING_TITLE} ${FINDING_ADVICE}" | grep -qi "gofmt"; then
  AUTO_FIX="1"
fi

TARGET_FILES=""
if [[ "${AUTO_FIX}" == "1" ]]; then
  TARGET_FILES="$(
    echo "${FINDING_LOCATION}" | tr ', ' '\n' | sed '/^$/d' | while read -r token; do
      token="${token%%:*}"
      if [[ "${token}" == *.go && -f "${ROOT}/${token}" ]]; then
        echo "${token}"
      fi
    done | sort -u
  )"
fi

if [[ -z "${TARGET_FILES}" && -n "${REVIEW_ART_DIR_RAW}" && "${AUTO_FIX}" == "1" ]]; then
  CHANGED_FILE_CANDIDATE="$(resolve_abs_path "${REVIEW_ART_DIR_RAW}/changed_files.txt" || true)"
  if [[ -n "${CHANGED_FILE_CANDIDATE}" && -f "${CHANGED_FILE_CANDIDATE}" ]]; then
    TARGET_FILES="$(grep -E '\.go$' "${CHANGED_FILE_CANDIDATE}" | sed '/^$/d' | sort -u || true)"
  fi
fi

TARGET_FILES_COUNT="$(echo "${TARGET_FILES}" | sed '/^$/d' | wc -l | tr -d ' ')"
TARGET_FILES_INLINE="$(echo "${TARGET_FILES}" | sed '/^$/d' | paste -sd ' ' - 2>/dev/null || true)"

{
  echo "# Fix Plan"
  echo
  echo "- run_id: ${RUN_ID}"
  echo "- finding_id: ${FINDING_ID}"
  echo "- old_status: ${OLD_STATUS:-unknown}"
  echo "- base: ${BASE_SHA}"
  echo "- head: ${HEAD_SHA}"
  echo "- range: ${RANGE}"
  echo "- apply: ${APPLY_FIX}"
  echo "- auto_fix_supported: ${AUTO_FIX}"
  echo "- target_files_count: ${TARGET_FILES_COUNT}"
  echo "- target_files: ${TARGET_FILES_INLINE:-<none>}"
  echo
  echo "## Finding"
  echo "- title: ${FINDING_TITLE:-TODO}"
  echo "- location: ${FINDING_LOCATION:-<unknown>}"
  echo "- advice: ${FINDING_ADVICE:-TODO}"
  echo
  echo "## Planned Actions"
  if [[ "${AUTO_FIX}" == "1" ]]; then
    if [[ "${APPLY_FIX}" == "1" ]]; then
      echo "1. Apply minimal gofmt fix to target files."
    else
      echo "1. Generate minimal gofmt patch only (no apply)."
    fi
  else
    echo "1. No safe auto-fix strategy detected."
    echo "2. Escalate to human edit with explicit commands."
  fi
  echo "2. Run verify_min unless --no-verify."
  echo "3. Update finding status + SSOT review lists."
} > "${FIX_PLAN_MD}"

stage 0 "preflight"
trace 0 "fix run start" "ok" "{\"id\":\"${FINDING_ID}\",\"auto_fix\":${AUTO_FIX}}"

ACTION_RC=0
NEED_HUMAN="0"
if [[ "${AUTO_FIX}" == "1" && "${TARGET_FILES_COUNT}" != "0" ]]; then
  stage 1 "execute minimal fix action"
  if [[ "${APPLY_FIX}" == "1" ]]; then
    set +e
    gofmt -w ${TARGET_FILES_INLINE} > /dev/null 2>> "${VERIFY_LOG}"
    ACTION_RC=$?
    set -e
    if [[ "${ACTION_RC}" == "0" ]]; then
      git diff --binary -- ${TARGET_FILES_INLINE} > "${PATCH_DIFF}" || true
    fi
  else
    : > "${PATCH_DIFF}"
    while IFS= read -r file; do
      [[ -n "${file}" ]] || continue
      tmp="$(mktemp)"
      gofmt "${file}" > "${tmp}" || true
      diff -u "${file}" "${tmp}" >> "${PATCH_DIFF}" || true
      rm -f "${tmp}"
    done <<< "${TARGET_FILES}"
  fi
else
  NEED_HUMAN="1"
  ACTION_RC=0
  trace 1 "auto-fix unsupported" "fail" "{\"id\":\"${FINDING_ID}\",\"type\":\"${FINDING_TYPE}\"}"
fi

PATCH_BYTES="$(wc -c < "${PATCH_DIFF}" | tr -d ' ')"

VERIFY_RC=0
if [[ "${NO_VERIFY}" != "1" ]]; then
  stage 2 "run verify_min commands"
  verify_lines="${VERIFY_MIN_LINES}"
  if [[ -z "${verify_lines}" && "${AUTO_FIX}" == "1" && "${TARGET_FILES_COUNT}" != "0" ]]; then
    verify_lines="gofmt -l ${TARGET_FILES_INLINE}"
  fi
  if [[ -n "${verify_lines}" ]]; then
    while IFS= read -r cmd; do
      [[ -n "${cmd}" ]] || continue
      if [[ -n "${TARGET_FILES_INLINE}" ]]; then
        cmd="${cmd//<files>/${TARGET_FILES_INLINE}}"
      fi
      cmd="$(normalize_verify_cmd "${cmd}")"
      [[ -n "${cmd}" ]] || continue
      echo "[verify] $cmd" >> "${VERIFY_LOG}"
      set +e
      bash -lc "${cmd}" >> "${VERIFY_LOG}" 2>&1
      rc=$?
      set -e
      if [[ "${rc}" != "0" ]]; then
        VERIFY_RC=1
      fi
    done <<< "${verify_lines}"
  fi
else
  stage 2 "verify skipped (--no-verify)"
fi

UPDATED_STATUS="${OLD_STATUS:-open}"
if [[ "${NEED_HUMAN}" == "1" ]]; then
  UPDATED_STATUS="open"
elif [[ "${APPLY_FIX}" == "1" && "${ACTION_RC}" == "0" && "${VERIFY_RC}" == "0" ]]; then
  UPDATED_STATUS="fixed"
elif [[ "${PATCH_BYTES}" != "0" ]]; then
  UPDATED_STATUS="partially_fixed"
else
  UPDATED_STATUS="open"
fi

cp "${FINDINGS_YAML_ABS}" "${ART_DIR}/findings.before.yaml"
update_finding_status "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "${UPDATED_STATUS}"
cp "${FINDINGS_YAML_ABS}" "${ART_DIR}/findings.after.yaml"

stage 3 "patch SSOT review lists"
REVIEW_MD_FOR_PATCH="${REVIEW_MD_RAW:-${PROJECT_ROOT}/artifacts/review/<unknown>/review.md}"
NEXT_ACTIONS_FOR_PATCH="${NEXT_ACTIONS_RAW:-${PROJECT_ROOT}/artifacts/review/<unknown>/next_actions.md}"
ARTIFACTS_DIR_FOR_PATCH="${REVIEW_ART_DIR_RAW:-$(dirname "${FINDINGS_YAML_RAW}")}"

set +e
python3 "${SSOT_PATCH_REVIEW}" \
  --op ensure \
  --ssot "${SSOT_FILE}" \
  --base "${BASE_SHA}" \
  --head "${HEAD_SHA}" \
  --run-range "${RANGE}" \
  --artifacts-dir "${ARTIFACTS_DIR_FOR_PATCH}" \
  --review-md "${REVIEW_MD_FOR_PATCH}" \
  --findings-yaml "${FINDINGS_YAML_RAW}" \
  --next-actions "${NEXT_ACTIONS_FOR_PATCH}" \
  --baseline-commit "${BASE_SHA}" >> "${VERIFY_LOG}" 2>&1
rc_ensure=$?
python3 "${SSOT_PATCH_REVIEW}" \
  --op update \
  --ssot "${SSOT_FILE}" \
  --base "${BASE_SHA}" \
  --head "${HEAD_SHA}" \
  --run-range "${RANGE}" \
  --artifacts-dir "${ARTIFACTS_DIR_FOR_PATCH}" \
  --review-md "${REVIEW_MD_FOR_PATCH}" \
  --findings-yaml "${FINDINGS_YAML_RAW}" \
  --next-actions "${NEXT_ACTIONS_FOR_PATCH}" \
  --baseline-commit "${BASE_SHA}" >> "${VERIFY_LOG}" 2>&1
rc_update=$?
set -e
SSOT_RC=0
if [[ "${rc_ensure}" != "0" || "${rc_update}" != "0" ]]; then
  SSOT_RC=1
fi

TOUCHED_FILES_JSON="$(lines_to_json_array "${TARGET_FILES}")"
TOUCHED_FILES_COUNT="${TARGET_FILES_COUNT}"
PATCH_ID="$(patch_id_of_head)"

cat > "${MANIFEST_JSON}" <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${RUN_ID}"),
  "type": "fix_one_by_one",
  "finding_id": $(json_quote "${FINDING_ID}"),
  "time": $(json_quote "$(now_iso)"),
  "env": $(json_quote "$(env_string)"),
  "base": $(json_quote "${BASE_SHA}"),
  "head": $(json_quote "${HEAD_SHA}"),
  "range": $(json_quote "${RANGE}"),
  "command": $(json_quote "fix_one_by_one --id ${FINDING_ID} --apply=${APPLY_FIX}"),
  "patch_id": $(json_quote "${PATCH_ID}"),
  "touched_files_count": ${TOUCHED_FILES_COUNT},
  "touched_files": ${TOUCHED_FILES_JSON},
  "rc": {
    "action": ${ACTION_RC},
    "verify": ${VERIFY_RC},
    "ssot": ${SSOT_RC}
  },
  "status": {
    "before": $(json_quote "${OLD_STATUS:-open}"),
    "after": $(json_quote "${UPDATED_STATUS}")
  },
  "artifacts": {
    "fix_plan": $(json_quote "${FIX_PLAN_MD}"),
    "patch": $(json_quote "${PATCH_DIFF}"),
    "verify_log": $(json_quote "${VERIFY_LOG}"),
    "trace": $(json_quote "${TRACE_JSONL}"),
    "manifest": $(json_quote "${MANIFEST_JSON}")
  }
}
EOF

RESULT_WORD="FAIL"
NEXT_CMD=""
if [[ "${UPDATED_STATUS}" == "fixed" && "${SSOT_RC}" == "0" ]]; then
  RESULT_WORD="PASS"
  NEXT_CMD="PR_READY_INCLUDE_REVIEW=1 bash ai/projects/topru-ai/verify/run_pr_ready_oneclick.sh"
else
  RESULT_WORD="FAIL"
  if [[ "${NEED_HUMAN}" == "1" ]]; then
    NEXT_CMD="bash ai/projects/topru-ai/verify/fix_one_by_one.sh --id ${FINDING_ID} --apply"
  else
    NEXT_CMD="bash ai/projects/topru-ai/verify/strict_review_fix_oneclick.sh --mode targeted --target ${FINDING_ID}"
  fi
fi

cat > "${RESULT_JSON}" <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${RUN_ID}"),
  "result": $(json_quote "${RESULT_WORD}"),
  "finding_id": $(json_quote "${FINDING_ID}"),
  "updated_status": $(json_quote "${UPDATED_STATUS}"),
  "need_human": $(json_quote "${NEED_HUMAN}"),
  "next": $(json_quote "${NEXT_CMD}"),
  "art_dir": $(json_quote "${ART_DIR}"),
  "paths": {
    "fix_plan": $(json_quote "${FIX_PLAN_MD}"),
    "patch": $(json_quote "${PATCH_DIFF}"),
    "verify_log": $(json_quote "${VERIFY_LOG}"),
    "trace": $(json_quote "${TRACE_JSONL}"),
    "manifest": $(json_quote "${MANIFEST_JSON}"),
    "result_json": $(json_quote "${RESULT_JSON}")
  }
}
EOF

if [[ "${NEED_HUMAN}" == "1" ]]; then
  echo "NEED_HUMAN: unable to auto-fix ${FINDING_ID} safely"
  echo "STEP1: apply minimal manual edit at ${FINDING_LOCATION:-<unknown>}"
  echo "STEP2: run verify_min commands recorded in ${VERIFY_LOG}"
  echo "STEP3: rerun bash ai/projects/topru-ai/verify/fix_one_by_one.sh --id ${FINDING_ID} --apply"
fi

echo "RESULT=${RESULT_WORD}"
echo "UPDATED: ${FINDING_ID} status=${UPDATED_STATUS}"
echo "NEXT=${NEXT_CMD}"
echo "DETAIL fix_plan.md=${FIX_PLAN_MD}"
echo "DETAIL patch.diff=${PATCH_DIFF}"
echo "DETAIL verify.log=${VERIFY_LOG}"
echo "DETAIL trace.jsonl=${TRACE_JSONL}"
echo "DETAIL manifest.json=${MANIFEST_JSON}"
echo "DETAIL result.json=${RESULT_JSON}"

if [[ "${RESULT_WORD}" == "PASS" ]]; then
  exit 0
fi
exit 1
