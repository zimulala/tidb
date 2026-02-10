#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "${ONECLICK_SUPPRESS_NOTICE:-0}" != "1" ]]; then
  echo "NOTICE: prefer oneclick.sh pr-ready|impl|fix" >&2
fi

PROJECT_ROOT="ai/projects/topru-ai"
TRACK_ID="resource-observability-topru"
PROTOCOL_ROOT="ai/ai-change-gates"
PR_READY_INCLUDE_REVIEW="${PR_READY_INCLUDE_REVIEW:-0}" # 1 to include strict-review gate

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT_ROOT="$2"; shift 2;;
    --track) TRACK_ID="$2"; shift 2;;
    --protocol-root) PROTOCOL_ROOT="$2"; shift 2;;
    *) echo "Unknown arg: $1"; exit 2;;
  esac
done

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

# We want to capture runner rc and print a summary; disable lib ERR trap in this wrapper.
ONECLICK_DISABLE_ERR_TRAP=1
source "ai/projects/topru-ai/verify/lib_oneclick.sh"
ONECLICK_DISABLE_ERR_TRAP=0

STATE_FILE="${PROJECT_ROOT}/PROJECT_STATE.md"
TRACK_FILE="${PROTOCOL_ROOT}/tracks/${TRACK_ID}/TRACK.md"

[[ -f "${STATE_FILE}" ]] || die "STATE_FILE not found: ${STATE_FILE}"
[[ -f "${TRACK_FILE}" ]] || die "TRACK_FILE not found: ${TRACK_FILE}"

file_sig() {
  # file_sig <path> -> checksum:size
  local path="$1"
  cksum "${path}" 2>/dev/null | awk '{print $1 ":" $2}' || true
}

resolve_base_sha() {
  if [[ -n "${ONECLICK_BASE_SHA:-}" ]]; then
    echo "${ONECLICK_BASE_SHA}"
    return 0
  fi
  local upstream base
  upstream="$(git rev-parse --abbrev-ref --symbolic-full-name @{upstream} 2>/dev/null || true)"
  if [[ -n "${upstream}" ]]; then
    base="$(git merge-base HEAD "${upstream}" 2>/dev/null || true)"
    if [[ -n "${base}" ]]; then
      echo "${base}"
      return 0
    fi
  fi
  git rev-parse HEAD~1 2>/dev/null || true
}

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
  # lines_to_json_array <multiline-items>
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

csv_to_json_array() {
  # csv_to_json_array "a,b,c" -> ["a","b","c"]
  local csv="${1:-}"
  if [[ -z "${csv}" ]]; then
    echo "[]"
    return 0
  fi
  lines_to_json_array "$(echo "${csv}" | tr ',' '\n' | sed '/^$/d')"
}

bracket_list_to_json_array() {
  # bracket_list_to_json_array "[a, b]" -> ["a","b"]
  local raw="${1:-[]}"
  raw="${raw#[}"
  raw="${raw%]}"
  raw="${raw//[[:space:]]/}"
  if [[ -z "${raw}" ]]; then
    echo "[]"
    return 0
  fi
  csv_to_json_array "${raw}"
}

bool_json() {
  local v="${1:-false}"
  if [[ "${v}" == "true" ]]; then
    echo "true"
  else
    echo "false"
  fi
}

STATE_SIG_BEFORE="$(file_sig "${STATE_FILE}")"
GIT_HEAD_SHA="$(git rev-parse HEAD 2>/dev/null || true)"
GIT_BASE_SHA="$(resolve_base_sha)"
GIT_RANGE=""
if [[ -n "${GIT_HEAD_SHA}" && -n "${GIT_BASE_SHA}" ]]; then
  GIT_RANGE="${GIT_BASE_SHA}..${GIT_HEAD_SHA}"
fi

# Always ensure recipes exist (push automation boundary)
bash "ai/projects/topru-ai/verify/generate_recipes.sh" >/dev/null 2>&1 || true

# Extract SSOT block
SSOT=$(
  awk '
    /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inblk=1; next}
    /<!-- NAVIGATOR:END SSOT_V2 -->/ {inblk=0}
    inblk==1 {print}
  ' "${STATE_FILE}"
)
[[ -n "${SSOT}" ]] || die "SSOT_V2 block not found in ${STATE_FILE}"

get_track_list() {
  local key="$1"
  grep -E "^${key}=" "${TRACK_FILE}" | sed -E "s/^${key}=\\[//; s/\\]\$//; s/[[:space:]]//g" || true
}
to_lines() { echo "$1" | tr ',' '\n' | sed '/^$/d'; }

MUST_RAW="$(get_track_list pr_ready_must)"
SHOULD_RAW="$(get_track_list pr_ready_should)"
COND_RAW="$(get_track_list pr_ready_conditional)"

MUST_LIST="$(to_lines "${MUST_RAW}")"
SHOULD_LIST="$(to_lines "${SHOULD_RAW}")"
COND_LIST="$(to_lines "${COND_RAW}")"
TRACK_MUST_JSON="$(lines_to_json_array "${MUST_LIST}")"
TRACK_SHOULD_JSON="$(lines_to_json_array "${SHOULD_LIST}")"
TRACK_CONDITIONAL_JSON="$(lines_to_json_array "${COND_LIST}")"

evidence_field() {
  # evidence_field <eid> <fieldname>
  local eid="$1"
  local field="$2"
  echo "${SSOT}" | awk -v id="${eid}" -v f="${field}" '
    $0 ~ "^[[:space:]]*-[[:space:]]*id:[[:space:]]*"id"([[:space:]]*$|[[:space:]]*#)" {found=1; next}
    found==1 && $0 ~ "^[[:space:]]*-[[:space:]]*id:" {exit}  # next item
    found==1 && $0 ~ "^[[:space:]]*"f":" {
      gsub("^[[:space:]]*"f":[[:space:]]*", "", $0)
      gsub(/"/, "", $0)
      print $0
      exit
    }
  ' 2>/dev/null || true
}

evidence_is_truly_captured() {
  # treat "Captured but TODO/empty" as not captured (more intelligent than status-only)
  local eid="$1"
  local st commit artifact
  st="$(evidence_field "$eid" "status")"
  commit="$(evidence_field "$eid" "commit")"
  artifact="$(evidence_field "$eid" "artifact")"

  [[ "${st}" == "Captured" ]] || return 1
  [[ -n "${commit}" ]] || return 1
  [[ "${commit}" != TODO_COMMIT ]] || return 1
  [[ -n "${artifact}" ]] || return 1
  [[ "${artifact}" != *"TODO"* ]] || return 1

  # artifact path might be quoted in SSOT; normalize
  if [[ -f "${artifact}" ]]; then
    return 0
  fi
  return 1
}

pr_ready_trace_stub() {
  # pr_ready_trace_stub <trace_path> <stage> <msg> <status> <json_details>
  local trace_path="$1"
  local st="$2"
  local msg="$3"
  local status="$4"
  local details="${5:-{}}"
  local t
  t="$(now_iso)"
  msg="${msg//\"/\' }"
  mkdir -p "$(dirname "${trace_path}")" >/dev/null 2>&1 || true
  echo "{\"time\":\"${t}\",\"stage\":\"${st}\",\"status\":\"${status}\",\"msg\":\"${msg}\",\"details\":${details}}" >> "${trace_path}" 2>/dev/null || true
}

RAN_EIDS=()
RAN_SCRIPTS=()
RAN_RCS=()
RAN_ART_DIRS=()
RAN_TRACES=()
EVIDENCE_EIDS=()
EVIDENCE_ACTIONS=()
EVIDENCE_RCS=()
EVIDENCE_ART_DIRS=()
EVIDENCE_TRACES=()
PR_READY_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
PR_READY_RESULT_DIR="${PROJECT_ROOT}/artifacts/pr_ready/runs/${PR_READY_RUN_ID}"
PR_READY_RESULT_FILE="${PR_READY_RESULT_DIR}/result.json"
PR_READY_MANIFEST_FILE="${PR_READY_RESULT_DIR}/manifest.json"
PR_READY_TRACE_FILE="${PR_READY_RESULT_DIR}/trace.jsonl"
REVIEW_RC=""
REVIEW_ART_DIR=""
REVIEW_TRACE=""
REVIEW_OPEN_CSV=""
AUDIT_PATCH_INVOKED="false"
STATE_FILE_PATCHED="false"

FINAL_PR_READY_BOOL="false"
FINAL_MUST_MISSING_JSON="[]"
FINAL_SHOULD_MISSING_JSON="[]"
FINAL_REVIEW_MD="<none>"
FINAL_FINDINGS_YAML="<none>"
FINAL_FINDINGS_TOTAL="0"
FINAL_REVIEW_OPEN_COUNT="0"
FINAL_REVIEW_RESULT="PASS"
FINAL_REVIEW_OPEN_JSON="[]"
FINAL_FIX_QUEUE_FILE=""
FINAL_NEXT_COMMAND="NONE"

pr_ready_main_trace() {
  # pr_ready_main_trace <stage> <msg> <status> <json_details>
  local st="$1"
  local msg="$2"
  local status="$3"
  local details="${4:-{}}"
  local t
  t="$(now_iso)"
  mkdir -p "${PR_READY_RESULT_DIR}" >/dev/null 2>&1 || true
  msg="${msg//\"/\' }"
  echo "{\"time\":\"${t}\",\"stage\":\"${st}\",\"status\":\"${status}\",\"msg\":\"${msg}\",\"details\":${details}}" >> "${PR_READY_TRACE_FILE}" 2>/dev/null || true
}

record_evidence_result() {
  # record_evidence_result <eid> <action:run|skip> <rc> <art_dir> <trace>
  EVIDENCE_EIDS+=("${1}")
  EVIDENCE_ACTIONS+=("${2}")
  EVIDENCE_RCS+=("${3}")
  EVIDENCE_ART_DIRS+=("${4}")
  EVIDENCE_TRACES+=("${5}")
}

csv_first_item() {
  local csv="${1:-}"
  if [[ -z "${csv}" ]]; then
    echo ""
    return 0
  fi
  echo "${csv}" | tr ',' '\n' | sed '/^$/d' | head -n 1
}

csv_contains() {
  # csv_contains <csv> <item>
  local csv="$1"
  local item="$2"
  [[ ",${csv}," == *",${item},"* ]]
}

json_array_to_csv() {
  # json_array_to_csv ["a","b"] -> a,b
  local raw="${1:-[]}"
  raw="${raw#[}"
  raw="${raw%]}"
  raw="${raw//\"/}"
  raw="${raw//[[:space:]]/}"
  echo "${raw}"
}

json_array_to_bracket_display() {
  local csv
  csv="$(json_array_to_csv "${1:-[]}")"
  if [[ -z "${csv}" ]]; then
    echo "[]"
  else
    echo "[${csv}]"
  fi
}

eid_runner_script() {
  # eid_runner_script <eid>
  local eid="$1"
  case "${eid}" in
    E_integ) echo "bash ai/projects/topru-ai/verify/run_e_integ_oneclick.sh" ;;
    E_func) echo "bash ai/projects/topru-ai/verify/run_e_func_oneclick.sh" ;;
    E_perf) echo "bash ai/projects/topru-ai/verify/run_e_perf_oneclick.sh" ;;
    E_compat) echo "bash ai/projects/topru-ai/verify/run_e_compat_oneclick.sh" ;;
    *) echo "" ;;
  esac
}

first_missing_must_eid() {
  local missing_csv eid
  missing_csv="$(json_array_to_csv "${FINAL_MUST_MISSING_JSON}")"
  if [[ -z "${missing_csv}" ]]; then
    echo ""
    return 0
  fi

  while IFS= read -r eid; do
    [[ -n "${eid}" ]] || continue
    if csv_contains "${missing_csv}" "${eid}"; then
      echo "${eid}"
      return 0
    fi
  done <<< "${MUST_LIST}"

  csv_first_item "${missing_csv}"
}

finding_yaml_field() {
  # finding_yaml_field <yaml_path> <id> <field>
  local yaml_path="$1"
  local finding_id="$2"
  local field="$3"
  [[ -n "${yaml_path}" && -f "${yaml_path}" ]] || return 0
  awk -v id="${finding_id}" -v f="${field}" '
    $0 ~ "^  - id: "id"$" {inside=1; next}
    inside==1 && $0 ~ "^  - id: " && $0 !~ "^  - id: "id"$" {exit}
    inside==1 && $0 ~ "^    "f":" {
      sub("^    "f":[[:space:]]*", "", $0)
      gsub(/"/, "", $0)
      print $0
      exit
    }
  ' "${yaml_path}" 2>/dev/null || true
}

generate_fix_queue() {
  FINAL_FIX_QUEUE_FILE=""
  [[ -n "${REVIEW_OPEN_CSV:-}" ]] || return 0

  local queue_dir="${PROJECT_ROOT}/artifacts/fix/${PR_READY_RUN_ID}"
  FINAL_FIX_QUEUE_FILE="${queue_dir}/fix_queue.json"
  mkdir -p "${queue_dir}" >/dev/null 2>&1 || true

  local items="["
  local first=1
  local id title location advice must_fix next_cmd
  while IFS= read -r id; do
    [[ -n "${id}" ]] || continue
    title="$(finding_yaml_field "${FINAL_FINDINGS_YAML}" "${id}" "title")"
    location="$(finding_yaml_field "${FINAL_FINDINGS_YAML}" "${id}" "location")"
    advice="$(finding_yaml_field "${FINAL_FINDINGS_YAML}" "${id}" "advice")"
    must_fix="$(finding_yaml_field "${FINAL_FINDINGS_YAML}" "${id}" "must_fix")"
    next_cmd="bash ai/projects/topru-ai/verify/fix_one_by_one.sh --id ${id}"
    if [[ ${first} -eq 0 ]]; then
      items+=","
    fi
    items+="{"
    items+="\"id\":$(json_quote "${id}"),"
    items+="\"must_fix\":$(json_quote "${must_fix:-unknown}"),"
    items+="\"title\":$(json_quote "${title:-TODO}"),"
    items+="\"location\":$(json_quote "${location:-<unknown>}"),"
    items+="\"advice\":$(json_quote "${advice:-TODO}"),"
    items+="\"next\":$(json_quote "${next_cmd}")"
    items+="}"
    first=0
  done <<< "$(echo "${REVIEW_OPEN_CSV}" | tr ',' '\n' | sed '/^$/d')"
  items+="]"

  cat > "${FINAL_FIX_QUEUE_FILE}" <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${PR_READY_RUN_ID}"),
  "track_id": $(json_quote "${TRACK_ID}"),
  "source_review_artifacts_dir": $(json_quote "${REVIEW_ART_DIR:-}"),
  "source_findings_yaml": $(json_quote "${FINAL_FINDINGS_YAML:-}"),
  "open_findings": ${items}
}
EOF
}

compute_next_command() {
  FINAL_NEXT_COMMAND="NONE"
  if [[ "${FINAL_REVIEW_OPEN_COUNT}" != "0" ]]; then
    local first_open
    first_open="$(csv_first_item "${REVIEW_OPEN_CSV:-}")"
    if [[ -n "${first_open}" ]]; then
      FINAL_NEXT_COMMAND="bash ai/projects/topru-ai/verify/fix_one_by_one.sh --id ${first_open}"
      return 0
    fi
  fi

  local missing_must_eid missing_cmd
  missing_must_eid="$(first_missing_must_eid)"
  if [[ -n "${missing_must_eid}" ]]; then
    missing_cmd="$(eid_runner_script "${missing_must_eid}")"
    if [[ -n "${missing_cmd}" ]]; then
      FINAL_NEXT_COMMAND="${missing_cmd}"
      return 0
    fi
  fi

  if [[ "${FINAL_PR_READY_BOOL}" == "true" ]]; then
    FINAL_NEXT_COMMAND="(human) ready to open PR / request review"
    return 0
  fi

  FINAL_NEXT_COMMAND="bash ai/projects/topru-ai/verify/run_pr_ready_oneclick.sh"
}

collect_final_state() {
  # collect_final_state <final_rc>
  local rc="${1:-1}"
  if [[ "${rc}" == "0" ]]; then
    FINAL_PR_READY_BOOL="true"
  else
    FINAL_PR_READY_BOOL="false"
  fi

  FINAL_MUST_MISSING_JSON="[]"
  FINAL_SHOULD_MISSING_JSON="[]"
  if [[ -n "${AUDIT_OUT_FILE:-}" && -f "${AUDIT_OUT_FILE}" ]]; then
    FINAL_MUST_MISSING_JSON="$(bracket_list_to_json_array "$(awk -F': ' '/^missing_must: /{print $2; exit}' "${AUDIT_OUT_FILE}" || echo "[]")")"
    FINAL_SHOULD_MISSING_JSON="$(bracket_list_to_json_array "$(awk -F': ' '/^missing_should: /{print $2; exit}' "${AUDIT_OUT_FILE}" || echo "[]")")"
  fi

  FINAL_REVIEW_MD="<none>"
  FINAL_FINDINGS_YAML="<none>"
  FINAL_FINDINGS_TOTAL="0"
  FINAL_REVIEW_OPEN_COUNT="0"
  FINAL_REVIEW_RESULT="PASS"
  FINAL_REVIEW_OPEN_JSON="[]"

  if [[ "${PR_READY_INCLUDE_REVIEW}" == "1" ]]; then
    FINAL_REVIEW_MD="$(review_last_run_field review_md)"
    FINAL_FINDINGS_YAML="$(review_last_run_field findings_yaml)"
    FINAL_REVIEW_OPEN_JSON="$(csv_to_json_array "${REVIEW_OPEN_CSV:-}")"

    if [[ -n "${REVIEW_OPEN_CSV:-}" ]]; then
      FINAL_REVIEW_OPEN_COUNT="$(echo "${REVIEW_OPEN_CSV}" | tr ',' '\n' | sed '/^$/d' | wc -l | tr -d ' ')"
    fi

    if [[ -n "${FINAL_FINDINGS_YAML}" && -f "${FINAL_FINDINGS_YAML}" ]]; then
      FINAL_FINDINGS_TOTAL="$(awk '/^  - id: /{c++} END{print c+0}' "${FINAL_FINDINGS_YAML}" 2>/dev/null || echo 0)"
    elif [[ -n "${FINAL_FINDINGS_YAML}" && -f "${ROOT}/${FINAL_FINDINGS_YAML}" ]]; then
      FINAL_FINDINGS_TOTAL="$(awk '/^  - id: /{c++} END{print c+0}' "${ROOT}/${FINAL_FINDINGS_YAML}" 2>/dev/null || echo 0)"
      FINAL_FINDINGS_YAML="${ROOT}/${FINAL_FINDINGS_YAML}"
    fi

    if [[ "${FINAL_REVIEW_OPEN_COUNT}" != "0" || "${REVIEW_RC:-0}" != "0" ]]; then
      FINAL_REVIEW_RESULT="FAIL"
    fi
  fi

  if [[ -n "${FINAL_FINDINGS_YAML}" && -f "${ROOT}/${FINAL_FINDINGS_YAML}" ]]; then
    FINAL_FINDINGS_YAML="${ROOT}/${FINAL_FINDINGS_YAML}"
  fi

  generate_fix_queue
  compute_next_command

  if [[ "$(file_sig "${STATE_FILE}")" != "${STATE_SIG_BEFORE}" ]]; then
    STATE_FILE_PATCHED="true"
  else
    STATE_FILE_PATCHED="false"
  fi
}

build_evidence_items_json() {
  local out="["
  local first=1
  local n="${#EVIDENCE_EIDS[@]}"
  local i=0
  while [[ ${i} -lt ${n} ]]; do
    if [[ ${first} -eq 0 ]]; then
      out+=","
    fi
    out+="{"
    out+="\"eid\":$(json_quote "${EVIDENCE_EIDS[$i]}"),"
    out+="\"action\":$(json_quote "${EVIDENCE_ACTIONS[$i]}"),"
    out+="\"rc\":${EVIDENCE_RCS[$i]},"
    out+="\"trace\":$(json_quote "${EVIDENCE_TRACES[$i]}"),"
    out+="\"art_dir\":$(json_quote "${EVIDENCE_ART_DIRS[$i]}")"
    out+="}"
    first=0
    i=$((i+1))
  done
  out+="]"
  printf '%s' "${out}"
}

build_result_json() {
  local include_review_json="false"
  if [[ "${PR_READY_INCLUDE_REVIEW}" == "1" ]]; then
    include_review_json="true"
  fi
  local pr_ready_json
  pr_ready_json="$(bool_json "${FINAL_PR_READY_BOOL}")"
  local state_patched_json
  state_patched_json="$(bool_json "${STATE_FILE_PATCHED}")"
  local audit_patched_json
  audit_patched_json="$(bool_json "${AUDIT_PATCH_INVOKED}")"
  local evidence_items_json
  evidence_items_json="$(build_evidence_items_json)"

  cat <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${PR_READY_RUN_ID}"),
  "track_id": $(json_quote "${TRACK_ID}"),
  "base": $(json_quote "${GIT_BASE_SHA:-}"),
  "head": $(json_quote "${GIT_HEAD_SHA:-}"),
  "range": $(json_quote "${GIT_RANGE:-}"),
  "include_review": ${include_review_json},
  "pr_ready": ${pr_ready_json},
  "next_command": $(json_quote "${FINAL_NEXT_COMMAND}"),
  "review": {
    "result": $(json_quote "${FINAL_REVIEW_RESULT}"),
    "open": ${FINAL_REVIEW_OPEN_JSON},
    "findings_total": ${FINAL_FINDINGS_TOTAL},
    "artifacts": {
      "review_md": $(json_quote "${FINAL_REVIEW_MD}"),
      "findings_yaml": $(json_quote "${FINAL_FINDINGS_YAML}"),
      "artifacts_dir": $(json_quote "${REVIEW_ART_DIR:-}")
    },
    "trace": $(json_quote "${REVIEW_TRACE:-}")
  },
  "evidence": {
    "must": ${TRACK_MUST_JSON},
    "should": ${TRACK_SHOULD_JSON},
    "conditional": ${TRACK_CONDITIONAL_JSON},
    "missing": {
      "must": ${FINAL_MUST_MISSING_JSON},
      "should": ${FINAL_SHOULD_MISSING_JSON}
    },
    "items": ${evidence_items_json}
  },
  "audit_file": $(json_quote "${AUDIT_OUT_FILE:-}"),
  "ssot_file": $(json_quote "${STATE_FILE}"),
  "run_manifest": $(json_quote "${PR_READY_MANIFEST_FILE}"),
  "trace": $(json_quote "${PR_READY_TRACE_FILE}"),
  "fix": {
    "queue_file": $(json_quote "${FINAL_FIX_QUEUE_FILE}")
  },
  "patched": {
    "ssot_file_modified": ${state_patched_json},
    "audit_patch_invoked": ${audit_patched_json}
  },
  "result_file": $(json_quote "${PR_READY_RESULT_FILE}")
}
EOF
}

emit_machine_output() {
  # emit_machine_output <final_rc>
  local _rc="${1:-1}"
  mkdir -p "${PR_READY_RESULT_DIR}" >/dev/null 2>&1 || true
  local result_json
  result_json="$(build_result_json)"
  printf '%s\n' "${result_json}" > "${PR_READY_RESULT_FILE}"
  echo "===ONECLICK_JSON_BEGIN==="
  printf '%s\n' "${result_json}"
  echo "===ONECLICK_JSON_END==="
}

write_run_manifest() {
  local must_missing_display
  must_missing_display="$(json_array_to_bracket_display "${FINAL_MUST_MISSING_JSON}")"
  cat > "${PR_READY_MANIFEST_FILE}" <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${PR_READY_RUN_ID}"),
  "track_id": $(json_quote "${TRACK_ID}"),
  "base": $(json_quote "${GIT_BASE_SHA:-}"),
  "head": $(json_quote "${GIT_HEAD_SHA:-}"),
  "range": $(json_quote "${GIT_RANGE:-}"),
  "include_review": $(bool_json "$( [[ "${PR_READY_INCLUDE_REVIEW}" == "1" ]] && echo true || echo false )"),
  "pr_ready": $(bool_json "${FINAL_PR_READY_BOOL}"),
  "review_result": $(json_quote "${FINAL_REVIEW_RESULT}"),
  "review_open_count": ${FINAL_REVIEW_OPEN_COUNT},
  "evidence_must_missing": $(json_quote "${must_missing_display}"),
  "next_command": $(json_quote "${FINAL_NEXT_COMMAND}"),
  "paths": {
    "ssot": $(json_quote "${STATE_FILE}"),
    "result_json": $(json_quote "${PR_READY_RESULT_FILE}"),
    "review_dir": $(json_quote "${REVIEW_ART_DIR:-<none>}"),
    "audit_file": $(json_quote "${AUDIT_OUT_FILE:-<none>}"),
    "trace": $(json_quote "${PR_READY_TRACE_FILE}"),
    "fix_queue": $(json_quote "${FINAL_FIX_QUEUE_FILE:-<none>}")
  }
}
EOF
}

emit_layered_human_output() {
  local must_missing_display
  must_missing_display="$(json_array_to_bracket_display "${FINAL_MUST_MISSING_JSON}")"

  echo "PR_READY=${FINAL_PR_READY_BOOL} REVIEW=${FINAL_REVIEW_RESULT} review_open=${FINAL_REVIEW_OPEN_COUNT} evidence_must_missing=${must_missing_display} RUN_ID=${PR_READY_RUN_ID}"
  echo "NEXT: ${FINAL_NEXT_COMMAND}"
  echo "SSOT: ${STATE_FILE}"
  echo "RESULT_JSON: ${PR_READY_RESULT_FILE}"
  echo "RUN_MANIFEST: ${PR_READY_MANIFEST_FILE}"
  echo "REVIEW_DIR: ${REVIEW_ART_DIR:-<none>}"
  echo "AUDIT_FILE: ${AUDIT_OUT_FILE:-<none>}"
  echo "TRACE: ${PR_READY_TRACE_FILE}"
}

emit_summary() {
  echo "[pr_ready] summary run_id=${PR_READY_RUN_ID}"
  local n="${#RAN_EIDS[@]}"
  if [[ "${n}" == "0" ]]; then
    echo "[pr_ready] summary: no runners executed"
  fi
  local i=0
  while [[ $i -lt $n ]]; do
    echo "[pr_ready]   - ${RAN_EIDS[$i]} rc=${RAN_RCS[$i]} art_dir=${RAN_ART_DIRS[$i]} trace=${RAN_TRACES[$i]} script=${RAN_SCRIPTS[$i]}"
    i=$((i+1))
  done
  if [[ "${PR_READY_INCLUDE_REVIEW}" == "1" ]]; then
    echo "[pr_ready] review: rc=${REVIEW_RC:-<unset>} artifacts_dir=${REVIEW_ART_DIR:-<unset>} trace=${REVIEW_TRACE:-<unset>} open=[${REVIEW_OPEN_CSV:-}]"
  fi
  echo "[pr_ready] audit_file=${AUDIT_OUT_FILE:-<unset>}"
}

final_conclusion() {
  collect_final_state "${1:-1}"

  echo
  echo "PR_READY=${FINAL_PR_READY_BOOL}"
  echo "evidence_must_missing=${FINAL_MUST_MISSING_JSON} evidence_should_missing=${FINAL_SHOULD_MISSING_JSON}"
  echo "review_result=${FINAL_REVIEW_RESULT} review_open_count=${FINAL_REVIEW_OPEN_COUNT} findings_total=${FINAL_FINDINGS_TOTAL}"
  echo "summary: run_id=${PR_READY_RUN_ID} next=${FINAL_NEXT_COMMAND}"
  echo "details: review_md=${FINAL_REVIEW_MD} findings_yaml=${FINAL_FINDINGS_YAML} fix_queue=${FINAL_FIX_QUEUE_FILE:-<none>} audit=${AUDIT_OUT_FILE:-<unset>} ssot=${STATE_FILE}"
}

emit_result_footer() {
  collect_final_state "${1:-1}"
  local result_word="FAIL"
  if [[ "${FINAL_PR_READY_BOOL}" == "true" ]]; then
    result_word="PASS"
  fi
  echo "RESULT=${result_word} RUN_ID=${PR_READY_RUN_ID} ART_DIR=${PR_READY_RESULT_DIR} NEXT=\"${FINAL_NEXT_COMMAND}\""
  echo "DETAIL result_json=${PR_READY_RESULT_FILE}"
  if [[ -n "${FINAL_REVIEW_MD}" && "${FINAL_REVIEW_MD}" != "<none>" ]]; then
    echo "DETAIL review_md=${FINAL_REVIEW_MD}"
  fi
  if [[ -n "${REVIEW_TRACE:-}" ]]; then
    echo "DETAIL trace.jsonl=${REVIEW_TRACE}"
  fi
  if [[ -n "${FINAL_FINDINGS_YAML}" && "${FINAL_FINDINGS_YAML}" != "<none>" ]]; then
    echo "DETAIL findings_yaml=${FINAL_FINDINGS_YAML}"
  fi
  if [[ -n "${FINAL_FIX_QUEUE_FILE}" ]]; then
    echo "DETAIL fix_queue=${FINAL_FIX_QUEUE_FILE}"
  fi
  if [[ -n "${AUDIT_OUT_FILE:-}" ]]; then
    echo "DETAIL audit=${AUDIT_OUT_FILE}"
  fi
}

review_last_run_field() {
  # review_last_run_field <fieldname>
  local field="$1"
  echo "${SSOT}" | awk -v f="${field}" '
    /^review:\s*$/ {inrev=1; next}
    inrev==1 && /^[A-Za-z0-9_]+:\s*$/ && $0 !~ /^review:/ {inrev=0}
    inrev==1 && /^  last_run:\s*$/ {inlast=1; next}
    inrev==1 && inlast==1 && /^[A-Za-z0-9_]+:\s*$/ {inlast=0}
    inrev==1 && inlast==1 && $0 ~ "^    "f":" {
      sub("^    "f":[[:space:]]*", "", $0)
      gsub(/"/, "", $0)
      print $0
      exit
    }
  ' 2>/dev/null || true
}

review_list_csv() {
  # review_list_csv <key> -> "R1,R2" or empty
  local key="$1"
  local line
  line="$(echo "${SSOT}" | awk -v k="${key}" '
    /^review:\s*$/ {inrev=1; next}
    inrev==1 && /^[A-Za-z0-9_]+:\s*$/ && $0 !~ /^review:/ {inrev=0}
    inrev==1 && $0 ~ "^  "k":" {print; exit}
  ' 2>/dev/null || true)"
  echo "${line}" | awk '
    match($0, /\[[^]]*\]/) {
      s=substr($0, RSTART+1, RLENGTH-2)
      gsub(/[[:space:]]/, "", s)
      print s
    }
  ' 2>/dev/null || true
}

run_strict_review() {
  local script="ai/projects/topru-ai/verify/strict_review_fix_oneclick.sh"
  [[ -f "${script}" ]] || die "strict-review script missing: ${script}"

  echo "[pr_ready] run REVIEW -> ${script} (art_dir=${PROJECT_ROOT}/artifacts/review/<RUN_ID>, trace=<computed-after-run>)"
  set +e
  bash "${script}"
  local rc=$?
  set -e

  # Refresh SSOT snapshot to read review.last_run pointers.
  SSOT=$(
    awk '
      /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inblk=1; next}
      /<!-- NAVIGATOR:END SSOT_V2 -->/ {inblk=0}
      inblk==1 {print}
    ' "${STATE_FILE}"
  )

  REVIEW_RC="${rc}"
  REVIEW_ART_DIR="$(review_last_run_field artifacts_dir)"
  REVIEW_TRACE="${REVIEW_ART_DIR}/trace.jsonl"
  REVIEW_OPEN_CSV="$(review_list_csv open)"

  echo "[pr_ready] done REVIEW rc=${rc} trace=${REVIEW_TRACE}"

  RAN_EIDS+=("REVIEW")
  RAN_SCRIPTS+=("${script}")
  RAN_RCS+=("${rc}")
  RAN_ART_DIRS+=("${REVIEW_ART_DIR:-${PROJECT_ROOT}/artifacts/review}")
  RAN_TRACES+=("${REVIEW_TRACE:-}")
  return 0
}

run_oneclick_for_eid() {
  local eid="$1"
  local script=""
  case "${eid}" in
    E_integ) script="ai/projects/topru-ai/verify/run_e_integ_oneclick.sh" ;;
    E_func)  script="ai/projects/topru-ai/verify/run_e_func_oneclick.sh" ;;
    E_perf)  script="ai/projects/topru-ai/verify/run_e_perf_oneclick.sh" ;;
    E_compat) script="ai/projects/topru-ai/verify/run_e_compat_oneclick.sh" ;; # optional later
    *) die "no oneclick mapping for eid=${eid}" ;;
  esac

  [[ -f "${script}" ]] || die "oneclick script missing: ${script}"
  local art_dir="${PROJECT_ROOT}/artifacts/evidence/${eid}"
  local trace_file="${art_dir}/trace.jsonl"
  echo "[pr_ready] run ${eid} -> ${script} (art_dir=${art_dir}, trace=${trace_file})"

  set +e
  bash "${script}"
  local rc=$?
  set -e

  echo "[pr_ready] done ${eid} rc=${rc} trace=${trace_file}"
  pr_ready_trace_stub "${trace_file}" "pr_ready" "runner done" "ok" "{\"eid\":\"${eid}\",\"rc\":${rc}}"

  RAN_EIDS+=("${eid}")
  RAN_SCRIPTS+=("${script}")
  RAN_RCS+=("${rc}")
  RAN_ART_DIRS+=("${art_dir}")
  RAN_TRACES+=("${trace_file}")
  record_evidence_result "${eid}" "run" "${rc}" "${art_dir}" "${trace_file}"
  return "${rc}"
}

finalize_and_exit() {
  # finalize_and_exit <rc>
  local rc="${1:-1}"
  collect_final_state "${rc}"
  mkdir -p "${PR_READY_RESULT_DIR}" >/dev/null 2>&1 || true
  write_run_manifest
  pr_ready_main_trace "finalize" "final layered output" "ok" "{\"rc\":${rc},\"next\":$(json_quote "${FINAL_NEXT_COMMAND}")}"
  emit_layered_human_output
  emit_machine_output "${rc}"
  exit "${rc}"
}

log "PR-ready navigator start"
log "track=${TRACK_ID}"
log "must=[${MUST_RAW}]"
log "should=[${SHOULD_RAW}]"
log "conditional=[${COND_RAW}]"

# Run MUST first
for eid in ${MUST_LIST}; do
  if evidence_is_truly_captured "${eid}"; then
    art_dir="${PROJECT_ROOT}/artifacts/evidence/${eid}"
    trace_file="${art_dir}/trace.jsonl"
    echo "[pr_ready] skip ${eid}: already captured (valid) (art_dir=${art_dir}, trace=${trace_file})"
    if [[ ! -f "${trace_file}" ]]; then
      pr_ready_trace_stub "${trace_file}" "pr_ready" "skip: already captured" "ok" "{\"eid\":\"${eid}\"}"
    fi
    record_evidence_result "${eid}" "skip" "0" "${art_dir}" "${trace_file}"
  else
    log "need ${eid}: not captured or invalid; running..."
    if ! run_oneclick_for_eid "${eid}"; then
      finalize_and_exit 1
    fi
    # refresh SSOT snapshot after patch
    SSOT=$(
      awk '
        /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inblk=1; next}
        /<!-- NAVIGATOR:END SSOT_V2 -->/ {inblk=0}
        inblk==1 {print}
      ' "${STATE_FILE}"
    )
  fi
done

# Then SHOULD
for eid in ${SHOULD_LIST}; do
  if evidence_is_truly_captured "${eid}"; then
    art_dir="${PROJECT_ROOT}/artifacts/evidence/${eid}"
    trace_file="${art_dir}/trace.jsonl"
    echo "[pr_ready] skip ${eid}: already captured (valid) (art_dir=${art_dir}, trace=${trace_file})"
    if [[ ! -f "${trace_file}" ]]; then
      pr_ready_trace_stub "${trace_file}" "pr_ready" "skip: already captured" "ok" "{\"eid\":\"${eid}\"}"
    fi
    record_evidence_result "${eid}" "skip" "0" "${art_dir}" "${trace_file}"
  else
    log "need ${eid}: not captured or invalid; running..."
    if ! run_oneclick_for_eid "${eid}"; then
      finalize_and_exit 1
    fi
    SSOT=$(
      awk '
        /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inblk=1; next}
        /<!-- NAVIGATOR:END SSOT_V2 -->/ {inblk=0}
        inblk==1 {print}
      ' "${STATE_FILE}"
    )
  fi
done

# Optional: strict-review gate (default off; enable via PR_READY_INCLUDE_REVIEW=1).
if [[ "${PR_READY_INCLUDE_REVIEW}" == "1" ]]; then
  log "PR_READY_INCLUDE_REVIEW=1: running strict-review oneclick ..."
  run_strict_review
else
  log "PR_READY_INCLUDE_REVIEW!=1: skip strict-review (fast path)"
fi

# Finally audit + patch pr_ready block back into SSOT
log "final audit + patch pr_ready ..."
AUDIT_OUT_FILE="${PROJECT_ROOT}/artifacts/audit/runs/${PR_READY_RUN_ID}/audit.txt"
AUDIT_PATCH_INVOKED="true"
AUDIT_OUT_FILE="${AUDIT_OUT_FILE}" bash "ai/projects/topru-ai/verify/audit_ssot.sh" --patch

final_rc=0
if [[ -f "${AUDIT_OUT_FILE}" ]]; then
  audit_status="$(awk -F': ' '/^pr_ready_status: /{print $2; exit}' "${AUDIT_OUT_FILE}" || true)"
  if [[ "${audit_status}" != "true" ]]; then
    final_rc=1
  fi
fi

if [[ "${PR_READY_INCLUDE_REVIEW}" == "1" ]]; then
  # If review has open items, treat PR as not ready.
  if [[ -n "${REVIEW_OPEN_CSV}" ]]; then
    final_rc=1
  fi
  if [[ -n "${REVIEW_RC}" && "${REVIEW_RC}" != "0" ]]; then
    final_rc=1
  fi
fi

log "DONE (rc=${final_rc})"
finalize_and_exit "${final_rc}"
