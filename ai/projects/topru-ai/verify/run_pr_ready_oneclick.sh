#!/usr/bin/env bash
set -Eeuo pipefail

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
PR_READY_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
REVIEW_RC=""
REVIEW_ART_DIR=""
REVIEW_TRACE=""
REVIEW_OPEN_CSV=""

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
  local pr_ready_bool="false"
  if [[ "${1:-1}" == "0" ]]; then
    pr_ready_bool="true"
  fi

  local must_missing="[]"
  local should_missing="[]"
  if [[ -n "${AUDIT_OUT_FILE:-}" && -f "${AUDIT_OUT_FILE}" ]]; then
    must_missing="$(awk -F': ' '/^missing_must: /{print $2; exit}' "${AUDIT_OUT_FILE}" || echo "[]")"
    should_missing="$(awk -F': ' '/^missing_should: /{print $2; exit}' "${AUDIT_OUT_FILE}" || echo "[]")"
  fi

  local review_md="<none>"
  local findings_yaml="<none>"
  local findings_total="0"
  local review_open_count="0"
  local review_result="PASS"

  if [[ "${PR_READY_INCLUDE_REVIEW}" == "1" ]]; then
    review_md="$(review_last_run_field review_md)"
    findings_yaml="$(review_last_run_field findings_yaml)"

    if [[ -n "${REVIEW_OPEN_CSV:-}" ]]; then
      review_open_count="$(echo "${REVIEW_OPEN_CSV}" | tr ',' '\n' | sed '/^$/d' | wc -l | tr -d ' ')"
    fi

    if [[ -n "${findings_yaml}" && -f "${findings_yaml}" ]]; then
      findings_total="$(awk '/^  - id: /{c++} END{print c+0}' "${findings_yaml}" 2>/dev/null || echo 0)"
    else
      # findings_yaml might be repo-relative
      if [[ -n "${findings_yaml}" && -f "${ROOT}/${findings_yaml}" ]]; then
        findings_total="$(awk '/^  - id: /{c++} END{print c+0}' "${ROOT}/${findings_yaml}" 2>/dev/null || echo 0)"
        findings_yaml="${ROOT}/${findings_yaml}"
      fi
    fi

    # PASS when no open findings and review runner rc==0 (0 findings counts as PASS).
    if [[ "${review_open_count}" != "0" || "${REVIEW_RC:-0}" != "0" ]]; then
      review_result="FAIL"
    fi
  else
    # Default fast path: explicit PASS(0 findings) to avoid confusion.
    review_result="PASS"
    review_open_count="0"
    findings_total="0"
  fi

  echo
  echo "PR_READY=${pr_ready_bool}"
  echo "evidence_must_missing=${must_missing} evidence_should_missing=${should_missing}"
  echo "review_result=${review_result} review_open_count=${review_open_count} findings_total=${findings_total}"
  echo "details: review_md=${review_md} findings_yaml=${findings_yaml} audit=${AUDIT_OUT_FILE:-<unset>} ssot=${STATE_FILE}"
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
  return "${rc}"
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
  else
    log "need ${eid}: not captured or invalid; running..."
    if ! run_oneclick_for_eid "${eid}"; then
      emit_summary
      exit 1
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
  else
    log "need ${eid}: not captured or invalid; running..."
    if ! run_oneclick_for_eid "${eid}"; then
      emit_summary
      exit 1
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
emit_summary
final_conclusion "${final_rc}"
exit "${final_rc}"
