#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="ai/projects/topru-ai"
TRACK_ID="resource-observability-topru"
PROTOCOL_ROOT="ai/ai-change-gates"

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

source "ai/projects/topru-ai/verify/lib_oneclick.sh"

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
  log "running oneclick for ${eid}: ${script}"
  bash "${script}"
}

log "PR-ready navigator start"
log "track=${TRACK_ID}"
log "must=[${MUST_RAW}]"
log "should=[${SHOULD_RAW}]"
log "conditional=[${COND_RAW}]"

# Run MUST first
for eid in ${MUST_LIST}; do
  if evidence_is_truly_captured "${eid}"; then
    log "skip ${eid}: already captured (valid)"
  else
    log "need ${eid}: not captured or invalid; running..."
    run_oneclick_for_eid "${eid}"
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
    log "skip ${eid}: already captured (valid)"
  else
    log "need ${eid}: not captured or invalid; running..."
    run_oneclick_for_eid "${eid}"
    SSOT=$(
      awk '
        /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inblk=1; next}
        /<!-- NAVIGATOR:END SSOT_V2 -->/ {inblk=0}
        inblk==1 {print}
      ' "${STATE_FILE}"
    )
  fi
done

# Finally audit + patch pr_ready block back into SSOT
log "final audit + patch pr_ready ..."
bash "ai/projects/topru-ai/verify/audit_ssot.sh" --patch

log "DONE"

