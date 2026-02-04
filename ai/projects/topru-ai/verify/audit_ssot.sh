#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="ai/projects/topru-ai"
MODE="read"   # read | patch
TRACK_ID="resource-observability-topru"
PROTOCOL_ROOT="ai/ai-change-gates"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT_ROOT="$2"; shift 2;;
    --track) TRACK_ID="$2"; shift 2;;
    --protocol-root) PROTOCOL_ROOT="$2"; shift 2;;
    --patch) MODE="patch"; shift;;
    *) echo "Unknown arg: $1"; exit 2;;
  esac
done

STATE_FILE="${PROJECT_ROOT}/PROJECT_STATE.md"
TRACK_FILE="${PROTOCOL_ROOT}/tracks/${TRACK_ID}/TRACK.md"
OUT_DIR="${PROJECT_ROOT}/artifacts/audit"
OUT_FILE="${OUT_DIR}/last_audit.txt"
mkdir -p "${OUT_DIR}"

if [[ ! -f "${STATE_FILE}" ]]; then
  echo "ERROR: ${STATE_FILE} not found" | tee "${OUT_FILE}"
  exit 2
fi

# Extract SSOT_V2 block
SSOT=$(
  awk '
    /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inblk=1; next}
    /<!-- NAVIGATOR:END SSOT_V2 -->/ {inblk=0}
    inblk==1 {print}
  ' "${STATE_FILE}"
)
if [[ -z "${SSOT}" ]]; then
  echo "ERROR: SSOT_V2 block not found in ${STATE_FILE}" | tee "${OUT_FILE}"
  exit 3
fi

# Helper: get track pr_ready lists (comma-separated IDs inside brackets)
get_track_list() {
  local key="$1"
  if [[ -f "${TRACK_FILE}" ]]; then
    grep -E "^${key}=" "${TRACK_FILE}" | sed -E "s/^${key}=\\[//; s/\\]\$//; s/[[:space:]]//g" || true
  fi
}

MUST_RAW="$(get_track_list pr_ready_must)"
SHOULD_RAW="$(get_track_list pr_ready_should)"
COND_RAW="$(get_track_list pr_ready_conditional)"

# Convert comma-separated to lines
to_lines() { echo "$1" | tr ',' '\n' | sed '/^$/d'; }

MUST_LIST="$(to_lines "${MUST_RAW}")"
SHOULD_LIST="$(to_lines "${SHOULD_RAW}")"
COND_LIST="$(to_lines "${COND_RAW}")"

# Helper: evidence status lookup (best-effort)
# We look for blocks like:
# - id: E_func
#   status: Captured
evidence_status() {
  local eid="$1"
  echo "${SSOT}" | awk -v id="${eid}" '
    $0 ~ "^[[:space:]]*-[[:space:]]*id:[[:space:]]*"id"([[:space:]]*$|[[:space:]]*#)" {found=1}
    found==1 && $0 ~ "^[[:space:]]*status:" {
      gsub(/^[[:space:]]*status:[[:space:]]*/, "", $0)
      gsub(/"/, "", $0)
      print $0
      exit
    }
  ' 2>/dev/null || true
}


missing_must=()
missing_should=()

for eid in ${MUST_LIST}; do
  st="$(evidence_status "${eid}")"
  if [[ "${st}" != "Captured" ]]; then
    missing_must+=("${eid}")
  fi
done

for eid in ${SHOULD_LIST}; do
  st="$(evidence_status "${eid}")"
  if [[ "${st}" != "Captured" ]]; then
    missing_should+=("${eid}")
  fi
done

pr_status="true"
if [[ "${#missing_must[@]}" -gt 0 ]]; then
  pr_status="false"
fi

# Output audit summary
{
  echo "SSOT_AUDIT"
  echo "project_root: ${PROJECT_ROOT}"
  echo "track: ${TRACK_ID}"
  echo "time_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "pr_ready_status: ${pr_status}"
  echo "missing_must: [$(IFS=,; echo "${missing_must[*]-}")]"
  echo "missing_should: [$(IFS=,; echo "${missing_should[*]-}")]"
  echo
  echo "NOTE: Run with --patch to write pr_ready fields back into SSOT."
} | tee "${OUT_FILE}"

echo "Wrote audit report: ${OUT_FILE}"

if [[ "${MODE}" != "patch" ]]; then
  exit 0
fi

# Patch PROJECT_STATE.md: update pr_ready section inside SSOT_V2
# Best-effort text patch: replace pr_ready block if present, otherwise append.
new_pr_block=$(
  cat <<EOF
pr_ready:
  from_track: true
  status: ${pr_status}
  must: [$(IFS=,; echo "${MUST_RAW}")]
  should: [$(IFS=,; echo "${SHOULD_RAW}")]
  conditional: [$(IFS=,; echo "${COND_RAW}")]
  missing: [$(IFS=,; echo "${missing_must[*]-}")]
  missing_must: [$(IFS=,; echo "${missing_must[*]-}")]
  missing_should: [$(IFS=,; echo "${missing_should[*]-}")]
  notes: "auto-updated by audit_ssot.sh"
EOF
)

tmp="$(mktemp)"
nbf="$(mktemp)"

# write block to a temp file (avoid awk -v multiline issues)
printf "%s\n" "${new_pr_block}" > "${nbf}"

awk -v nbf="${nbf}" '
  function print_nb() {
    while ((getline line < nbf) > 0) print line
    close(nbf)
  }

  BEGIN {inssot=0; inpr=0; prfound=0}

  /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inssot=1; print; next}
  /<!-- NAVIGATOR:END SSOT_V2 -->/ {
    if (inssot==1 && prfound==0) { print_nb() }
    inssot=0; print; next
  }

  {
    if (inssot==1 && $0 ~ /^pr_ready:/) { prfound=1; inpr=1; print_nb(); next }

    if (inpr==1) {
      # stop skipping when next top-level key starts (non-indented) or end marker handled above
      if ($0 ~ /^[a-zA-Z0-9_]+:/ && $0 !~ /^pr_ready:/) { inpr=0; print; next }
      next
    }

    print
  }
' "${STATE_FILE}" > "${tmp}"

mv "${tmp}" "${STATE_FILE}"
rm -f "${nbf}"
echo "Patched pr_ready in SSOT: ${STATE_FILE}"

