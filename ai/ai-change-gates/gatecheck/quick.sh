#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
STATE_FILE="${ROOT}/ai/projects/topru-ai/PROJECT_STATE.md"
TOOLS_INDEX="${ROOT}/ai/ai-change-gates/tools.index.json"
SKILLS_INDEX="${ROOT}/ai/ai-change-gates/skills.index.json"
ONECLICK_SCRIPT="${ROOT}/ai/projects/topru-ai/verify/run_pr_ready_oneclick.sh"

fail() {
  echo "[quick][FAIL] $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

ssot_block() {
  awk '
    /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inblk=1; next}
    /<!-- NAVIGATOR:END SSOT_V2 -->/ {inblk=0}
    inblk==1 {print}
  ' "$1"
}

check_ssot() {
  [[ -f "${STATE_FILE}" ]] || fail "state file not found: ${STATE_FILE}"
  local block
  block="$(ssot_block "${STATE_FILE}")"
  [[ -n "${block}" ]] || fail "SSOT_V2 block missing or empty in ${STATE_FILE}"

  echo "${block}" | rg -q '^[[:space:]]*evidence:' || fail "SSOT_V2 missing key: evidence"
  echo "${block}" | rg -q '^[[:space:]]*review:' || fail "SSOT_V2 missing key: review"
  echo "${block}" | rg -q '^[[:space:]]*pr_ready:' || fail "SSOT_V2 missing key: pr_ready"
  echo "[quick][OK] ssot_v2 block present and keys found"
}

check_indexes() {
  [[ -f "${TOOLS_INDEX}" ]] || fail "missing tools index: ${TOOLS_INDEX}"
  [[ -f "${SKILLS_INDEX}" ]] || fail "missing skills index: ${SKILLS_INDEX}"

  jq -e . "${TOOLS_INDEX}" >/dev/null || fail "invalid JSON: ${TOOLS_INDEX}"
  jq -e . "${SKILLS_INDEX}" >/dev/null || fail "invalid JSON: ${SKILLS_INDEX}"
  echo "[quick][OK] index json parseable"
}

check_skill_specs() {
  local spec
  local found=0
  for spec in "${ROOT}"/ai/skills_src/*/SPEC.yaml; do
    [[ -f "${spec}" ]] || continue
    found=1
    rg -q '^name:' "${spec}" || fail "SPEC missing key: name in ${spec}"
    rg -q '^version:' "${spec}" || fail "SPEC missing key: version in ${spec}"
    rg -q '^description:' "${spec}" || fail "SPEC missing key: description in ${spec}"
    rg -q '^triggers:' "${spec}" || fail "SPEC missing key: triggers in ${spec}"
    rg -q '^outputs:' "${spec}" || fail "SPEC missing key: outputs in ${spec}"
    rg -q '^safety:' "${spec}" || fail "SPEC missing key: safety in ${spec}"
  done
  [[ "${found}" -eq 1 ]] || fail "no skill SPEC.yaml found under ${ROOT}/ai/skills_src"
  echo "[quick][OK] skill spec required keys found"
}

check_oneclick_marker() {
  [[ -f "${ONECLICK_SCRIPT}" ]] || fail "missing oneclick script: ${ONECLICK_SCRIPT}"
  rg -q '===ONECLICK_JSON_BEGIN===' "${ONECLICK_SCRIPT}" || fail "marker begin missing in ${ONECLICK_SCRIPT}"
  rg -q '===ONECLICK_JSON_END===' "${ONECLICK_SCRIPT}" || fail "marker end missing in ${ONECLICK_SCRIPT}"
  echo "[quick][OK] oneclick json markers found"
}

check_wt_version() {
  if command -v wt >/dev/null 2>&1; then
    wt --version >/dev/null || fail "wt --version failed"
    echo "[quick][OK] wt --version runnable from PATH"
    return 0
  fi

  local wt_src="${ROOT}/ai/ai-change-gates/tools/wt/wt"
  [[ -x "${wt_src}" ]] || fail "wt not found in PATH and source is not executable: ${wt_src}"
  "${wt_src}" --version >/dev/null || fail "source wt --version failed"
  echo "[quick][OK] wt --version runnable from source script (PATH install optional)"
}

check_danger_patterns() {
  local scan_targets=()
  [[ -d "${ROOT}/ai/ai-change-gates/scripts" ]] && scan_targets+=("${ROOT}/ai/ai-change-gates/scripts")
  [[ -d "${ROOT}/ai/ai-change-gates/prompts" ]] && scan_targets+=("${ROOT}/ai/ai-change-gates/prompts")
  [[ -d "${ROOT}/ai/scripts" ]] && scan_targets+=("${ROOT}/ai/scripts")
  [[ -d "${ROOT}/ai/prompts" ]] && scan_targets+=("${ROOT}/ai/prompts")

  if [[ ${#scan_targets[@]} -eq 0 ]]; then
    echo "[quick][OK] no scripts/prompts dirs to scan"
    return 0
  fi

  local findings
  findings="$(
    rg -n --no-heading \
      -e 'curl[[:space:]]*\|[[:space:]]*(bash|sh)\b' \
      -e 'rm[[:space:]]+-rf[[:space:]]+/' \
      "${scan_targets[@]}" || true
  )"
  if [[ -n "${findings}" ]]; then
    echo "${findings}" >&2
    fail "dangerous command pattern found in scripts/prompts"
  fi
  echo "[quick][OK] no dangerous command patterns in scripts/prompts"
}

main() {
  require_cmd jq
  require_cmd rg
  check_ssot
  check_indexes
  check_skill_specs
  check_oneclick_marker
  check_wt_version
  check_danger_patterns
  echo "[quick][PASS] all checks passed"
}

main "$@"
