#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
FAIL_CODES="${ROOT}/ai/ai-change-gates/contracts/fail_codes.json"
NAV_JSON=""
PATCH_TXT=""
IMPLEMENT_TXT=""
SSOT_PATH=""

FAILS_TMP="$(mktemp /tmp/role_outputs_fail.XXXXXX)"
trap 'rm -f "${FAILS_TMP}"' EXIT

usage() {
  cat <<'USAGE'
Usage:
  bash ai/ai-change-gates/gatecheck/role_outputs.sh \
    [--navigator-json <file>] \
    [--patch-output <file> --ssot-path <path>] \
    [--implement-output <file>]
USAGE
}

min_fix_of() {
  local code="$1"
  jq -r --arg c "${code}" '.codes[] | select(.code==$c) | .min_fix' "${FAIL_CODES}" 2>/dev/null | head -n1
}

add_fail() {
  local code="$1"
  local detail="$2"
  local min_fix
  min_fix="$(min_fix_of "${code}")"
  [[ -n "${min_fix}" ]] || min_fix="See role contracts and fix deterministic violations."
  echo "$(jq -nc --arg code "${code}" --arg detail "${detail}" --arg min_fix "${min_fix}" '{code:$code,detail:$detail,min_fix:$min_fix}')" >> "${FAILS_TMP}"
}

check_navigator() {
  [[ -n "${NAV_JSON}" ]] || return 0

  if ! jq -e . "${NAV_JSON}" >/dev/null 2>&1; then
    add_fail "E_SCHEMA_INVALID" "navigator output is not valid JSON"
    return 0
  fi

  jq -e '
    .role == "navigator" and
    (.state | type == "object") and
    (.next_actions | type == "array") and
    ((.next_actions | length) >= 1 and (.next_actions | length) <= 3) and
    (.risk_flags | type == "array") and
    (.assumptions | type == "array")
  ' "${NAV_JSON}" >/dev/null 2>&1 || add_fail "E_SCHEMA_INVALID" "navigator output missing required fields"

  jq -e '(.next_actions | length) <= 3' "${NAV_JSON}" >/dev/null 2>&1 || add_fail "E_TOO_MANY_ACTIONS" "navigator next_actions exceeds 3"

  jq -e '([.assumptions[]? | select(.status == "forbidden_open")] | length) == 0' "${NAV_JSON}" >/dev/null 2>&1 \
    || add_fail "E_OPEN_ASSUMPTION" "navigator assumptions contains forbidden_open"
}

check_patch() {
  [[ -n "${PATCH_TXT}" ]] || return 0

  [[ -n "${SSOT_PATH}" ]] || add_fail "E_PATCH_SCOPE" "--ssot-path is required when checking patch output"

  grep -q '^===PATCH_PLAN===$' "${PATCH_TXT}" || add_fail "E_SCHEMA_INVALID" "PATCH_PLAN marker missing"
  grep -q '^===END_PATCH_PLAN===$' "${PATCH_TXT}" || add_fail "E_SCHEMA_INVALID" "END_PATCH_PLAN marker missing"
  grep -q '^===PATCH_DIFF===$' "${PATCH_TXT}" || add_fail "E_SCHEMA_INVALID" "PATCH_DIFF marker missing"
  grep -q '^===END_PATCH_DIFF===$' "${PATCH_TXT}" || add_fail "E_SCHEMA_INVALID" "END_PATCH_DIFF marker missing"

  if [[ -n "${SSOT_PATH}" ]]; then
    while IFS= read -r p; do
      [[ -n "${p}" ]] || continue
      [[ "${p}" == "/dev/null" ]] && continue
      p="${p#a/}"
      p="${p#b/}"
      if [[ "${p}" == "${SSOT_PATH}" || "${p}" == ai/ai-change-gates/* ]]; then
        continue
      fi
      add_fail "E_PATCH_SCOPE" "patch path outside allowlist: ${p}"
    done < <(
      awk '/^===PATCH_DIFF===/{inblk=1;next}/^===END_PATCH_DIFF===/{inblk=0}inblk==1{print}' "${PATCH_TXT}" \
        | awk '/^\+\+\+ b\//{print substr($0,7)} /^--- a\//{print substr($0,7)} /^diff --git a\//{print $3 "\n" $4}'
    )
  fi
}

check_implement() {
  local a ta c p ea eta ec ep
  local outside

  [[ -n "${IMPLEMENT_TXT}" ]] || return 0

  a="$(grep -n '^===ASSUMPTIONS===$' "${IMPLEMENT_TXT}" | head -n1 | cut -d: -f1 || true)"
  ta="$(grep -n '^===TEST_PLAN===$' "${IMPLEMENT_TXT}" | head -n1 | cut -d: -f1 || true)"
  c="$(grep -n '^===CODE_DIFF===$' "${IMPLEMENT_TXT}" | head -n1 | cut -d: -f1 || true)"
  p="$(grep -n '^===PROOF===$' "${IMPLEMENT_TXT}" | head -n1 | cut -d: -f1 || true)"
  ea="$(grep -n '^===END_ASSUMPTIONS===$' "${IMPLEMENT_TXT}" | head -n1 | cut -d: -f1 || true)"
  eta="$(grep -n '^===END_TEST_PLAN===$' "${IMPLEMENT_TXT}" | head -n1 | cut -d: -f1 || true)"
  ec="$(grep -n '^===END_CODE_DIFF===$' "${IMPLEMENT_TXT}" | head -n1 | cut -d: -f1 || true)"
  ep="$(grep -n '^===END_PROOF===$' "${IMPLEMENT_TXT}" | head -n1 | cut -d: -f1 || true)"

  [[ -n "${a}" && -n "${ta}" && -n "${c}" && -n "${p}" ]] || add_fail "E_SCHEMA_INVALID" "implement section start marker missing"
  [[ -n "${ea}" && -n "${eta}" && -n "${ec}" && -n "${ep}" ]] || add_fail "E_SCHEMA_INVALID" "implement section end marker missing"

  if [[ -n "${a}" && -n "${ta}" && -n "${c}" && -n "${p}" ]]; then
    if ! { [[ "${a}" -lt "${ta}" ]] && [[ "${ta}" -lt "${c}" ]] && [[ "${c}" -lt "${p}" ]]; }; then
      add_fail "E_SCHEMA_INVALID" "implement section order must be ASSUMPTIONS->TEST_PLAN->CODE_DIFF->PROOF"
    fi
  fi

  outside="$(awk '
    /^===ASSUMPTIONS===$/,/^===END_ASSUMPTIONS===$/ {next}
    /^===TEST_PLAN===$/,/^===END_TEST_PLAN===$/ {next}
    /^===CODE_DIFF===$/,/^===END_CODE_DIFF===$/ {next}
    /^===PROOF===$/,/^===END_PROOF===$/ {next}
    {print}
  ' "${IMPLEMENT_TXT}" | sed '/^[[:space:]]*$/d')"

  [[ -z "${outside}" ]] || add_fail "E_SCHEMA_INVALID" "implement output contains text outside required sections"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --navigator-json)
      NAV_JSON="$2"; shift 2 ;;
    --patch-output)
      PATCH_TXT="$2"; shift 2 ;;
    --implement-output)
      IMPLEMENT_TXT="$2"; shift 2 ;;
    --ssot-path)
      SSOT_PATH="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

[[ -f "${FAIL_CODES}" ]] || { echo "missing fail codes: ${FAIL_CODES}" >&2; exit 2; }
command -v jq >/dev/null 2>&1 || { echo "missing command: jq" >&2; exit 2; }

check_navigator
check_patch
check_implement

fails_json="$(jq -s '.' "${FAILS_TMP}")"
count="$(echo "${fails_json}" | jq 'length')"
status="pass"
if [[ "${count}" -gt 0 ]]; then
  status="fail"
fi

result="$(jq -n --arg status "${status}" --argjson fail_codes "${fails_json}" '{status:$status,fail_codes:$fail_codes}')"
echo "${result}"
if [[ "${status}" == "fail" ]]; then
  exit 1
fi
