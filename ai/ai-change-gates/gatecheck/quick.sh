#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
STATE_FILE="${ROOT}/ai/projects/topru-ai/PROJECT_STATE.md"
STATE_REL="ai/projects/topru-ai/PROJECT_STATE.md"
RUN_RECORD_DIR="${ROOT}/ai/ai-change-gates/runs"
RUN_RECORD_TEMPLATE="${ROOT}/ai/ai-change-gates/templates/run_record.template.json"
RUN_RECORD_SCHEMA="${ROOT}/ai/ai-change-gates/contracts/run_record.schema.json"
FAIL_CODES_FILE="${ROOT}/ai/ai-change-gates/contracts/fail_codes.json"
JSON_OUT=""

LATEST_RUN_REL=""
LAST_RUN_REL=""
LAST_RUN_JSON_ABS=""
SSOT_BLOCK=""

FAIL_TMP="$(mktemp /tmp/quick_fail.XXXXXX)"
WARN_TMP="$(mktemp /tmp/quick_warn.XXXXXX)"
trap 'rm -f "${FAIL_TMP}" "${WARN_TMP}"' EXIT

usage() {
  cat <<'USAGE'
Usage:
  bash ai/ai-change-gates/gatecheck/quick.sh [--json-out <path>]
USAGE
}

die() {
  echo "[quick][FATAL] $*" >&2
  exit 2
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

json_quote() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  s="${s//$'\r'/\\r}"
  s="${s//$'\t'/\\t}"
  printf '"%s"' "${s}"
}

to_rel_path() {
  local p="$1"
  if [[ "${p}" == "${ROOT}/"* ]]; then
    printf '%s' "${p#${ROOT}/}"
  else
    printf '%s' "${p}"
  fi
}

meta_field() {
  local code="$1"
  local field="$2"
  jq -r --arg c "${code}" --arg f "${field}" '.codes[] | select(.code==$c) | .[$f]' "${FAIL_CODES_FILE}" | head -n1
}

code_exists() {
  local code="$1"
  grep -q "\"code\":\"${code}\"" "${FAIL_TMP}" 2>/dev/null && return 0
  grep -q "\"code\":\"${code}\"" "${WARN_TMP}" 2>/dev/null && return 0
  return 1
}

add_issue() {
  local code="$1"
  local detail="$2"
  local title severity min_fix obj

  [[ -f "${FAIL_CODES_FILE}" ]] || die "fail codes contract missing: ${FAIL_CODES_FILE}"
  code_exists "${code}" && return 0

  title="$(meta_field "${code}" "title")"
  severity="$(meta_field "${code}" "severity")"
  min_fix="$(meta_field "${code}" "min_fix")"

  [[ -n "${title}" ]] || die "unknown fail code: ${code}"
  [[ -n "${severity}" ]] || die "missing severity for code: ${code}"
  [[ -n "${min_fix}" ]] || die "missing min_fix for code: ${code}"

  obj="$(jq -nc \
    --arg code "${code}" \
    --arg title "${title}" \
    --arg severity "${severity}" \
    --arg min_fix "${min_fix}" \
    --arg detail "${detail}" \
    '{code:$code,title:$title,severity:$severity,min_fix:$min_fix,detail:$detail}')"

  if [[ "${severity}" == "warn" ]]; then
    echo "${obj}" >> "${WARN_TMP}"
  else
    echo "${obj}" >> "${FAIL_TMP}"
  fi
}

ssot_block() {
  awk '
    /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inblk=1; next}
    /<!-- NAVIGATOR:END SSOT_V2 -->/ {inblk=0}
    inblk==1 {print}
  ' "$1"
}

extract_last_run() {
  local block="$1"
  local lr
  lr="$(printf '%s\n' "${block}" | awk '
    /^last_run:[[:space:]]*/ {
      sub(/^last_run:[[:space:]]*/, "", $0)
      gsub(/^["\047]|["\047]$/, "", $0)
      print
      exit
    }
  ')"
  if [[ -n "${lr}" ]]; then
    echo "${lr}"
    return 0
  fi

  awk '
    /^- last_run:[[:space:]]*/ {
      sub(/^- last_run:[[:space:]]*/, "", $0)
      gsub(/^["\047]|["\047]$/, "", $0)
      print
      exit
    }
  ' "${STATE_FILE}"
}

latest_run_json_rel() {
  local latest
  latest="$(find "${RUN_RECORD_DIR}" -maxdepth 1 -type f -name '*_run-*.json' | sort | tail -n1 || true)"
  if [[ -n "${latest}" ]]; then
    to_rel_path "${latest}"
  fi
}

count_next_actions_in_ssot() {
  local block="$1"
  printf '%s\n' "${block}" | awk '
    /^next_actions:[[:space:]]*$/ {insec=1; next}
    insec && /^[A-Za-z0-9_]+:[[:space:]]*$/ {insec=0}
    insec && /^  - / {c++}
    END {print c+0}
  '
}

count_next_actions_without_evidence() {
  local block="$1"
  printf '%s\n' "${block}" | awk '
    /^next_actions:[[:space:]]*$/ {insec=1; next}
    insec && /^[A-Za-z0-9_]+:[[:space:]]*$/ {
      if (item==1 && has_evidence==0) missing++
      insec=0
      next
    }
    insec && /^  - / {
      if (item==1 && has_evidence==0) missing++
      item=1
      has_evidence=0
      next
    }
    insec && /^    (produces|evidence|verify|verification):/ {has_evidence=1}
    END {
      if (insec==1 && item==1 && has_evidence==0) missing++
      print missing+0
    }
  '
}

count_bad_assumptions() {
  local block="$1"
  printf '%s\n' "${block}" | awk '
    /^assumptions:[[:space:]]*$/ {insec=1; next}
    insec && /^[A-Za-z0-9_]+:[[:space:]]*$/ {
      if (item==1 && (open==1 || owner==0 || deadline==0)) bad++
      insec=0
      next
    }
    insec && /^  - / {
      if (item==1 && (open==1 || owner==0 || deadline==0)) bad++
      item=1
      open=0
      owner=0
      deadline=0
      next
    }
    insec && /^    status:[[:space:]]*Open([[:space:]]|$)/ {open=1}
    insec && /^    owner:[[:space:]]*[^[:space:]]+/ {owner=1}
    insec && /^    deadline:[[:space:]]*[^[:space:]]+/ {deadline=1}
    END {
      if (insec==1 && item==1 && (open==1 || owner==0 || deadline==0)) bad++
      print bad+0
    }
  '
}

check_contract_assets() {
  local missing=()
  [[ -f "${RUN_RECORD_TEMPLATE}" ]] || missing+=("${RUN_RECORD_TEMPLATE}")
  [[ -f "${RUN_RECORD_SCHEMA}" ]] || missing+=("${RUN_RECORD_SCHEMA}")
  [[ -f "${FAIL_CODES_FILE}" ]] || missing+=("${FAIL_CODES_FILE}")

  if [[ ${#missing[@]} -gt 0 ]]; then
    add_issue "E_SCHEMA_INVALID" "missing files: ${missing[*]}"
    return 0
  fi

  jq -e . "${RUN_RECORD_TEMPLATE}" >/dev/null 2>&1 || add_issue "E_SCHEMA_INVALID" "invalid json: ${RUN_RECORD_TEMPLATE}"
  jq -e . "${RUN_RECORD_SCHEMA}" >/dev/null 2>&1 || add_issue "E_SCHEMA_INVALID" "invalid json: ${RUN_RECORD_SCHEMA}"
  jq -e . "${FAIL_CODES_FILE}" >/dev/null 2>&1 || add_issue "E_SCHEMA_INVALID" "invalid json: ${FAIL_CODES_FILE}"
}

check_patch_scope() {
  local line path oldp newp
  local bad=()

  while IFS= read -r line; do
    [[ -n "${line}" ]] || continue
    path="${line:3}"

    if [[ "${path}" == *" -> "* ]]; then
      oldp="${path%% -> *}"
      newp="${path##* -> }"
      for p in "${oldp}" "${newp}"; do
        [[ -n "${p}" ]] || continue
        if [[ "${p}" == ai/ai-change-gates/* || "${p}" == "${STATE_REL}" ]]; then
          continue
        fi
        bad+=("${p}")
      done
      continue
    fi

    if [[ "${path}" == ai/ai-change-gates/* || "${path}" == "${STATE_REL}" ]]; then
      continue
    fi
    bad+=("${path}")
  done < <(git -C "${ROOT}" status --porcelain)

  if [[ ${#bad[@]} -gt 0 ]]; then
    add_issue "E_PATCH_SCOPE" "out-of-allowlist paths: ${bad[*]}"
  fi
}

check_ssot_schema_and_rules() {
  local missing=()
  local action_count missing_evidence bad_assumptions

  [[ -f "${STATE_FILE}" ]] || { add_issue "E_SCHEMA_INVALID" "state file missing: ${STATE_FILE}"; return 0; }

  SSOT_BLOCK="$(ssot_block "${STATE_FILE}")"
  if [[ -z "${SSOT_BLOCK}" ]]; then
    add_issue "E_SCHEMA_INVALID" "SSOT_V2 block missing or empty"
    return 0
  fi

  for k in state claims findings evidence next_actions review pr_ready last_run; do
    printf '%s\n' "${SSOT_BLOCK}" | grep -Eq "^${k}:[[:space:]]*" || missing+=("${k}")
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    add_issue "E_SCHEMA_INVALID" "SSOT missing keys: ${missing[*]}"
  fi

  action_count="$(count_next_actions_in_ssot "${SSOT_BLOCK}")"
  if [[ "${action_count}" =~ ^[0-9]+$ ]] && [[ "${action_count}" -gt 3 ]]; then
    add_issue "E_TOO_MANY_ACTIONS" "SSOT next_actions count=${action_count}"
  fi

  missing_evidence="$(count_next_actions_without_evidence "${SSOT_BLOCK}")"
  if [[ "${missing_evidence}" =~ ^[0-9]+$ ]] && [[ "${missing_evidence}" -gt 0 ]]; then
    add_issue "E_NO_EVIDENCE" "next_actions missing evidence/verification fields: ${missing_evidence}"
  fi

  bad_assumptions="$(count_bad_assumptions "${SSOT_BLOCK}")"
  if [[ "${bad_assumptions}" =~ ^[0-9]+$ ]] && [[ "${bad_assumptions}" -gt 0 ]]; then
    add_issue "E_OPEN_ASSUMPTION" "assumptions open/missing owner/deadline: ${bad_assumptions}"
  fi

  if printf '%s\n' "${SSOT_BLOCK}" | grep -Eiq 'risk' && ! printf '%s\n' "${SSOT_BLOCK}" | grep -Eiq 'mitigation'; then
    add_issue "W_RISK_NO_MITIGATION" "risk marker exists without mitigation text"
  fi
}

check_run_record_presence_and_pointer() {
  local latest_rel

  latest_rel="$(latest_run_json_rel)"
  LATEST_RUN_REL="${latest_rel}"
  if [[ -z "${latest_rel}" ]]; then
    add_issue "E_NO_RUN_RECORD" "no run record json found under ${RUN_RECORD_DIR}"
  fi

  LAST_RUN_REL="$(extract_last_run "${SSOT_BLOCK}")"
  if [[ -z "${LAST_RUN_REL}" ]]; then
    add_issue "E_MISSING_LAST_RUN" "last_run missing in SSOT"
    return 0
  fi

  if [[ "${LAST_RUN_REL}" != ai/ai-change-gates/runs/* ]]; then
    add_issue "E_MISSING_LAST_RUN" "last_run outside runs/: ${LAST_RUN_REL}"
    return 0
  fi

  LAST_RUN_JSON_ABS="${ROOT}/${LAST_RUN_REL}"
  [[ -f "${LAST_RUN_JSON_ABS}" ]] || add_issue "E_MISSING_LAST_RUN" "last_run json not found: ${LAST_RUN_REL}"

  if [[ -n "${LATEST_RUN_REL}" && "${LAST_RUN_REL}" != "${LATEST_RUN_REL}" ]]; then
    add_issue "E_MISSING_LAST_RUN" "last_run not latest: current=${LAST_RUN_REL}, latest=${LATEST_RUN_REL}"
  fi
}

check_run_record_contract() {
  local run_actions

  [[ -n "${LAST_RUN_JSON_ABS}" ]] || return 0
  [[ -f "${LAST_RUN_JSON_ABS}" ]] || return 0

  jq -e '
    (.run_id | type == "string" and length > 0) and
    (.timestamp | type == "string" and length > 0) and
    (.trigger | type == "string" and length > 0) and
    (.inputs.ssot_path | type == "string" and length > 0) and
    (.inputs.ssot_hash | type == "string" and length > 0) and
    (.inputs.read_files | type == "array") and
    (.outputs.written_files | type == "array") and
    (.roles.navigator | type == "object") and
    (.roles.patch | type == "object") and
    (.verifier.status | test("^(pass|fail)$")) and
    (.verifier.fail_codes | type == "array") and
    (.latency_ms.navigator | type == "number") and
    (.latency_ms.patch | type == "number") and
    (.latency_ms.verifier | type == "number") and
    (.next_actions | type == "array")
  ' "${LAST_RUN_JSON_ABS}" >/dev/null 2>&1 || add_issue "E_BAD_RUN_RECORD" "minimum contract failed: ${LAST_RUN_REL}"

  jq -e --arg ssot_rel "${STATE_REL}" --arg run_rel "${LAST_RUN_REL}" '
    [ .outputs.written_files[].path ] as $paths |
    ($paths | index($ssot_rel)) != null and
    ($paths | index($run_rel)) != null
  ' "${LAST_RUN_JSON_ABS}" >/dev/null 2>&1 || add_issue "E_BAD_RUN_RECORD" "written_files missing ssot/run json paths"

  run_actions="$(jq -r '.next_actions | length' "${LAST_RUN_JSON_ABS}" 2>/dev/null || echo 0)"
  if [[ "${run_actions}" =~ ^[0-9]+$ ]] && [[ "${run_actions}" -gt 3 ]]; then
    add_issue "E_TOO_MANY_ACTIONS" "run_record next_actions count=${run_actions}"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --json-out)
      [[ $# -ge 2 ]] || die "--json-out requires a value"
      JSON_OUT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown arg: $1"
      ;;
  esac
done

need_cmd git
need_cmd jq

check_contract_assets
check_patch_scope
check_ssot_schema_and_rules
check_run_record_presence_and_pointer
check_run_record_contract

fail_arr="$(jq -s '.' "${FAIL_TMP}")"
warn_arr="$(jq -s '.' "${WARN_TMP}")"
fail_count="$(echo "${fail_arr}" | jq 'length')"
warn_count="$(echo "${warn_arr}" | jq 'length')"

status="pass"
if [[ "${fail_count}" -gt 0 ]]; then
  status="fail"
elif [[ "${warn_count}" -gt 0 ]]; then
  status="warn"
fi

result_json="$(jq -n \
  --arg status "${status}" \
  --arg checked_at "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
  --arg state_file "${STATE_REL}" \
  --arg last_run "${LAST_RUN_REL}" \
  --arg latest_run "${LATEST_RUN_REL}" \
  --argjson fail_codes "${fail_arr}" \
  --argjson warnings "${warn_arr}" \
  '{
    status: $status,
    checked_at: $checked_at,
    state_file: $state_file,
    last_run: $last_run,
    latest_run: $latest_run,
    fail_codes: $fail_codes,
    warnings: $warnings
  }')"

if [[ -n "${JSON_OUT}" ]]; then
  mkdir -p "$(dirname "${JSON_OUT}")"
  echo "${result_json}" > "${JSON_OUT}"
fi

echo "[quick] status=${status} blockers=${fail_count} warnings=${warn_count}"
if [[ "${fail_count}" -gt 0 ]]; then
  echo "[quick] blockers:"
  echo "${result_json}" | jq -r '.fail_codes[] | "- \(.code): \(.title) | MIN_FIX: \(.min_fix)"'
fi
if [[ "${warn_count}" -gt 0 ]]; then
  echo "[quick] warnings:"
  echo "${result_json}" | jq -r '.warnings[] | "- \(.code): \(.title) | MIN_FIX: \(.min_fix)"'
fi

echo "===GATECHECK_JSON_BEGIN==="
echo "${result_json}"
echo "===GATECHECK_JSON_END==="

if [[ "${status}" == "fail" ]]; then
  exit 1
fi
exit 0
