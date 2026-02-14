#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
FAIL_CODES_FILE="${ROOT}/ai/ai-change-gates/contracts/fail_codes.json"
ARTIFACTS_DIR=""
MODE="quick"
ALLOW_OPEN_MUST_FIX="0"
JSON_OUT=""

FAILS_TMP="$(mktemp /tmp/review_pack_fail.XXXXXX)"
trap 'rm -f "${FAILS_TMP}"' EXIT

usage() {
  cat <<'USAGE'
Usage:
  bash ai/ai-change-gates/gatecheck/review_pack.sh \
    --artifacts-dir <dir> \
    [--mode quick|full] \
    [--allow-open-must-fix] \
    [--json-out <path>]
USAGE
}

die() {
  echo "[review_pack][ERROR] $*" >&2
  exit 2
}

min_fix_of() {
  local code="$1"
  if [[ -f "${FAIL_CODES_FILE}" ]]; then
    jq -r --arg c "${code}" '.codes[] | select(.code==$c) | .min_fix' "${FAIL_CODES_FILE}" 2>/dev/null | head -n1
  else
    echo ""
  fi
}

add_fail() {
  local code="$1"
  local detail="$2"
  local min_fix
  min_fix="$(min_fix_of "${code}")"
  [[ -n "${min_fix}" ]] || min_fix="See ai/ai-change-gates/contracts/fail_codes.json"
  echo "$(jq -nc --arg code "${code}" --arg detail "${detail}" --arg min_fix "${min_fix}" '{code:$code,detail:$detail,min_fix:$min_fix}')" >> "${FAILS_TMP}"
}

check_presence() {
  local review_md="$1"
  local findings_yaml="$2"
  local missing=()

  [[ -f "${review_md}" ]] || missing+=("review.md missing")
  [[ -s "${review_md}" ]] || missing+=("review.md empty")
  [[ -f "${findings_yaml}" ]] || missing+=("findings.yaml missing")
  [[ -s "${findings_yaml}" ]] || missing+=("findings.yaml empty")

  if [[ ${#missing[@]} -gt 0 ]]; then
    add_fail "REVIEW_PACK_MISSING_FILES" "${missing[*]}"
    return 1
  fi
  return 0
}

check_changed_files_consistency() {
  local changed_files="$1"
  local diff_patch="$2"
  local changed_count=0

  if [[ -f "${changed_files}" ]]; then
    changed_count="$(sed '/^[[:space:]]*$/d' "${changed_files}" | wc -l | tr -d ' ' )"
  fi
  [[ "${changed_count}" =~ ^[0-9]+$ ]] || changed_count=0

  if [[ -s "${diff_patch}" && "${changed_count}" -eq 0 ]]; then
    add_fail "REVIEW_PACK_CHANGED_FILES_EMPTY" "diff.patch has content but changed_files.txt is empty/missing"
    return 1
  fi
  return 0
}

check_layers() {
  local review_md="$1"
  local l1="0" l2="0" l3="0"

  if grep -Eiq '^##[[:space:]]*1([[:space:])]|$)' "${review_md}" || grep -Eiq '^##[[:space:]]*summary([[:space:]]|$)' "${review_md}"; then
    l1="1"
  fi
  if grep -Eiq '^##[[:space:]]*2([[:space:])]|$)' "${review_md}" || grep -Eiq '^##[[:space:]]*review[[:space:]]+strategy([[:space:]]|$)' "${review_md}"; then
    l2="1"
  fi
  if grep -Eiq '^##[[:space:]]*3([[:space:])]|$)' "${review_md}" || grep -Eiq '^##[[:space:]]*findings([[:space:]]|$)' "${review_md}"; then
    l3="1"
  fi

  if [[ "${l1}" != "1" || "${l2}" != "1" || "${l3}" != "1" ]]; then
    add_fail "REVIEW_PACK_MISSING_LAYERS" "missing layers: layer1=${l1} layer2=${l2} layer3=${l3}"
    return 1
  fi
  return 0
}

check_findings_yaml() {
  local findings_yaml="$1"
  local parse_out open_must

  parse_out="$(awk '
    BEGIN { idx=0; bad=0; open_must=0 }

    /^[[:space:]]*-[[:space:]]*id:[[:space:]]*/ {
      idx++
      has_id[idx]=1
      id_val=$0
      sub(/^[[:space:]]*-[[:space:]]*id:[[:space:]]*/, "", id_val)
      gsub(/"/, "", id_val)
      item_id[idx]=id_val
      next
    }

    idx > 0 {
      if ($0 ~ /^[[:space:]]*fingerprint:[[:space:]]*/) has_fp[idx]=1
      if ($0 ~ /^[[:space:]]*status:[[:space:]]*/) {
        has_status[idx]=1
        val=$0
        sub(/^[[:space:]]*status:[[:space:]]*/, "", val)
        gsub(/"/, "", val)
        status[idx]=tolower(val)
      }
      if ($0 ~ /^[[:space:]]*must_fix:[[:space:]]*/) {
        has_mf[idx]=1
        val=$0
        sub(/^[[:space:]]*must_fix:[[:space:]]*/, "", val)
        gsub(/"/, "", val)
        val=tolower(val)
        if (val == "yes") val="true"
        if (val == "no") val="false"
        must_fix[idx]=val
      }
      if ($0 ~ /^[[:space:]]*locations:[[:space:]]*/) has_locations[idx]=1
      if ($0 ~ /^[[:space:]]*closure:[[:space:]]*/) has_closure[idx]=1
    }

    END {
      if (idx == 0) {
        print "NO_ITEMS"
        exit 0
      }

      for (i=1; i<=idx; i++) {
        missing=""
        if (!has_id[i]) missing=missing "id,"
        if (!has_fp[i]) missing=missing "fingerprint,"
        if (!has_status[i]) missing=missing "status,"
        if (!has_mf[i]) missing=missing "must_fix,"
        if (!has_locations[i]) missing=missing "locations,"
        if (!has_closure[i]) missing=missing "closure,"
        if (missing != "") {
          sub(/,$/, "", missing)
          print "MISSING\t" i "\t" missing "\t" item_id[i]
          bad=1
        }

        if (must_fix[i] == "true" && status[i] == "open") {
          open_must++
        }
      }

      print "OPEN_MUST\t" open_must
      if (bad == 1) {
        exit 3
      }
    }
  ' "${findings_yaml}" 2>&1)"

  if echo "${parse_out}" | grep -q '^NO_ITEMS$'; then
    echo "0"
    return 0
  fi

  if echo "${parse_out}" | grep -q '^MISSING'; then
    while IFS= read -r line; do
      [[ "${line}" == MISSING* ]] || continue
      add_fail "REVIEW_PACK_FINDINGS_SCHEMA_INVALID" "${line}"
    done <<< "${parse_out}"
    open_must="$(echo "${parse_out}" | awk -F'\t' '/^OPEN_MUST/{print $2; exit}')"
    echo "${open_must:-0}"
    return 1
  fi

  open_must="$(echo "${parse_out}" | awk -F'\t' '/^OPEN_MUST/{print $2; exit}')"
  echo "${open_must:-0}"
  return 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --artifacts-dir)
      [[ $# -ge 2 ]] || die "--artifacts-dir requires a value"
      ARTIFACTS_DIR="$2"
      shift 2
      ;;
    --mode)
      [[ $# -ge 2 ]] || die "--mode requires a value"
      MODE="$2"
      shift 2
      ;;
    --allow-open-must-fix)
      ALLOW_OPEN_MUST_FIX="1"
      shift
      ;;
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

[[ -n "${ARTIFACTS_DIR}" ]] || die "--artifacts-dir is required"
[[ "${MODE}" == "quick" || "${MODE}" == "full" ]] || die "--mode must be quick|full"
command -v jq >/dev/null 2>&1 || die "missing command: jq"

REVIEW_MD="${ARTIFACTS_DIR}/review.md"
FINDINGS_YAML="${ARTIFACTS_DIR}/findings.yaml"
CHANGED_FILES="${ARTIFACTS_DIR}/changed_files.txt"
DIFF_PATCH="${ARTIFACTS_DIR}/diff.patch"

check_presence "${REVIEW_MD}" "${FINDINGS_YAML}" || true
check_changed_files_consistency "${CHANGED_FILES}" "${DIFF_PATCH}" || true
if [[ -s "${REVIEW_MD}" ]]; then
  check_layers "${REVIEW_MD}" || true
fi

OPEN_MUST_FIX_COUNT=0
if [[ -s "${FINDINGS_YAML}" ]]; then
  OPEN_MUST_FIX_COUNT="$(check_findings_yaml "${FINDINGS_YAML}" || true)"
fi
[[ "${OPEN_MUST_FIX_COUNT}" =~ ^[0-9]+$ ]] || OPEN_MUST_FIX_COUNT=0

if [[ "${ALLOW_OPEN_MUST_FIX}" != "1" && "${OPEN_MUST_FIX_COUNT}" -gt 0 ]]; then
  add_fail "REVIEW_PACK_OPEN_MUST_FIX" "open must_fix findings count=${OPEN_MUST_FIX_COUNT}"
fi

fails_json="$(jq -s '.' "${FAILS_TMP}")"
fail_count="$(echo "${fails_json}" | jq 'length')"
status="pass"
if [[ "${fail_count}" -gt 0 ]]; then
  status="fail"
fi

REVIEW_ARTIFACTS_DIR="${ARTIFACTS_DIR}"
if [[ "${REVIEW_ARTIFACTS_DIR}" == "${ROOT}/"* ]]; then
  REVIEW_ARTIFACTS_DIR="${REVIEW_ARTIFACTS_DIR#${ROOT}/}"
fi

INPUT_BASE=""
INPUT_HEAD=""
INPUT_PATCH_ID=""
MANIFEST_FILE="${ARTIFACTS_DIR}/manifest.json"
if [[ -f "${MANIFEST_FILE}" ]]; then
  INPUT_BASE="$(jq -r '.base // ""' "${MANIFEST_FILE}" 2>/dev/null || true)"
  INPUT_HEAD="$(jq -r '.head // ""' "${MANIFEST_FILE}" 2>/dev/null || true)"
  INPUT_PATCH_ID="$(jq -r '.patch_id // ""' "${MANIFEST_FILE}" 2>/dev/null || true)"
fi

result_json="$(jq -n \
  --arg status "${status}" \
  --arg artifacts_dir "${ARTIFACTS_DIR}" \
  --arg mode "${MODE}" \
  --arg review_artifacts_dir "${REVIEW_ARTIFACTS_DIR}" \
  --arg base "${INPUT_BASE}" \
  --arg head "${INPUT_HEAD}" \
  --arg patch_id "${INPUT_PATCH_ID}" \
  --argjson allow_open_must_fix "$( [[ "${ALLOW_OPEN_MUST_FIX}" == "1" ]] && echo true || echo false )" \
  --argjson open_must_fix_count "${OPEN_MUST_FIX_COUNT}" \
  --arg checked_at "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
  --argjson fail_codes "${fails_json}" \
  '{status:$status,artifacts_dir:$artifacts_dir,mode:$mode,inputs:{review_artifacts_dir:$review_artifacts_dir,base:$base,head:$head,patch_id:$patch_id},allow_open_must_fix:$allow_open_must_fix,open_must_fix_count:$open_must_fix_count,checked_at:$checked_at,fail_codes:$fail_codes}')"

if [[ -n "${JSON_OUT}" ]]; then
  mkdir -p "$(dirname "${JSON_OUT}")"
  echo "${result_json}" > "${JSON_OUT}"
fi

if [[ "${status}" == "pass" ]]; then
  echo "REVIEW_PACK_GATE=PASS"
else
  echo "REVIEW_PACK_GATE=FAIL"
  echo "${result_json}" | jq -r '.fail_codes[] | "- \(.code): \(.detail) | MIN_FIX: \(.min_fix)"'
fi

echo "===GATECHECK_JSON_BEGIN==="
echo "${result_json}"
echo "===GATECHECK_JSON_END==="

if [[ "${status}" == "fail" ]]; then
  exit 1
fi
exit 0
