#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
RUNS_DIR_REL="ai/ai-change-gates/runs"
RUNS_DIR="${ROOT}/${RUNS_DIR_REL}"
FAIL_CODES_FILE="${ROOT}/ai/ai-change-gates/contracts/fail_codes.json"

SSOT_PATH=""
MODE="navigate_patch"
TRIGGER="local"
RUN_ID=""
DIFF_SUMMARY=""
NOTES=""
VERIFIER_STATUS="pass"
LAT_NAVIGATOR="0"
LAT_PATCH="0"
LAT_VERIFIER="0"
LAT_IMPLEMENT=""
NAVIGATOR_SUMMARY=""
NAVIGATOR_OUTPUT_REF=""
PATCH_SUMMARY=""
PATCH_OUTPUT_REF=""
IMPLEMENT_SUMMARY=""
IMPLEMENT_OUTPUT_REF=""

READ_FILES=()
WRITTEN_SPECS=()
FAIL_CODES=()
NEXT_ACTIONS=()

usage() {
  cat <<'USAGE'
Usage:
  bash ai/ai-change-gates/tools/run_record.sh \
    --ssot <path> \
    [--mode navigate|navigate_patch] \
    [--trigger manual|ci|local] \
    [--run-id <id>] \
    [--read-file <path>]... \
    [--written-file <path>]... \
    [--navigator-summary <text> | --navigator-output-ref <path>] \
    [--patch-summary <text> | --patch-output-ref <path>] \
    [--implement-summary <text> | --implement-output-ref <path>] \
    [--verifier-status pass|fail] [--verifier-fail-code <code>]... \
    [--latency-navigator <ms>] [--latency-patch <ms>] [--latency-verifier <ms>] [--latency-implement <ms>] \
    [--next-action <text>]... \
    [--diff-summary <text>] [--notes <text>]
USAGE
}

die() {
  echo "[run_record][ERROR] $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
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

to_rel_path() {
  local p="$1"
  if [[ "${p}" == "${ROOT}/"* ]]; then
    printf '%s' "${p#${ROOT}/}"
  else
    printf '%s' "${p}"
  fi
}

path_exists_in_array() {
  local want="$1"
  shift
  local v
  for v in "$@"; do
    [[ "${v}" == "${want}" ]] && return 0
  done
  return 1
}

sha256_of_file() {
  local f="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${f}" | awk '{print $1}'
  else
    shasum -a 256 "${f}" | awk '{print $1}'
  fi
}

trim_200() {
  local s="$1"
  if [[ ${#s} -le 200 ]]; then
    printf '%s' "${s}"
  else
    printf '%s' "${s:0:200}"
  fi
}

calc_numstat() {
  local rel="$1"
  local abs="${ROOT}/${rel}"
  local added removed stat_line

  if git -C "${ROOT}" ls-files --error-unmatch "${rel}" >/dev/null 2>&1; then
    stat_line="$(git -C "${ROOT}" diff --numstat -- "${rel}" | tail -n1 || true)"
    if [[ -n "${stat_line}" ]]; then
      added="$(echo "${stat_line}" | awk '{print $1}')"
      removed="$(echo "${stat_line}" | awk '{print $2}')"
      [[ "${added}" =~ ^[0-9]+$ ]] || added="0"
      [[ "${removed}" =~ ^[0-9]+$ ]] || removed="0"
      printf '%s|%s' "${added}" "${removed}"
      return 0
    fi
    printf '0|0'
    return 0
  fi

  if [[ -f "${abs}" ]]; then
    added="$(wc -l < "${abs}" | tr -d ' ')"
    [[ "${added}" =~ ^[0-9]+$ ]] || added="0"
    printf '%s|0' "${added}"
    return 0
  fi

  printf '0|0'
}

update_ssot_last_run() {
  local ssot_abs="$1"
  local rel_path="$2"
  local tmp
  tmp="$(mktemp "${ssot_abs}.tmp.XXXXXX")"

  if grep -q '<!-- NAVIGATOR:BEGIN SSOT_V2 -->' "${ssot_abs}" 2>/dev/null; then
    awk -v p="${rel_path}" '
      /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inblk=1; print; next}
      /<!-- NAVIGATOR:END SSOT_V2 -->/ {
        if (inblk==1 && saw_last_run==0 && inserted==0) {
          print "last_run: " p
        }
        inblk=0
        print
        next
      }
      {
        if (inblk==1) {
          if ($0 ~ /^last_run:[[:space:]]*/) {
            print "last_run: " p
            saw_last_run=1
            next
          }
          if (saw_last_run==0 && inserted==0 && $0 ~ /^claims:[[:space:]]*/) {
            print "last_run: " p
            inserted=1
          }
        }
        print
      }
    ' "${ssot_abs}" > "${tmp}"
  else
    awk -v p="${rel_path}" '
      {
        if ($0 ~ /^- last_run:[[:space:]]*/ && replaced==0) {
          print "- last_run: " p
          replaced=1
          next
        }
        print
      }
      END {
        if (replaced==0) {
          print ""
          print "## Run History"
          print "- last_run: " p
        }
      }
    ' "${ssot_abs}" > "${tmp}"
  fi

  mv "${tmp}" "${ssot_abs}"
}

emit_role_object() {
  local summary="$1"
  local output_ref="$2"
  if [[ -n "${output_ref}" ]]; then
    echo "{\"output_ref\": $(json_quote "${output_ref}")}"
  elif [[ -n "${summary}" ]]; then
    echo "{\"summary\": $(json_quote "${summary}")}"
  else
    echo "{}"
  fi
}


fail_code_min_fix() {
  local code="$1"
  if [[ -f "${FAIL_CODES_FILE}" ]]; then
    jq -r --arg c "${code}" '.codes[] | select(.code==$c) | .min_fix' "${FAIL_CODES_FILE}" 2>/dev/null | head -n1
  else
    echo ""
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ssot)
      [[ $# -ge 2 ]] || die "--ssot requires a value"
      SSOT_PATH="$2"
      shift 2
      ;;
    --mode)
      [[ $# -ge 2 ]] || die "--mode requires a value"
      MODE="$2"
      shift 2
      ;;
    --trigger)
      [[ $# -ge 2 ]] || die "--trigger requires a value"
      TRIGGER="$2"
      shift 2
      ;;
    --run-id)
      [[ $# -ge 2 ]] || die "--run-id requires a value"
      RUN_ID="$2"
      shift 2
      ;;
    --read-file)
      [[ $# -ge 2 ]] || die "--read-file requires a value"
      READ_FILES+=("$2")
      shift 2
      ;;
    --written-file)
      [[ $# -ge 2 ]] || die "--written-file requires a value"
      WRITTEN_SPECS+=("$2")
      shift 2
      ;;
    --navigator-summary)
      [[ $# -ge 2 ]] || die "--navigator-summary requires a value"
      NAVIGATOR_SUMMARY="$2"
      shift 2
      ;;
    --navigator-output-ref)
      [[ $# -ge 2 ]] || die "--navigator-output-ref requires a value"
      NAVIGATOR_OUTPUT_REF="$2"
      shift 2
      ;;
    --patch-summary)
      [[ $# -ge 2 ]] || die "--patch-summary requires a value"
      PATCH_SUMMARY="$2"
      shift 2
      ;;
    --patch-output-ref)
      [[ $# -ge 2 ]] || die "--patch-output-ref requires a value"
      PATCH_OUTPUT_REF="$2"
      shift 2
      ;;
    --implement-summary)
      [[ $# -ge 2 ]] || die "--implement-summary requires a value"
      IMPLEMENT_SUMMARY="$2"
      shift 2
      ;;
    --implement-output-ref)
      [[ $# -ge 2 ]] || die "--implement-output-ref requires a value"
      IMPLEMENT_OUTPUT_REF="$2"
      shift 2
      ;;
    --verifier-status)
      [[ $# -ge 2 ]] || die "--verifier-status requires a value"
      VERIFIER_STATUS="$2"
      shift 2
      ;;
    --verifier-fail-code)
      [[ $# -ge 2 ]] || die "--verifier-fail-code requires a value"
      FAIL_CODES+=("$2")
      shift 2
      ;;
    --latency-navigator)
      [[ $# -ge 2 ]] || die "--latency-navigator requires a value"
      LAT_NAVIGATOR="$2"
      shift 2
      ;;
    --latency-patch)
      [[ $# -ge 2 ]] || die "--latency-patch requires a value"
      LAT_PATCH="$2"
      shift 2
      ;;
    --latency-verifier)
      [[ $# -ge 2 ]] || die "--latency-verifier requires a value"
      LAT_VERIFIER="$2"
      shift 2
      ;;
    --latency-implement)
      [[ $# -ge 2 ]] || die "--latency-implement requires a value"
      LAT_IMPLEMENT="$2"
      shift 2
      ;;
    --next-action)
      [[ $# -ge 2 ]] || die "--next-action requires a value"
      NEXT_ACTIONS+=("$2")
      shift 2
      ;;
    --diff-summary)
      [[ $# -ge 2 ]] || die "--diff-summary requires a value"
      DIFF_SUMMARY="$2"
      shift 2
      ;;
    --notes)
      [[ $# -ge 2 ]] || die "--notes requires a value"
      NOTES="$2"
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

[[ -n "${SSOT_PATH}" ]] || die "--ssot is required"
[[ "${MODE}" == "navigate" || "${MODE}" == "navigate_patch" ]] || die "--mode must be navigate or navigate_patch"
[[ "${VERIFIER_STATUS}" == "pass" || "${VERIFIER_STATUS}" == "fail" ]] || die "--verifier-status must be pass or fail"
[[ "${LAT_NAVIGATOR}" =~ ^[0-9]+$ ]] || die "--latency-navigator must be integer"
[[ "${LAT_PATCH}" =~ ^[0-9]+$ ]] || die "--latency-patch must be integer"
[[ "${LAT_VERIFIER}" =~ ^[0-9]+$ ]] || die "--latency-verifier must be integer"
if [[ -n "${LAT_IMPLEMENT}" ]]; then
  [[ "${LAT_IMPLEMENT}" =~ ^[0-9]+$ ]] || die "--latency-implement must be integer"
fi
[[ ${#NEXT_ACTIONS[@]} -le 3 ]] || die "next_actions must be <= 3"

if [[ "${SSOT_PATH}" == /* ]]; then
  SSOT_ABS="${SSOT_PATH}"
else
  SSOT_ABS="${ROOT}/${SSOT_PATH}"
fi
[[ -f "${SSOT_ABS}" ]] || die "ssot not found: ${SSOT_ABS}"
SSOT_REL="$(to_rel_path "${SSOT_ABS}")"

SSOT_HASH="$(sha256_of_file "${SSOT_ABS}")"
TIMESTAMP="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
DATE_PART="$(date -u +"%Y-%m-%d")"
if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="$(date -u +"%H%M%SZ")-$(git -C "${ROOT}" rev-parse --short HEAD 2>/dev/null || echo unknown)"
fi

mkdir -p "${RUNS_DIR}"
RUN_JSON_REL="${RUNS_DIR_REL}/${DATE_PART}_run-${RUN_ID}.json"
RUN_MD_REL="${RUNS_DIR_REL}/${DATE_PART}_run-${RUN_ID}.md"
RUN_JSON_ABS="${ROOT}/${RUN_JSON_REL}"
RUN_MD_ABS="${ROOT}/${RUN_MD_REL}"

[[ ! -e "${RUN_JSON_ABS}" ]] || die "run record exists: ${RUN_JSON_REL}"
[[ ! -e "${RUN_MD_ABS}" ]] || die "run record exists: ${RUN_MD_REL}"

if [[ -z "${DIFF_SUMMARY}" ]]; then
  DIFF_SUMMARY="$(trim_200 "mode=${MODE}; updated=${SSOT_REL}; created=${RUN_JSON_REL},${RUN_MD_REL}")"
else
  DIFF_SUMMARY="$(trim_200 "${DIFF_SUMMARY}")"
fi

if [[ -z "${NAVIGATOR_SUMMARY}" && -z "${NAVIGATOR_OUTPUT_REF}" ]]; then
  NAVIGATOR_SUMMARY="generated by ${MODE}"
fi
if [[ "${MODE}" == "navigate" && -z "${PATCH_SUMMARY}" && -z "${PATCH_OUTPUT_REF}" ]]; then
  PATCH_SUMMARY="not_applicable"
fi
if [[ "${MODE}" == "navigate_patch" && -z "${PATCH_SUMMARY}" && -z "${PATCH_OUTPUT_REF}" ]]; then
  PATCH_SUMMARY="ssot and governance patch completed"
fi

update_ssot_last_run "${SSOT_ABS}" "${RUN_JSON_REL}"

READ_RELS=()
if [[ ${#READ_FILES[@]} -eq 0 ]]; then
  READ_FILES+=("${SSOT_REL}")
fi
for p in "${READ_FILES[@]:-}"; do
  [[ -n "${p}" ]] || continue
  if [[ "${p}" == /* ]]; then
    READ_RELS+=("$(to_rel_path "${p}")")
  else
    READ_RELS+=("${p}")
  fi
done

WRITTEN_PATHS=("${SSOT_REL}" "${RUN_JSON_REL}" "${RUN_MD_REL}")
for spec in "${WRITTEN_SPECS[@]:-}"; do
  [[ -n "${spec}" ]] || continue
  path_part="${spec%%:*}"
  if [[ "${path_part}" == /* ]]; then
    path_part="$(to_rel_path "${path_part}")"
  fi
  if ! path_exists_in_array "${path_part}" "${WRITTEN_PATHS[@]}"; then
    WRITTEN_PATHS+=("${path_part}")
  fi
done

{
  echo "# Run Record"
  echo
  echo "## Summary"
  echo "- run_id: ${RUN_ID}"
  echo "- timestamp: ${TIMESTAMP}"
  echo "- status: ${VERIFIER_STATUS}"
  echo "- mode: ${MODE}"
  echo
  echo "## Inputs"
  echo "- ssot_path: ${SSOT_REL}"
  echo "- ssot_hash: ${SSOT_HASH}"
  echo "- read_files:"
  for rel in "${READ_RELS[@]}"; do
    echo "  - ${rel}"
  done
  echo
  echo "## Outputs"
  echo "- written_files:"
  for rel in "${WRITTEN_PATHS[@]}"; do
    echo "  - ${rel}"
  done
  echo "- diff_summary: ${DIFF_SUMMARY}"
  echo
  echo "## Verifier"
  if [[ ${#FAIL_CODES[@]} -gt 0 ]]; then
    echo "- fail_codes:"
    for code in "${FAIL_CODES[@]}"; do
      mf="$(fail_code_min_fix "${code}")"
      if [[ -n "${mf}" ]]; then
        echo "  - ${code}: ${mf}"
      else
        echo "  - ${code}"
      fi
    done
  else
    echo "- fail_codes: none"
  fi
  if [[ ${#NEXT_ACTIONS[@]} -gt 0 ]]; then
    echo "- min_fix: ${NEXT_ACTIONS[0]}"
  else
    echo "- min_fix: none"
  fi
  echo
  echo "## Next actions"
  if [[ ${#NEXT_ACTIONS[@]} -gt 0 ]]; then
    for ((i=0; i<${#NEXT_ACTIONS[@]}; i++)); do
      idx=$((i+1))
      echo "${idx}. ${NEXT_ACTIONS[$i]}"
    done
  else
    echo "- none"
  fi
  echo
  echo "## Notes"
  if [[ -n "${NOTES}" ]]; then
    echo "${NOTES}"
  else
    echo "-"
  fi
} > "${RUN_MD_ABS}"

md_line_count="$(wc -l < "${RUN_MD_ABS}" | tr -d ' ')"
[[ "${md_line_count}" =~ ^[0-9]+$ ]] || md_line_count="0"

{
  echo "{"
  echo "  \"run_id\": $(json_quote "${RUN_ID}"),"
  echo "  \"timestamp\": $(json_quote "${TIMESTAMP}"),"
  echo "  \"trigger\": $(json_quote "${TRIGGER}"),"
  echo "  \"inputs\": {"
  echo "    \"ssot_path\": $(json_quote "${SSOT_REL}"),"
  echo "    \"ssot_hash\": $(json_quote "${SSOT_HASH}"),"
  echo "    \"read_files\": ["
  for ((i=0; i<${#READ_RELS[@]}; i++)); do
    sep=","; [[ $i -eq $((${#READ_RELS[@]}-1)) ]] && sep=""
    echo "      $(json_quote "${READ_RELS[$i]}")${sep}"
  done
  echo "    ]"
  echo "  },"
  echo "  \"outputs\": {"
  echo "    \"written_files\": ["
  for ((i=0; i<${#WRITTEN_PATHS[@]}; i++)); do
    rel="${WRITTEN_PATHS[$i]}"
    if [[ "${rel}" == "${RUN_JSON_REL}" ]]; then
      add="0"
      del="0"
    elif [[ "${rel}" == "${RUN_MD_REL}" ]]; then
      add="${md_line_count}"
      del="0"
    else
      stats="$(calc_numstat "${rel}")"
      add="${stats%%|*}"
      del="${stats##*|}"
    fi
    sep=","; [[ $i -eq $((${#WRITTEN_PATHS[@]}-1)) ]] && sep=""
    echo "      {\"path\": $(json_quote "${rel}"), \"added_lines\": ${add}, \"removed_lines\": ${del}}${sep}"
  done
  echo "    ],"
  echo "    \"diff_summary\": $(json_quote "${DIFF_SUMMARY}")"
  echo "  },"
  echo "  \"roles\": {"
  echo "    \"navigator\": $(emit_role_object "${NAVIGATOR_SUMMARY}" "${NAVIGATOR_OUTPUT_REF}"),"
  echo "    \"patch\": $(emit_role_object "${PATCH_SUMMARY}" "${PATCH_OUTPUT_REF}"),"
  echo "    \"implement\": $(emit_role_object "${IMPLEMENT_SUMMARY}" "${IMPLEMENT_OUTPUT_REF}")"
  echo "  },"
  echo "  \"verifier\": {"
  echo "    \"status\": $(json_quote "${VERIFIER_STATUS}"),"
  echo "    \"fail_codes\": ["
  for ((i=0; i<${#FAIL_CODES[@]}; i++)); do
    sep=","; [[ $i -eq $((${#FAIL_CODES[@]}-1)) ]] && sep=""
    echo "      $(json_quote "${FAIL_CODES[$i]}")${sep}"
  done
  echo "    ]"
  echo "  },"
  echo "  \"latency_ms\": {"
  echo "    \"navigator\": ${LAT_NAVIGATOR},"
  echo "    \"patch\": ${LAT_PATCH},"
  echo "    \"verifier\": ${LAT_VERIFIER},"
  if [[ -n "${LAT_IMPLEMENT}" ]]; then
    echo "    \"implement\": ${LAT_IMPLEMENT}"
  else
    echo "    \"implement\": null"
  fi
  echo "  },"
  echo "  \"next_actions\": ["
  for ((i=0; i<${#NEXT_ACTIONS[@]}; i++)); do
    sep=","; [[ $i -eq $((${#NEXT_ACTIONS[@]}-1)) ]] && sep=""
    echo "    $(json_quote "${NEXT_ACTIONS[$i]}")${sep}"
  done
  echo "  ],"
  echo "  \"notes\": $(json_quote "${NOTES}")"
  echo "}"
} > "${RUN_JSON_ABS}"

json_line_count="$(wc -l < "${RUN_JSON_ABS}" | tr -d ' ')"
[[ "${json_line_count}" =~ ^[0-9]+$ ]] || json_line_count="0"

jq --arg run_json_path "${RUN_JSON_REL}" --argjson run_json_lines "${json_line_count}" '
  .outputs.written_files |= map(
    if .path == $run_json_path then
      .added_lines = $run_json_lines | .removed_lines = 0
    else
      .
    end
  )
' "${RUN_JSON_ABS}" > "${RUN_JSON_ABS}.tmp"
mv "${RUN_JSON_ABS}.tmp" "${RUN_JSON_ABS}"

jq -e . "${RUN_JSON_ABS}" >/dev/null

echo "RUN_RECORD_JSON=${RUN_JSON_REL}"
echo "RUN_RECORD_MD=${RUN_MD_REL}"
echo "SSOT_UPDATED=${SSOT_REL}"
