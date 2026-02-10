#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'EOF'
STRICT_REVIEW_FIX_ONECLICK

Usage:
  bash ai/projects/topru-ai/verify/strict_review_fix_oneclick.sh [--base <hash>] [--head <hash>] \
    [--mode first|incr|targeted] [--target R1,R2,...] [--no-patch-ssot]

Defaults:
  --head: git rev-parse HEAD
  --base: best-effort merge-base vs origin/master|origin/main|master|main
  --mode:
    - if PROJECT_STATE SSOT has review.baseline_commit -> incr
    - else -> first

This script:
  - collects review evidence (changed_files, diff, commits)
  - generates review.md + findings.yaml + next_actions.md (best-effort)
  - writes artifacts under ai/projects/topru-ai/artifacts/review/<RUN_ID>/
  - patches SSOT_V2 review section in ai/projects/topru-ai/PROJECT_STATE.md (unless --no-patch-ssot)

Safety:
  - no GitHub comments, no auto-merge, no auto-fix code.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"     # .../ai/projects/topru-ai
REPO_DIR="$(cd "${PROJECT_DIR}/../../.." && pwd)" # repo root
SSOT_FILE="${PROJECT_DIR}/PROJECT_STATE.md"
LIB_ONECLICK="${PROJECT_DIR}/verify/lib_oneclick.sh"

BASE=""
HEAD=""
MODE=""
TARGET_IDS=""
PATCH_SSOT="1"
SSOT_PATCH_RESULT="skipped"
FINAL_RC=0
BASE_PROVIDED="0"
HEAD_PROVIDED="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base) BASE="$2"; BASE_PROVIDED="1"; shift 2;;
    --head) HEAD="$2"; HEAD_PROVIDED="1"; shift 2;;
    --mode) MODE="$2"; shift 2;;
    --target) TARGET_IDS="$2"; shift 2;;
    --no-patch-ssot) PATCH_SSOT="0"; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

if [[ -z "${HEAD}" ]]; then
  HEAD="$(git -C "${REPO_DIR}" rev-parse HEAD)"
fi

cd "${REPO_DIR}"

TIME_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
HEAD_SHORT="$(echo "${HEAD}" | cut -c1-7)"
if [[ -z "${MODE}" ]]; then
  if [[ -f "${SSOT_FILE}" ]] && grep -Eq "^review:\\s*$" "${SSOT_FILE}" && grep -q "baseline_commit:" "${SSOT_FILE}"; then
    MODE="incr"
  else
    MODE="first"
  fi
fi

RUN_ID="${TIME_UTC}_${HEAD_SHORT}_${MODE}"
OUT_DIR="${PROJECT_DIR}/artifacts/review/${RUN_ID}"
mkdir -p "${OUT_DIR}"

RUN_LOG="${OUT_DIR}/run.log"
CHANGED_FILES="${OUT_DIR}/changed_files.txt"
DIFF_PATCH="${OUT_DIR}/diff.patch"
PATCH_DIFF="${OUT_DIR}/patch.diff"
COMMITS_TXT="${OUT_DIR}/commits.txt"
SUMMARY_TXT="${OUT_DIR}/summary.txt"
REVIEW_MD="${OUT_DIR}/review.md"
FINDINGS_YAML="${OUT_DIR}/findings.yaml"
NEXT_ACTIONS_MD="${OUT_DIR}/next_actions.md"
FIX_QUEUE_MD="${OUT_DIR}/fix_queue.md"
TRACE_JSONL="${OUT_DIR}/trace.jsonl"
MANIFEST_JSON="${OUT_DIR}/manifest.json"
FIX_QUEUE_JSON="${OUT_DIR}/fix_queue.json"
RESULT_JSON="${OUT_DIR}/result.json"

# Create required files early (even if we fail later).
: >"${RUN_LOG}"
: >"${CHANGED_FILES}"
: >"${DIFF_PATCH}"
: >"${PATCH_DIFF}"
: >"${COMMITS_TXT}"
: >"${SUMMARY_TXT}"
: >"${REVIEW_MD}"
: >"${FINDINGS_YAML}"
: >"${NEXT_ACTIONS_MD}"
: >"${FIX_QUEUE_MD}"
: >"${TRACE_JSONL}"
: >"${MANIFEST_JSON}"
: >"${FIX_QUEUE_JSON}"
: >"${RESULT_JSON}"

# Reuse oneclick stage/trace convention for debuggability.
if [[ -f "${LIB_ONECLICK}" ]]; then
  ONECLICK_DISABLE_ERR_TRAP=1
  # shellcheck source=/dev/null
  source "${LIB_ONECLICK}"
  ONECLICK_DISABLE_ERR_TRAP=0
  ART_DIR="${OUT_DIR}"
  TRACE="${TRACE_JSONL}"
  init_trace "${OUT_DIR}"
else
  echo "WARN: lib_oneclick.sh not found: ${LIB_ONECLICK} (trace disabled)" >&2
fi

# Fallback stage/trace if lib was not loaded (keep script functional).
if ! command -v stage >/dev/null 2>&1; then
  now_iso() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
  trace() {
    local st="$1"; local msg="$2"; local status="$3"; local details="${4:-{}}"
    local t; t="$(now_iso)"
    msg="${msg//\"/\' }"
    echo "{\"time\":\"${t}\",\"stage\":\"${st}\",\"status\":\"${status}\",\"msg\":\"${msg}\",\"details\":${details}}" >> "${TRACE_JSONL}" 2>/dev/null || true
  }
  stage() {
    echo "[strict_review][stage $1] $2"
    trace "$1" "$2" "ok" "{}"
  }
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

json_quote() {
  printf '"%s"' "$(json_escape "$1")"
}

print_summary() {
  {
    echo
    echo "=== strict-review summary ==="
    echo "artifacts_dir: ${OUT_DIR}"
    echo "  - review_md: ${REVIEW_MD}"
    echo "  - findings_yaml: ${FINDINGS_YAML}"
    echo "  - next_actions: ${NEXT_ACTIONS_MD}"
    echo "  - fix_queue: ${FIX_QUEUE_MD}"
    echo "  - changed_files: ${CHANGED_FILES}"
    echo "  - diff_patch: ${DIFF_PATCH}"
    echo "  - commits: ${COMMITS_TXT}"
    echo "  - summary: ${SUMMARY_TXT}"
    echo "  - trace: ${TRACE_JSONL}"
    echo "range: ${BASE:-<unset>}..${HEAD}"
    echo "ssot_patch: ${SSOT_PATCH_RESULT}"
    echo "exit_code: ${FINAL_RC}"
  } | tee -a "${RUN_LOG}"
}

best_effort_base() {
  local head="$1"
  local candidates=("origin/master" "origin/main" "master" "main")
  for c in "${candidates[@]}"; do
    if git -C "${REPO_DIR}" rev-parse --verify "${c}" >/dev/null 2>&1; then
      git -C "${REPO_DIR}" merge-base "${c}" "${head}" && return 0
    fi
  done
  echo ""
  return 0
}

if [[ ! -f "${SSOT_FILE}" ]]; then
  stage 0 "preflight failed: SSOT missing"
  echo "ERROR: SSOT not found: ${SSOT_FILE}" >&2
  FINAL_RC=2
  print_summary
  exit "${FINAL_RC}"
fi

stage 0 "preflight ok"

ssot_review_value() {
  # ssot_review_value <key>
  # reads key under review: block inside SSOT_V2 (2-space indent), best-effort.
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

ssot_review_list_csv() {
  # ssot_review_list_csv <key>
  # parses `  open: [R1, R2]` to `R1,R2`
  local key="$1"
  local line
  line="$(awk -v k="${key}" '
    /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inssot=1; next}
    /<!-- NAVIGATOR:END SSOT_V2 -->/ {inssot=0}
    inssot && /^review:\s*$/ {inrev=1; next}
    inssot && inrev && /^[A-Za-z0-9_]+:\s*$/ {inrev=0}
    inssot && inrev && $0 ~ "^  "k":" {print; exit}
  ' "${SSOT_FILE}" 2>/dev/null || true)"
  # Extract bracket content (including empty list) and remove whitespace.
  echo "${line}" | awk '
    match($0, /\\[[^]]*\\]/) {
      s=substr($0, RSTART+1, RLENGTH-2)
      gsub(/[[:space:]]/, "", s)
      print s
    }
  ' 2>/dev/null || true
}

SSOT_REVIEW_BASELINE_COMMIT="$(ssot_review_value baseline_commit)"
SSOT_REVIEW_OPEN_CSV="$(ssot_review_list_csv open)"
SSOT_REVIEW_FIXED_CSV="$(ssot_review_list_csv fixed)"
SSOT_REVIEW_PARTIAL_CSV="$(ssot_review_list_csv partially_fixed)"

ssot_review_last_run_value() {
  # ssot_review_last_run_value <key>
  # reads key under review.last_run: block inside SSOT_V2 (4-space indent), best-effort.
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

SSOT_REVIEW_LAST_ART_DIR="$(ssot_review_last_run_value artifacts_dir)"
SSOT_REVIEW_LAST_FINDINGS_YAML="$(ssot_review_last_run_value findings_yaml)"

# If incremental and --base not provided, use SSOT baseline_commit as base.
if [[ "${MODE}" == "incr" && "${BASE_PROVIDED}" != "1" && -n "${SSOT_REVIEW_BASELINE_COMMIT}" ]]; then
  BASE="${SSOT_REVIEW_BASELINE_COMMIT}"
fi

if [[ -z "${BASE}" ]]; then
  BASE="$(best_effort_base "${HEAD}")"
fi
if [[ -z "${BASE}" ]]; then
  stage 0 "preflight failed: could not determine base"
  echo "ERROR: could not determine --base (please provide --base <hash>)" >&2
  FINAL_RC=2
  print_summary
  exit "${FINAL_RC}"
fi

echo "STRICT_REVIEW_FIX_ONECLICK"
echo "time_utc: ${TIME_UTC}"
echo "mode: ${MODE}"
echo "base: ${BASE}"
echo "head: ${HEAD}"
echo "range: ${BASE}..${HEAD}"
echo "out_dir: ${OUT_DIR}"
echo "target: ${TARGET_IDS:-<none>}"
echo "trace: ${TRACE_JSONL}"
echo

stage 1 "collect evidence: changed_files/diff/commits"
git -C "${REPO_DIR}" diff --name-only "${BASE}..${HEAD}" >"${CHANGED_FILES}" || true
git -C "${REPO_DIR}" diff "${BASE}..${HEAD}" >"${DIFF_PATCH}" || true
cp "${DIFF_PATCH}" "${PATCH_DIFF}" 2>/dev/null || true
git -C "${REPO_DIR}" log --oneline --decorate "${BASE}..${HEAD}" >"${COMMITS_TXT}" || true
trace 1 "evidence collected" "ok" "{\"changed_files\":\"${CHANGED_FILES}\",\"diff_patch\":\"${DIFF_PATCH}\"}"

CHANGED_COUNT="$(wc -l <"${CHANGED_FILES}" | tr -d ' ')"
COMMIT_COUNT="$(wc -l <"${COMMITS_TXT}" | tr -d ' ' || echo 0)"

{
  echo "STRICT_REVIEW_SUMMARY"
  echo "base: ${BASE}"
  echo "head: ${HEAD}"
  echo "range: ${BASE}..${HEAD}"
  echo "mode: ${MODE}"
  echo "changed_files_count: ${CHANGED_COUNT}"
  echo "commits_count: ${COMMIT_COUNT}"
  echo
  echo "changed_files:"
  sed -n '1,200p' "${CHANGED_FILES}"
  echo
  echo "commits:"
  sed -n '1,200p' "${COMMITS_TXT}" || true
} >"${SUMMARY_TXT}"

# -------------------------
# Step 2: findings + review
# -------------------------

stage 2 "generate findings.yaml + review.md + next_actions.md"

sha1_of() {
  # sha1_of <string>
  if command -v shasum >/dev/null 2>&1; then
    printf "%s" "$1" | shasum -a 1 | awk '{print $1}'
    return 0
  fi
  if command -v sha1sum >/dev/null 2>&1; then
    printf "%s" "$1" | sha1sum | awk '{print $1}'
    return 0
  fi
  echo "UNKNOWN"
}

abs_path() {
  # abs_path <path>
  # Treat repo-relative paths as relative to $REPO_DIR.
  local p="$1"
  if [[ -z "${p}" ]]; then
    echo ""
    return 0
  fi
  if [[ "${p}" == /* ]]; then
    echo "${p}"
    return 0
  fi
  echo "${REPO_DIR}/${p}"
}

# Best-effort auto finding: gofmt drift on changed .go files (must-fix).
CHANGED_GO_FILES="$(grep -E "\\.go$" "${CHANGED_FILES}" || true)"
GOFMT_BAD=""
if [[ -n "${CHANGED_GO_FILES}" ]] && command -v gofmt >/dev/null 2>&1; then
  # shellcheck disable=SC2086
  GOFMT_BAD="$(cd "${REPO_DIR}" && echo "${CHANGED_GO_FILES}" | xargs -n 50 gofmt -l 2>/dev/null || true)"
fi

csv_to_ids() {
  # Normalize items to `R<number>` tokens (best-effort) to keep incremental workflow stable.
  echo "$1" | tr ',' '\n' | sed 's/[[:space:]]//g' | awk '
    match($0, /R[0-9]+/) { print substr($0, RSTART, RLENGTH) }
  ' | sed '/^$/d'
}

max_r_num_from_ids() {
  local max=0
  local id
  for id in "$@"; do
    if [[ "${id}" =~ ^R([0-9]+)$ ]]; then
      n="${BASH_REMATCH[1]}"
      if (( n > max )); then max="${n}"; fi
    fi
  done
  echo "${max}"
}

prev_finding_field() {
  # prev_finding_field <file> <id> <key>
  local f="$1"
  local id="$2"
  local key="$3"
  [[ -f "${f}" ]] || return 0
  awk -v id="${id}" -v k="${key}" '
    $0 ~ "^  - id: "id"$" {in=1; next}
    in==1 && $0 ~ "^  - id: " && $0 !~ "^  - id: "id"$" {exit}
    in==1 && $0 ~ "^    "k":" {
      sub("^    "k":[[:space:]]*", "", $0)
      gsub(/"/, "", $0)
      print $0
      exit
    }
  ' "${f}" 2>/dev/null || true
}

prev_finding_list() {
  # prev_finding_list <file> <id> <key>  (prints each list item on its own line)
  local f="$1"
  local id="$2"
  local key="$3"
  [[ -f "${f}" ]] || return 0
  awk -v id="${id}" -v k="${key}" '
    $0 ~ "^  - id: "id"$" {in=1; next}
    in==1 && $0 ~ "^  - id: " && $0 !~ "^  - id: "id"$" {exit}
    in==1 && $0 ~ "^    "k":\\s*$" {inlist=1; next}
    in==1 && inlist==1 && $0 ~ "^    [A-Za-z0-9_]+:" {exit}
    in==1 && inlist==1 && $0 ~ "^      - " {
      sub("^      -[[:space:]]*", "", $0)
      gsub(/"/, "", $0)
      print $0
    }
  ' "${f}" 2>/dev/null || true
}

first_changed="$(sed -n '1p' "${CHANGED_FILES}" | tr -d '\r' || true)"
changed_loc="${first_changed:-<none>}"
if [[ "${CHANGED_COUNT}" != "0" && "${CHANGED_COUNT}" != "1" && -n "${first_changed}" ]]; then
  changed_loc="${first_changed} (+$((CHANGED_COUNT-1)) more)"
fi

# Incremental enforcement: default only process SSOT.review.open.
PROCESS_IDS_CSV=""
case "${MODE}" in
  first) PROCESS_IDS_CSV="" ;;
  incr) PROCESS_IDS_CSV="${SSOT_REVIEW_OPEN_CSV}" ;;
  targeted)
    if [[ -z "${TARGET_IDS}" ]]; then
      echo "ERROR: --mode targeted requires --target R1,R2,..." >&2
      FINAL_RC=2
      print_summary
      exit "${FINAL_RC}"
    fi
    PROCESS_IDS_CSV="${TARGET_IDS}"
    ;;
  *) echo "ERROR: unknown mode: ${MODE}" >&2; exit 2 ;;
esac

# Determine whether new areas are touched since last review (file-level heuristic).
TOUCHED_NEW_FILES=""
TOUCHED_NEW_FILES_COUNT="0"
ALLOW_NEW_FINDINGS="1"
if [[ "${MODE}" == "incr" || "${MODE}" == "targeted" ]]; then
  ALLOW_NEW_FINDINGS="0"
  prev_art_dir_abs="$(abs_path "${SSOT_REVIEW_LAST_ART_DIR}")"
  prev_changed="${prev_art_dir_abs}/changed_files.txt"
  if [[ -f "${prev_changed}" ]]; then
    tmp_prev="$(mktemp)"; tmp_curr="$(mktemp)"; tmp_new="$(mktemp)"
    sort -u "${prev_changed}" >"${tmp_prev}" || true
    sort -u "${CHANGED_FILES}" >"${tmp_curr}" || true
    comm -13 "${tmp_prev}" "${tmp_curr}" >"${tmp_new}" || true
    TOUCHED_NEW_FILES_COUNT="$(wc -l <"${tmp_new}" | tr -d ' ')"
    TOUCHED_NEW_FILES="$(sed -n '1,20p' "${tmp_new}" | paste -sd ',' -)"
    rm -f "${tmp_prev}" "${tmp_curr}" "${tmp_new}"
    if [[ "${TOUCHED_NEW_FILES_COUNT}" != "0" ]]; then
      ALLOW_NEW_FINDINGS="1"
    fi
  else
    # Enforce: without baseline evidence, do not add new findings automatically.
    TOUCHED_NEW_FILES_COUNT="0"
    TOUCHED_NEW_FILES="(missing baseline changed_files.txt under last_run.artifacts_dir)"
  fi
fi

prev_findings_abs="$(abs_path "${SSOT_REVIEW_LAST_FINDINGS_YAML}")"

# Build findings.yaml. In incremental/targeted mode, we only carry forward the open IDs by default.
{
  echo "baseline: \"${BASE}\""
  echo "head: \"${HEAD}\""
  echo "range: \"${BASE}..${HEAD}\""
  echo "mode: \"${MODE}\""
  echo "findings:"

  included_ids=()
  if [[ -n "${PROCESS_IDS_CSV}" ]]; then
    while IFS= read -r rid; do
      [[ -z "${rid}" ]] && continue

      # Load previous fields (best-effort). Fill defaults if missing.
      prev_status="$(prev_finding_field "${prev_findings_abs}" "${rid}" "status")"
      prev_type="$(prev_finding_field "${prev_findings_abs}" "${rid}" "type")"
      prev_must_fix="$(prev_finding_field "${prev_findings_abs}" "${rid}" "must_fix")"
      prev_impact="$(prev_finding_field "${prev_findings_abs}" "${rid}" "impact")"
      prev_scope="$(prev_finding_field "${prev_findings_abs}" "${rid}" "scope")"
      prev_likelihood="$(prev_finding_field "${prev_findings_abs}" "${rid}" "likelihood")"
      prev_location="$(prev_finding_field "${prev_findings_abs}" "${rid}" "location")"
      prev_title="$(prev_finding_field "${prev_findings_abs}" "${rid}" "title")"
      prev_advice="$(prev_finding_field "${prev_findings_abs}" "${rid}" "advice")"
      prev_fp="$(prev_finding_field "${prev_findings_abs}" "${rid}" "fingerprint")"

      st="${prev_status:-open}"
      typ="${prev_type:-Risk}"
      must_fix="${prev_must_fix:-true}"
      impact="${prev_impact:-Compatibility}"
      scope="${prev_scope:-local}"
      likelihood="${prev_likelihood:-Med}"
      location="${prev_location:-${changed_loc}}"
      title="${prev_title:-TODO}"
      advice="${prev_advice:-TODO}"
      fp="${prev_fp}"
      if [[ -z "${fp}" || "${fp}" == "UNKNOWN" ]]; then
        fp="$(sha1_of "${typ}|${location}|${title}")"
      fi

      # Normalize status to allowed set: open|fixed|partially_fixed.
      case "${st}" in
        open|fixed|partially_fixed) ;;
        closed) st="fixed" ;;
        partial) st="partially_fixed" ;;
        *) st="open" ;;
      esac

      # Targeted status verification (best-effort):
      # - If it looks like a gofmt drift finding, we can auto-verify it.
      if [[ "${typ}" == "Style" ]] && echo "${title}" | grep -q "gofmt" 2>/dev/null; then
        if [[ -z "${GOFMT_BAD}" ]]; then
          st="fixed"
        else
          st="open"
          location="${GOFMT_BAD}"
        fi
      fi

      echo "  - id: ${rid}"
      echo "    fingerprint: \"${fp}\""
      echo "    status: ${st}"
      echo "    type: \"${typ}\""
      echo "    must_fix: ${must_fix}"
      echo "    impact: \"${impact}\""
      echo "    scope: \"${scope}\""
      echo "    likelihood: \"${likelihood}\""
      echo "    location: \"${location}\""
      echo "    title: \"${title}\""
      echo "    advice: \"${advice}\""
      vmin_lines="$(prev_finding_list "${prev_findings_abs}" "${rid}" "verify_min" || true)"
      if [[ -n "${vmin_lines}" ]]; then
        echo "    verify_min:"
        echo "${vmin_lines}" | sed 's/^/      - "/; s/$/"/'
      else
        echo "    verify_min: []"
      fi

      vopt_lines="$(prev_finding_list "${prev_findings_abs}" "${rid}" "verify_opt" || true)"
      if [[ -n "${vopt_lines}" ]]; then
        echo "    verify_opt:"
        echo "${vopt_lines}" | sed 's/^/      - "/; s/$/"/'
      else
        echo "    verify_opt: []"
      fi

      cln_lines="$(prev_finding_list "${prev_findings_abs}" "${rid}" "cleanup" || true)"
      if [[ -n "${cln_lines}" ]]; then
        echo "    cleanup:"
        echo "${cln_lines}" | sed 's/^/      - "/; s/$/"/'
      else
        echo "    cleanup: []"
      fi
      included_ids+=("${rid}")
    done < <(csv_to_ids "${PROCESS_IDS_CSV}")
  fi

  # New findings (allowed in first, or in incr only if touched new files).
  # Heuristic: gofmt drift on changed Go files.
  has_gofmt="0"
  for x in "${included_ids[@]-}"; do
    t="$(prev_finding_field "${prev_findings_abs}" "${x}" "type")"
    ttl="$(prev_finding_field "${prev_findings_abs}" "${x}" "title")"
    if [[ "${t}" == "Style" ]] && echo "${ttl}" | grep -q "gofmt" 2>/dev/null; then
      has_gofmt="1"
      break
    fi
  done

  if [[ "${MODE}" == "first" ]]; then
    ALLOW_NEW_FINDINGS="1"
  fi
  if [[ "${ALLOW_NEW_FINDINGS}" == "1" && -n "${GOFMT_BAD}" && "${has_gofmt}" != "1" ]]; then
    maxn="$(max_r_num_from_ids "${included_ids[@]-}")"
    nid="R$((maxn+1))"
    fp="$(sha1_of "Style|${GOFMT_BAD}|Changed Go files are not gofmt'ed")"
    echo "  - id: ${nid}"
    echo "    fingerprint: \"${fp}\""
    echo "    status: open"
    echo "    type: \"Style\""
    echo "    must_fix: true"
    echo "    impact: \"Noise\""
    echo "    scope: \"local\""
    echo "    likelihood: \"High\""
    echo "    location: \"${GOFMT_BAD}\""
    echo "    title: \"Changed Go files are not gofmt'ed\""
    echo "    advice: \"Run gofmt on the listed files (do not change semantics).\""
    echo "    verify_min:"
    echo "      - \"gofmt -l <files> (expect empty)\""
    echo "    verify_opt: []"
    echo "    cleanup: []"
    included_ids+=("${nid}")
  fi
} >"${FINDINGS_YAML}"

all_ids_csv="$(awk '/^  - id: /{print $3}' "${FINDINGS_YAML}" | paste -sd ',' - 2>/dev/null || true)"
new_ids_csv=""
if [[ -n "${all_ids_csv}" ]]; then
  while IFS= read -r id; do
    [[ -z "${id}" ]] && continue
    if [[ -z "${PROCESS_IDS_CSV}" ]]; then
      # first review: all findings are "new"
      if [[ -z "${new_ids_csv}" ]]; then new_ids_csv="${id}"; else new_ids_csv="${new_ids_csv},${id}"; fi
      continue
    fi
    if [[ ",${PROCESS_IDS_CSV}," != *",${id},"* ]]; then
      if [[ -z "${new_ids_csv}" ]]; then new_ids_csv="${id}"; else new_ids_csv="${new_ids_csv},${id}"; fi
    fi
  done < <(echo "${all_ids_csv}" | tr ',' '\n')
fi

new_reason="(none)"
if [[ "${MODE}" == "incr" && -n "${new_ids_csv}" ]]; then
  if [[ "${TOUCHED_NEW_FILES_COUNT}" != "0" ]]; then
    new_reason="touched new files since baseline: [${TOUCHED_NEW_FILES}]"
  else
    new_reason="unexpected: new findings added but touched_new_files_count=0 (check enforcement logic)"
  fi
fi

{
  echo "# STRICT_REVIEW — TopRU (auto-generated)"
  echo
  echo "## Snapshot"
  echo "- mode: ${MODE}"
  echo "- base: ${BASE}"
  echo "- head: ${HEAD}"
  echo "- range: ${BASE}..${HEAD}"
  echo "- changed_files_count: ${CHANGED_COUNT}"
  echo "- artifacts_dir: ${OUT_DIR}"
  echo "- trace: ${TRACE_JSONL}"
  echo
  if [[ "${MODE}" == "incr" ]]; then
    echo "## Incremental Review Summary"
    echo "- baseline_commit: ${SSOT_REVIEW_BASELINE_COMMIT:-<none>}"
    echo "- new_range: ${SSOT_REVIEW_BASELINE_COMMIT:-<none>}..${HEAD}"
    echo "- previous_open: [${SSOT_REVIEW_OPEN_CSV}]"
    echo "- previous_fixed: [${SSOT_REVIEW_FIXED_CSV}]"
    echo "- previous_partially_fixed: [${SSOT_REVIEW_PARTIAL_CSV}]"
    echo "- enforce: only process SSOT.review.open unless touched new files"
    echo "- processed_open: [${PROCESS_IDS_CSV}]"
    echo "- touched_new_files_count: ${TOUCHED_NEW_FILES_COUNT}"
    echo "- touched_new_files: [${TOUCHED_NEW_FILES}]"
    echo "- new_findings: [${new_ids_csv}]"
    echo "- new_reason: ${new_reason}"
    echo
  fi
  echo
  echo "## Evidence Collected"
  echo "- changed_files: ${CHANGED_FILES}"
  echo "- diff_patch: ${DIFF_PATCH}"
  echo "- commits: ${COMMITS_TXT}"
  echo "- summary: ${SUMMARY_TXT}"
  echo
  echo "## Findings"
  echo
  awk '
    function q(s) { gsub(/^[[:space:]]+|[[:space:]]+$/, "", s); return s }
    function stripq(s) { gsub(/^"/, "", s); gsub(/"$/, "", s); return s }
    function join_cmd(x, cmd) {
      cmd=stripq(cmd)
      if (cmd == "") return x
      if (x == "") return "`" cmd "`"
      return x "; `" cmd "`"
    }
    function flush() {
      if (id == "") return
      idx++
      must=(must_fix=="true"?"Yes":"No")
      min=(vmin==""?"(none)":vmin)
      opt=(vopt==""?"(none)":vopt)
      cln=(cleanup==""?"(none)":cleanup)
      print "- [" idx "] (" id ") **类型**: " type "  **Must fix**: " must
      print "  - **影响面**: " impact
      print "  - **范围**: " scope
      print "  - **概率**: " likelihood
      print "  - **位置**: `" location "`"
      print "  - **问题**: " title
      print "  - **建议动作**: " advice
      print "  - **验证（最小，必须）**: " min
      print "  - **验证（推荐，可选）**: " opt
      print "  - **清理**: " cln
      print ""
      id=""; status=""; type=""; must_fix=""; impact=""; scope=""; likelihood=""; location=""; title=""; advice=""
      vmin=""; vopt=""; cleanup=""; sec=""
    }

    /^  - id: / { flush(); id=$3; next }
    /^    status: / { status=$2; next }
    /^    type: / { sub(/^    type: /, "", $0); type=stripq(q($0)); next }
    /^    must_fix: / { must_fix=$2; next }
    /^    impact: / { sub(/^    impact: /, "", $0); impact=stripq(q($0)); next }
    /^    scope: / { sub(/^    scope: /, "", $0); scope=stripq(q($0)); next }
    /^    likelihood: / { sub(/^    likelihood: /, "", $0); likelihood=stripq(q($0)); next }
    /^    location: / { sub(/^    location: /, "", $0); location=stripq(q($0)); next }
    /^    title: / { sub(/^    title: /, "", $0); title=stripq(q($0)); next }
    /^    advice: / { sub(/^    advice: /, "", $0); advice=stripq(q($0)); next }
    /^    verify_min:/ { sec="vmin"; next }
    /^    verify_opt:/ { sec="vopt"; next }
    /^    cleanup:/ { sec="cleanup"; next }
    /^    [A-Za-z0-9_]+:/ { sec=""; next }
    /^      - / {
      sub(/^      - /, "", $0)
      if (sec=="vmin") vmin=join_cmd(vmin, $0)
      else if (sec=="vopt") vopt=join_cmd(vopt, $0)
      else if (sec=="cleanup") cleanup=join_cmd(cleanup, $0)
      next
    }
    END { flush() }
  ' "${FINDINGS_YAML}"
} >"${REVIEW_MD}"

# next_actions.md (must-fix first)
{
  echo "# Next Actions (from strict review)"
  echo
  echo "Artifacts: ${OUT_DIR}"
  echo
  echo "1) Fix open must-fix findings first:"
  awk '
    /^  - id: /{id=$3}
    /^    must_fix: true$/{must=1}
    /^    status: open$/{ if (id != "" && must == 1) { print "   - " id }; id=""; must=0 }
  ' "${FINDINGS_YAML}" | sed -n '1,50p'
  echo
  echo "2) Re-run (incremental re-review):"
  echo "   bash ai/projects/topru-ai/verify/strict_review_fix_oneclick.sh --base ${BASE} --head ${HEAD} --mode incr"
} >"${NEXT_ACTIONS_MD}"

trace 2 "findings+docs written" "ok" "{\"findings_yaml\":\"${FINDINGS_YAML}\",\"review_md\":\"${REVIEW_MD}\",\"next_actions\":\"${NEXT_ACTIONS_MD}\"}"

# Findings stats (UX): print to stdout and persist in summary.txt.
list_ids_by_status() {
  # list_ids_by_status <status> (prints IDs, one per line)
  local st="$1"
  awk -v st="${st}" '
    /^  - id: /{id=$3}
    /^    status: /{
      if (id != "" && $2 == st) print id
      id=""
    }
  ' "${FINDINGS_YAML}" 2>/dev/null || true
}

finding_field() {
  # finding_field <id> <field>
  local id="$1"
  local key="$2"
  awk -v id="${id}" -v k="${key}" '
    $0 ~ "^  - id: "id"$" {inside=1; next}
    inside==1 && $0 ~ "^  - id: " && $0 !~ "^  - id: "id"$" {exit}
    inside==1 && $0 ~ "^    "k":" {
      sub("^    "k":[[:space:]]*", "", $0)
      gsub(/"/, "", $0)
      print $0
      exit
    }
  ' "${FINDINGS_YAML}" 2>/dev/null || true
}

finding_list_inline() {
  # finding_list_inline <id> <key>
  local id="$1"
  local key="$2"
  awk -v id="${id}" -v k="${key}" '
    $0 ~ "^  - id: "id"$" {inside=1; next}
    inside==1 && $0 ~ "^  - id: " && $0 !~ "^  - id: "id"$" {exit}
    inside==1 && $0 ~ "^    "k":\\s*$" {inlist=1; next}
    inside==1 && inlist==1 && $0 ~ "^    [A-Za-z0-9_]+:" {exit}
    inside==1 && inlist==1 && $0 ~ "^      - " {
      sub("^      -[[:space:]]*", "", $0)
      gsub(/"/, "", $0)
      if (out == "") out=$0
      else out=out"; "$0
      next
    }
    END { print out }
  ' "${FINDINGS_YAML}" 2>/dev/null || true
}

write_fix_queue_item() {
  # write_fix_queue_item <id>
  local id="$1"
  local must_fix_raw must_fix location advice verify_min cleanup
  must_fix_raw="$(finding_field "${id}" "must_fix")"
  if [[ "${must_fix_raw}" == "true" ]]; then
    must_fix="Yes"
  else
    must_fix="No"
  fi
  location="$(finding_field "${id}" "location")"
  advice="$(finding_field "${id}" "advice")"
  verify_min="$(finding_list_inline "${id}" "verify_min")"
  cleanup="$(finding_list_inline "${id}" "cleanup")"
  if [[ -z "${verify_min}" ]]; then
    verify_min="(none)"
  fi
  if [[ -z "${cleanup}" ]]; then
    cleanup="(none)"
  fi

  {
    echo "- finding_id: ${id}"
    echo "  must_fix: ${must_fix}"
    echo "  location: ${location:-<unknown>}"
    echo "  recommended_fix: ${advice:-TODO}"
    echo "  verify_min: ${verify_min}"
    echo "  cleanup: ${cleanup}"
    echo "  next_cmd: bash ai/projects/topru-ai/verify/fix_one_by_one.sh --id ${id} --base ${BASE} --head ${HEAD}"
  } >> "${FIX_QUEUE_MD}"
}

findings_total="$(awk '/^  - id: /{c++} END{print c+0}' "${FINDINGS_YAML}" 2>/dev/null || echo 0)"
open_ids="$(list_ids_by_status open)"
fixed_ids="$(list_ids_by_status fixed)"
partial_ids="$(list_ids_by_status partially_fixed)"

open_count="$(echo "${open_ids}" | sed '/^$/d' | wc -l | tr -d ' ')"
fixed_count="$(echo "${fixed_ids}" | sed '/^$/d' | wc -l | tr -d ' ')"
partial_count="$(echo "${partial_ids}" | sed '/^$/d' | wc -l | tr -d ' ')"

open_list="$(echo "${open_ids}" | sed '/^$/d' | head -n 10 | paste -sd ',' - 2>/dev/null || true)"
fixed_list="$(echo "${fixed_ids}" | sed '/^$/d' | head -n 10 | paste -sd ',' - 2>/dev/null || true)"
partial_list="$(echo "${partial_ids}" | sed '/^$/d' | head -n 10 | paste -sd ',' - 2>/dev/null || true)"

first_open_id="$(echo "${open_ids}" | sed '/^$/d' | head -n 1 || true)"

result="FAIL"
if [[ "${open_count}" == "0" ]]; then
  result="PASS"
fi

{
  echo "# Fix Queue"
  echo
  echo "- run_id: ${RUN_ID}"
  echo "- range: ${BASE}..${HEAD}"
  echo "- open: [${open_list}]"
  echo
} > "${FIX_QUEUE_MD}"

open_must_ids="$(
  awk '
    /^  - id: / {id=$3; next}
    /^    must_fix: true$/ {must=1; next}
    /^    status: open$/ {
      if (id != "" && must == 1) print id
      id=""; must=0
      next
    }
    /^    status: / {
      id=""; must=0
      next
    }
  ' "${FINDINGS_YAML}" 2>/dev/null || true
)"
open_non_must_ids="$(
  awk '
    /^  - id: / {id=$3; must=0; next}
    /^    must_fix: true$/ {must=1; next}
    /^    status: open$/ {
      if (id != "" && must != 1) print id
      id=""; must=0
      next
    }
    /^    status: / {
      id=""; must=0
      next
    }
  ' "${FINDINGS_YAML}" 2>/dev/null || true
)"

if [[ "${open_count}" == "0" ]]; then
  echo "no open findings" >> "${FIX_QUEUE_MD}"
else
  while IFS= read -r rid; do
    [[ -n "${rid}" ]] || continue
    write_fix_queue_item "${rid}"
  done <<< "${open_must_ids}"
  while IFS= read -r rid; do
    [[ -n "${rid}" ]] || continue
    write_fix_queue_item "${rid}"
  done <<< "${open_non_must_ids}"
fi

{
  echo
  echo "FINDINGS_STATS"
  echo "findings_total=${findings_total}"
  echo "open_count=${open_count} open=[${open_list}]"
  echo "fixed_count=${fixed_count} fixed=[${fixed_list}]"
  echo "partially_fixed_count=${partial_count} partially_fixed=[${partial_list}]"
  echo "result=${result}"
  echo "open=[${open_list}]"
  echo "fix_queue=${FIX_QUEUE_MD}"
  echo "review_md=${REVIEW_MD}"
  echo "findings_yaml=${FINDINGS_YAML}"
} | tee -a "${SUMMARY_TXT}"

if [[ "${BASE}" == "${HEAD}" && "${CHANGED_COUNT}" == "0" && "${open_count}" == "0" ]]; then
  echo "no diff and no open findings; skipping review work" | tee -a "${SUMMARY_TXT}"
fi

# review run manifest (commit-bound)
stage 3 "write manifest.json"
cat >"${MANIFEST_JSON}" <<EOF
{
  "run_id": "${RUN_ID}",
  "type": "strict_review_fix_oneclick",
  "mode": "${MODE}",
  "base": "${BASE}",
  "head": "${HEAD}",
  "time": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "commit": "$(git -C "${REPO_DIR}" rev-parse HEAD)",
  "patch_id": "$(git -C "${REPO_DIR}" show HEAD | git patch-id --stable | awk '{print $1}')",
  "artifacts": [
    "run.log",
    "changed_files.txt",
    "diff.patch",
    "patch.diff",
    "commits.txt",
    "summary.txt",
    "review.md",
    "findings.yaml",
    "next_actions.md",
    "fix_queue.md",
    "fix_queue.json",
    "trace.jsonl",
    "manifest.json",
    "result.json"
  ]
}
EOF

# -------------------------
# Step 3: patch SSOT review
# -------------------------
if [[ "${PATCH_SSOT}" == "1" ]]; then
  stage 4 "patch SSOT review block"
  ART_DIR_REL="ai/projects/topru-ai/artifacts/review/${RUN_ID}"
  set +e
  python3 "${PROJECT_DIR}/verify/ssot_patch_review.py" \
    --op ensure \
    --ssot "${SSOT_FILE}" \
    --base "${BASE}" \
    --head "${HEAD}" \
    --run-range "${BASE}..${HEAD}" \
    --artifacts-dir "${ART_DIR_REL}" \
    --review-md "${ART_DIR_REL}/review.md" \
    --findings-yaml "${ART_DIR_REL}/findings.yaml" \
    --next-actions "${ART_DIR_REL}/next_actions.md"
  rc_ensure=$?
  python3 "${PROJECT_DIR}/verify/ssot_patch_review.py" \
    --op update \
    --ssot "${SSOT_FILE}" \
    --base "${BASE}" \
    --head "${HEAD}" \
    --run-range "${BASE}..${HEAD}" \
    --artifacts-dir "${ART_DIR_REL}" \
    --review-md "${ART_DIR_REL}/review.md" \
    --findings-yaml "${ART_DIR_REL}/findings.yaml" \
    --next-actions "${ART_DIR_REL}/next_actions.md"
  rc_update=$?
  set -e

  if [[ "${rc_ensure}" == "0" && "${rc_update}" == "0" ]]; then
    SSOT_PATCH_RESULT="ok"
    trace 4 "ssot patched" "ok" "{\"op\":\"ensure+update\",\"ssot\":\"${SSOT_FILE}\"}"
  else
    SSOT_PATCH_RESULT="fail(ensure=${rc_ensure},update=${rc_update})"
    trace 4 "ssot patch failed" "fail" "{\"ensure\":${rc_ensure},\"update\":${rc_update}}"
    FINAL_RC=3
  fi
else
  echo "SSOT patch disabled (--no-patch-ssot)"
fi

# -------------------------
# Step 5 (best-effort): PR-ready check (audit + review-open)
# -------------------------
echo
echo "=== PR-ready check (best-effort) ==="
stage 5 "audit SSOT pr_ready + compute PR_READY"
AUDIT_FILE="${OUT_DIR}/audit.txt"
if [[ -x "${PROJECT_DIR}/verify/audit_ssot.sh" ]]; then
  AUDIT_OUT_FILE="${AUDIT_FILE}" bash "${PROJECT_DIR}/verify/audit_ssot.sh" \
    --project "ai/projects/topru-ai" \
    --track "resource-observability-topru" \
    --protocol-root "ai/ai-change-gates" || true
else
  echo "NOTE: audit_ssot.sh not found/executable; skipped"
fi

OPEN_COUNT="$( (grep -E '^[[:space:]]*status:[[:space:]]*open[[:space:]]*$' "${FINDINGS_YAML}" || true) | wc -l | tr -d ' ' )"
if [[ "${OPEN_COUNT}" == "0" ]]; then
  echo "REVIEW_OPEN=0"
else
  echo "REVIEW_OPEN=${OPEN_COUNT}"
fi

# Also print PASS/FAIL per Stage2 rule (PASS when open is empty).
echo "RESULT=${result}"

AUDIT_STATUS="unknown"
if [[ -f "${AUDIT_FILE}" ]]; then
  AUDIT_STATUS="$(awk -F': ' '/^pr_ready_status: /{print $2; exit}' "${AUDIT_FILE}" || true)"
fi
echo "AUDIT_PR_READY=${AUDIT_STATUS}"

PR_READY="false"
if [[ "${OPEN_COUNT}" == "0" && "${AUDIT_STATUS}" == "true" ]]; then
  PR_READY="true"
fi
echo "PR_READY=${PR_READY}"
echo "Artifacts: ${OUT_DIR}"

trace 5 "pr_ready computed" "ok" "{\"review_open\":${OPEN_COUNT},\"audit_pr_ready\":\"${AUDIT_STATUS}\",\"pr_ready\":\"${PR_READY}\"}"

# Exit code policy:
# - 0: script ran + SSOT patch ok + PR_READY=true
# - 3: script ran + SSOT patch ok but PR_READY=false (or patch failed)
if [[ "${PR_READY}" == "true" && "${SSOT_PATCH_RESULT}" == "ok" ]]; then
  FINAL_RC=0
else
  FINAL_RC="${FINAL_RC:-3}"
  if [[ "${FINAL_RC}" == "0" ]]; then
    FINAL_RC=3
  fi
fi

NEXT_CMD="NONE"
if [[ "${OPEN_COUNT}" != "0" ]]; then
  NEXT_CMD="bash ai/projects/topru-ai/verify/fix_one_by_one.sh --id ${first_open_id} --base ${BASE} --head ${HEAD}"
elif [[ "${PR_READY}" != "true" ]]; then
  NEXT_CMD="PR_READY_INCLUDE_REVIEW=1 bash ai/projects/topru-ai/verify/run_pr_ready_oneclick.sh"
fi

if [[ "${FINAL_RC}" == "0" ]]; then
  final_result="PASS"
else
  final_result="FAIL"
fi

{
  echo "{"
  echo "  \"schema_version\": \"v1\","
  echo "  \"run_id\": $(json_quote "${RUN_ID}"),"
  echo "  \"mode\": $(json_quote "${MODE}"),"
  echo "  \"base\": $(json_quote "${BASE}"),"
  echo "  \"head\": $(json_quote "${HEAD}"),"
  echo "  \"range\": $(json_quote "${BASE}..${HEAD}"),"
  echo "  \"result\": $(json_quote "${final_result}"),"
  echo "  \"pr_ready\": $(json_quote "${PR_READY}"),"
  echo "  \"open_count\": ${OPEN_COUNT},"
  echo "  \"open_list\": $(json_quote "${open_list}"),"
  echo "  \"next\": $(json_quote "${NEXT_CMD}"),"
  echo "  \"art_dir\": $(json_quote "${OUT_DIR}"),"
  echo "  \"paths\": {"
  echo "    \"trace\": $(json_quote "${TRACE_JSONL}"),"
  echo "    \"manifest\": $(json_quote "${MANIFEST_JSON}"),"
  echo "    \"patch\": $(json_quote "${PATCH_DIFF}"),"
  echo "    \"review_md\": $(json_quote "${REVIEW_MD}"),"
  echo "    \"findings_yaml\": $(json_quote "${FINDINGS_YAML}"),"
  echo "    \"fix_queue_md\": $(json_quote "${FIX_QUEUE_MD}"),"
  echo "    \"fix_queue\": $(json_quote "${FIX_QUEUE_JSON}"),"
  echo "    \"result_json\": $(json_quote "${RESULT_JSON}")"
  echo "  }"
  echo "}"
} > "${RESULT_JSON}"

{
  echo "{"
  echo "  \"schema_version\": \"v1\","
  echo "  \"run_id\": $(json_quote "${RUN_ID}"),"
  echo "  \"open_count\": ${OPEN_COUNT},"
  echo "  \"items\": ["
  idx=0
  while IFS= read -r rid; do
    [[ -n "${rid}" ]] || continue
    if [[ "${idx}" != "0" ]]; then
      echo "    ,"
    fi
    echo "    {\"id\": $(json_quote "${rid}"), \"next\": $(json_quote "bash ai/projects/topru-ai/verify/fix_one_by_one.sh --id ${rid} --base ${BASE} --head ${HEAD}")}"
    idx=$((idx+1))
  done <<< "$(echo "${open_ids}" | sed '/^$/d')"
  echo "  ]"
  echo "}"
} > "${FIX_QUEUE_JSON}"

print_summary
echo "RESULT=${final_result} RUN_ID=${RUN_ID} ART_DIR=${OUT_DIR} NEXT=\"${NEXT_CMD}\""
echo "DETAIL trace.jsonl=${TRACE_JSONL}"
echo "DETAIL manifest.json=${MANIFEST_JSON}"
echo "DETAIL patch.diff=${PATCH_DIFF}"
echo "DETAIL review.md=${REVIEW_MD}"
echo "DETAIL findings.yaml=${FINDINGS_YAML}"
echo "DETAIL fix_queue.md=${FIX_QUEUE_MD}"
echo "DETAIL result.json=${RESULT_JSON}"
exit "${FINAL_RC}"
