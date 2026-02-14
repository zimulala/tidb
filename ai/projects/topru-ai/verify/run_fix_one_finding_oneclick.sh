#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "${ONECLICK_SUPPRESS_NOTICE:-0}" != "1" ]]; then
  echo "NOTICE: prefer oneclick.sh fix" >&2
fi

usage() {
  cat <<'USAGE'
RUN_FIX_ONE_FINDING_ONECLICK

Usage:
  bash ai/projects/topru-ai/verify/run_fix_one_finding_oneclick.sh [--id Rn] [--artifacts-dir <dir>] \
    [--apply] [--verify-min] [--recipe-only] [--impl-cmd "<cmd>"] [--overrides-file <path>] [--project-root <path>]

Behavior:
  - Unified per-finding fix entrypoint.
  - Reads finding from latest review findings.yaml (from SSOT.review.last_run by default).
  - Routes style/format findings to fix_one_by_one.sh.
  - Routes structural findings to implement_oneclick.sh (plan first; exec only when --apply and command available).
  - Optionally runs verify_min (including recipe IDs via expand_verify_recipe.sh).
  - If --overrides-file is omitted, defaults to ai/projects/topru-ai/verify/verify_recipes.override.yaml when present.
USAGE
}

FINDING_ID=""
ARTIFACTS_DIR=""
APPLY="0"
VERIFY_MIN="0"
RECIPE_ONLY="0"
IMPL_CMD=""
OVERRIDES_FILE=""
PROJECT_ROOT="ai/projects/topru-ai"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --id)
      FINDING_ID="$2"
      shift 2
      ;;
    --artifacts-dir)
      ARTIFACTS_DIR="$2"
      shift 2
      ;;
    --apply)
      APPLY="1"
      shift
      ;;
    --verify-min)
      VERIFY_MIN="1"
      shift
      ;;
    --recipe-only)
      RECIPE_ONLY="1"
      shift
      ;;
    --impl-cmd)
      IMPL_CMD="$2"
      shift 2
      ;;
    --overrides-file)
      OVERRIDES_FILE="$2"
      shift 2
      ;;
    --project-root)
      PROJECT_ROOT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
done

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

VERIFY_DIR="${PROJECT_ROOT}/verify"
SSOT_FILE="${PROJECT_ROOT}/PROJECT_STATE.md"
LIB_ONECLICK="${VERIFY_DIR}/lib_oneclick.sh"
FIX_ONE_SCRIPT="${VERIFY_DIR}/fix_one_by_one.sh"
IMPLEMENT_SCRIPT="${VERIFY_DIR}/implement_oneclick.sh"
SSOT_PATCH_REVIEW="${VERIFY_DIR}/ssot_patch_review.py"
EXPAND_RECIPE_TOOL="ai/ai-change-gates/tools/expand_verify_recipe.sh"
DEFAULT_RECIPE_OVERRIDES="${PROJECT_ROOT}/verify/verify_recipes.override.yaml"

if [[ -z "${OVERRIDES_FILE}" && -f "${DEFAULT_RECIPE_OVERRIDES}" ]]; then
  OVERRIDES_FILE="${DEFAULT_RECIPE_OVERRIDES}"
fi

[[ -f "${SSOT_FILE}" ]] || { echo "ERROR: SSOT not found: ${SSOT_FILE}" >&2; exit 2; }
[[ -f "${FIX_ONE_SCRIPT}" ]] || { echo "ERROR: missing ${FIX_ONE_SCRIPT}" >&2; exit 2; }
[[ -f "${IMPLEMENT_SCRIPT}" ]] || { echo "ERROR: missing ${IMPLEMENT_SCRIPT}" >&2; exit 2; }
[[ -f "${SSOT_PATCH_REVIEW}" ]] || { echo "ERROR: missing ${SSOT_PATCH_REVIEW}" >&2; exit 2; }
[[ -x "${EXPAND_RECIPE_TOOL}" ]] || { echo "ERROR: missing ${EXPAND_RECIPE_TOOL}" >&2; exit 2; }

if [[ -f "${LIB_ONECLICK}" ]]; then
  ONECLICK_DISABLE_ERR_TRAP=1
  # shellcheck source=/dev/null
  source "${LIB_ONECLICK}"
  ONECLICK_DISABLE_ERR_TRAP=0
fi

if ! command -v now_iso >/dev/null 2>&1; then
  now_iso() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
fi
if ! command -v patch_id_of_head >/dev/null 2>&1; then
  patch_id_of_head() { (git show HEAD | git patch-id --stable | awk '{print $1}') 2>/dev/null || echo "UNKNOWN"; }
fi
if ! command -v env_string >/dev/null 2>&1; then
  env_string() { echo "$(uname -s)/$(uname -m)"; }
fi
if ! declare -F trace >/dev/null 2>&1; then
  trace() {
    local st="$1"; local msg="$2"; local status="$3"; local details="${4:-{}}"
    local t; t="$(now_iso)"
    msg="${msg//\"/\' }"
    echo "{\"time\":\"${t}\",\"stage\":\"${st}\",\"status\":\"${status}\",\"msg\":\"${msg}\",\"details\":${details}}" >> "${TRACE_JSONL}" 2>/dev/null || true
  }
fi
if ! declare -F stage >/dev/null 2>&1; then
  stage() {
    echo "[run_fix][stage $1] $2"
    trace "$1" "$2" "ok" "{}"
  }
fi
if ! declare -F init_trace >/dev/null 2>&1; then
  init_trace() {
    : > "${TRACE_JSONL}" 2>/dev/null || true
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

to_rel_path() {
  local p="$1"
  if [[ "${p}" == "${ROOT}/"* ]]; then
    echo "${p#${ROOT}/}"
  else
    echo "${p}"
  fi
}

resolve_abs_path() {
  local p="$1"
  local abs_dir
  if [[ -z "${p}" ]]; then
    echo ""
    return 0
  fi
  if [[ "${p}" == /* ]]; then
    abs_dir="$(cd "$(dirname "${p}")" 2>/dev/null && pwd -P)" || return 1
    echo "${abs_dir}/$(basename "${p}")"
    return 0
  fi
  abs_dir="$(cd "$(dirname "${ROOT}/${p}")" 2>/dev/null && pwd -P)" || return 1
  echo "${abs_dir}/$(basename "${p}")"
}

ssot_review_value() {
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

ssot_review_last_run_value() {
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

finding_field() {
  local file="$1"
  local id="$2"
  local key="$3"
  awk -v id="${id}" -v k="${key}" '
    $0 ~ "^[[:space:]]*-[[:space:]]*id:[[:space:]]*"id"([[:space:]]*$|[[:space:]]*#)" {inside=1; next}
    inside==1 && $0 ~ "^[[:space:]]*-[[:space:]]*id:" && $0 !~ "^[[:space:]]*-[[:space:]]*id:[[:space:]]*"id"([[:space:]]*$|[[:space:]]*#)" {exit}
    inside==1 && $0 ~ "^[[:space:]]*"k":" {
      sub("^[[:space:]]*"k":[[:space:]]*", "", $0)
      gsub(/"/, "", $0)
      print $0
      exit
    }
  ' "${file}" 2>/dev/null || true
}

finding_list() {
  local file="$1"
  local id="$2"
  local key="$3"
  awk -v id="${id}" -v k="${key}" '
    $0 ~ "^[[:space:]]*-[[:space:]]*id:[[:space:]]*"id"([[:space:]]*$|[[:space:]]*#)" {inside=1; next}
    inside==1 && $0 ~ "^[[:space:]]*-[[:space:]]*id:" && $0 !~ "^[[:space:]]*-[[:space:]]*id:[[:space:]]*"id"([[:space:]]*$|[[:space:]]*#)" {exit}
    inside==1 && $0 ~ "^[[:space:]]*"k":[[:space:]]*$" {inlist=1; next}
    inside==1 && inlist==1 && $0 ~ "^[[:space:]]*[A-Za-z0-9_]+:" {exit}
    inside==1 && inlist==1 && $0 ~ "^[[:space:]]*-[[:space:]]+" {
      sub("^[[:space:]]*-[[:space:]]+", "", $0)
      gsub(/^"|"$/, "", $0)
      print $0
    }
  ' "${file}" 2>/dev/null || true
}

first_open_finding_id() {
  local file="$1"
  awk '
    /^[[:space:]]*-[[:space:]]*id:[[:space:]]*/ {
      id=$0
      sub(/^[[:space:]]*-[[:space:]]*id:[[:space:]]*/, "", id)
      gsub(/"/, "", id)
      next
    }
    /^[[:space:]]*status:[[:space:]]*open([[:space:]]|$)/ {
      if (id != "") {
        print id
        exit
      }
    }
  ' "${file}" 2>/dev/null || true
}

update_finding_status() {
  local file="$1"
  local id="$2"
  local new_status="$3"
  local tmp
  tmp="$(mktemp /tmp/findings_status.XXXXXX)"
  awk -v id="${id}" -v st="${new_status}" '
    $0 ~ "^[[:space:]]*-[[:space:]]*id:[[:space:]]*"id"([[:space:]]*$|[[:space:]]*#)" {inside=1; print; next}
    inside==1 && $0 ~ "^[[:space:]]*-[[:space:]]*id:" && $0 !~ "^[[:space:]]*-[[:space:]]*id:[[:space:]]*"id"([[:space:]]*$|[[:space:]]*#)" {inside=0}
    inside==1 && $0 ~ "^[[:space:]]*status:" {
      print "    status: "st
      inside=0
      next
    }
    {print}
  ' "${file}" > "${tmp}"
  mv "${tmp}" "${file}"
}

normalize_recipe_id() {
  local v="$1"
  v="${v#RECIPE:}"
  v="${v#recipe:}"
  echo "${v}"
}

is_recipe_token() {
  local v="$1"
  [[ "${v}" =~ ^(RECIPE:|recipe:)?V_[A-Z0-9_]+$ ]]
}

infer_recipe_from_finding() {
  local text="$1"
  local t
  t="$(echo "${text}" | tr '[:upper:]' '[:lower:]')"
  if echo "${t}" | grep -Eq 'interval|window|determinis'; then
    echo "V_INTERVAL_DETERMINISM"
    return 0
  fi
  if echo "${t}" | grep -Eq 'cancel|exit|goroutine|hang'; then
    echo "V_CONCURRENCY_CANCEL_EXIT"
    return 0
  fi
  if echo "${t}" | grep -Eq 'backpressure|slow consumer|channel block|blocking'; then
    echo "V_BACKPRESSURE_SLOW_CONSUMER"
    return 0
  fi
  if echo "${t}" | grep -Eq 'perf|lock|contention|scalab'; then
    echo "V_PERF_SANITY_AGG"
    return 0
  fi
  echo ""
}

REVIEW_ART_DIR_RAW="${ARTIFACTS_DIR:-$(ssot_review_last_run_value artifacts_dir)}"
[[ -n "${REVIEW_ART_DIR_RAW}" ]] || { echo "ERROR: missing --artifacts-dir and SSOT.review.last_run.artifacts_dir" >&2; exit 2; }
REVIEW_ART_DIR_ABS="$(resolve_abs_path "${REVIEW_ART_DIR_RAW}")" || { echo "ERROR: bad artifacts-dir: ${REVIEW_ART_DIR_RAW}" >&2; exit 2; }

FINDINGS_YAML_RAW="${REVIEW_ART_DIR_ABS}/findings.yaml"
if [[ ! -f "${FINDINGS_YAML_RAW}" ]]; then
  FINDINGS_YAML_RAW="$(ssot_review_last_run_value findings_yaml)"
fi
FINDINGS_YAML_ABS="$(resolve_abs_path "${FINDINGS_YAML_RAW}")" || { echo "ERROR: bad findings path: ${FINDINGS_YAML_RAW}" >&2; exit 2; }
[[ -f "${FINDINGS_YAML_ABS}" ]] || { echo "ERROR: findings.yaml not found: ${FINDINGS_YAML_ABS}" >&2; exit 2; }

if [[ -z "${FINDING_ID}" ]]; then
  FINDING_ID="$(first_open_finding_id "${FINDINGS_YAML_ABS}")"
fi
[[ -n "${FINDING_ID}" ]] || { echo "ERROR: no open finding found and --id not provided" >&2; exit 2; }

OLD_STATUS="$(finding_field "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "status")"
[[ -n "${OLD_STATUS}" ]] || { echo "ERROR: finding not found: ${FINDING_ID}" >&2; exit 2; }

FINDING_TYPE="$(finding_field "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "type")"
FINDING_TITLE="$(finding_field "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "title")"
FINDING_ISSUE="$(finding_field "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "issue")"
FINDING_ADVICE="$(finding_field "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "advice")"
FINDING_CLOSURE="$(finding_field "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "closure")"
FINDING_LOCATION="$(finding_field "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "location")"
VERIFY_MIN_ITEMS="$(finding_list "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "verify_min")"
LOCATION_ITEMS="$(finding_list "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "locations")"

BASE_SHA="$(ssot_review_value baseline_commit)"
HEAD_SHA="$(git rev-parse HEAD 2>/dev/null || true)"
[[ -n "${BASE_SHA}" ]] || BASE_SHA="${HEAD_SHA}"
RANGE="${BASE_SHA}..${HEAD_SHA}"

TIME_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
HEAD_SHORT="$(git rev-parse --short HEAD 2>/dev/null || echo UNKNOWN)"
RUN_ID="${TIME_UTC}_${HEAD_SHORT}_fix_${FINDING_ID}"
ART_DIR="${PROJECT_ROOT}/artifacts/fix/${RUN_ID}"
PLAN_MD="${ART_DIR}/plan.md"
PATCH_DIFF="${ART_DIR}/patch.diff"
VERIFY_MIN_LOG="${ART_DIR}/verify_min.log"
ROUTE_LOG="${ART_DIR}/route.log"
TRACE_JSONL="${ART_DIR}/trace.jsonl"
MANIFEST_JSON="${ART_DIR}/manifest.json"
RESULT_JSON="${ART_DIR}/result.json"

mkdir -p "${ART_DIR}"
: > "${PLAN_MD}"
: > "${PATCH_DIFF}"
: > "${VERIFY_MIN_LOG}"
: > "${ROUTE_LOG}"
: > "${TRACE_JSONL}"
: > "${MANIFEST_JSON}"
: > "${RESULT_JSON}"
init_trace "${ART_DIR}"

TYPE_LC="$(echo "${FINDING_TYPE} ${FINDING_TITLE} ${FINDING_ISSUE} ${FINDING_ADVICE}" | tr '[:upper:]' '[:lower:]')"
STYLE_CLASS="0"
if echo "${TYPE_LC}" | grep -Eq 'style|format|gofmt'; then
  STYLE_CLASS="1"
fi

ROUTE="none"
ROUTE_RC=0
VERIFY_RC=0
VERIFY_COUNT=0
VERIFY_CMDS=""
NEED_HUMAN="0"
STATUS_HINT=""

{
  echo "# Fix Run Plan"
  echo
  echo "- run_id: ${RUN_ID}"
  echo "- finding_id: ${FINDING_ID}"
  echo "- old_status: ${OLD_STATUS}"
  echo "- finding_type: ${FINDING_TYPE:-unknown}"
  echo "- base: ${BASE_SHA}"
  echo "- head: ${HEAD_SHA}"
  echo "- range: ${RANGE}"
  echo "- apply: ${APPLY}"
  echo "- verify_min: ${VERIFY_MIN}"
  echo "- recipe_only: ${RECIPE_ONLY}"
  echo
  echo "## Finding Summary"
  echo "- title: ${FINDING_TITLE:-<none>}"
  echo "- issue: ${FINDING_ISSUE:-<none>}"
  echo "- advice: ${FINDING_ADVICE:-<none>}"
  echo "- location: ${FINDING_LOCATION:-<none>}"
  if [[ -n "${LOCATION_ITEMS}" ]]; then
    echo "- locations:"
    echo "${LOCATION_ITEMS}" | sed 's/^/  - /'
  fi
} > "${PLAN_MD}"

stage 0 "preflight"
trace 0 "start" "ok" "{\"finding_id\":\"${FINDING_ID}\",\"style_class\":${STYLE_CLASS}}"

if [[ "${RECIPE_ONLY}" != "1" ]]; then
  if [[ "${STYLE_CLASS}" == "1" ]]; then
    ROUTE="fix_one_by_one"
    stage 1 "route -> fix_one_by_one"
    cmd=(bash "${FIX_ONE_SCRIPT}" --id "${FINDING_ID}" --base "${BASE_SHA}" --head "${HEAD_SHA}")
    if [[ "${APPLY}" == "1" ]]; then
      cmd+=(--apply)
    fi
    set +e
    "${cmd[@]}" > "${ROUTE_LOG}" 2>&1
    ROUTE_RC=$?
    set -e
    STATUS_HINT="$(awk -F'status=' '/^UPDATED:/{print $2; exit}' "${ROUTE_LOG}" | tr -d '\r' || true)"
  else
    ROUTE="implement_oneclick"
    stage 1 "route -> implement_oneclick --plan"
    set +e
    bash "${IMPLEMENT_SCRIPT}" --plan --base "${BASE_SHA}" --head "${HEAD_SHA}" > "${ROUTE_LOG}" 2>&1
    plan_rc=$?
    set -e
    ROUTE_RC=${plan_rc}

    if [[ "${APPLY}" == "1" ]]; then
      if [[ -z "${IMPL_CMD}" ]]; then
        IMPL_CMD="$(echo "${FINDING_ADVICE}" | sed -nE 's/.*`([^`]+)`.*/\1/p' | head -n1)"
      fi
      if [[ -z "${IMPL_CMD}" ]]; then
        NEED_HUMAN="1"
        echo "NEED_HUMAN: structural finding requires explicit --impl-cmd" >> "${ROUTE_LOG}"
      else
        stage 2 "route -> implement_oneclick --exec"
        set +e
        bash "${IMPLEMENT_SCRIPT}" --exec --base "${BASE_SHA}" --head "${HEAD_SHA}" --cmd "${IMPL_CMD}" >> "${ROUTE_LOG}" 2>&1
        exec_rc=$?
        set -e
        if [[ "${ROUTE_RC}" == "0" ]]; then
          ROUTE_RC=${exec_rc}
        fi
      fi
    else
      NEED_HUMAN="1"
      echo "NEED_HUMAN: plan generated. rerun with --apply --impl-cmd for execution." >> "${ROUTE_LOG}"
    fi
  fi
else
  ROUTE="recipe_only"
  stage 1 "route skipped (--recipe-only)"
fi

# Best-effort patch capture from route log
PATCH_FROM_LOG="$(awk -F= '/^DETAIL patch.diff=/{print $2; exit}' "${ROUTE_LOG}" | tr -d '\r' || true)"
if [[ -n "${PATCH_FROM_LOG}" ]]; then
  PATCH_FROM_ABS="$(resolve_abs_path "${PATCH_FROM_LOG}")" || PATCH_FROM_ABS=""
  if [[ -n "${PATCH_FROM_ABS}" && -f "${PATCH_FROM_ABS}" ]]; then
    cp "${PATCH_FROM_ABS}" "${PATCH_DIFF}" || true
  fi
fi

if [[ ! -s "${PATCH_DIFF}" ]]; then
  echo "# no route patch artifact (route=${ROUTE}, apply=${APPLY})" > "${PATCH_DIFF}"
fi

if [[ "${VERIFY_MIN}" == "1" ]]; then
  stage 3 "verify_min"

  VERIFY_ITEMS="${VERIFY_MIN_ITEMS}"
  if [[ -z "${VERIFY_ITEMS}" ]]; then
    inferred="$(infer_recipe_from_finding "${TYPE_LC}")"
    if [[ -n "${inferred}" ]]; then
      VERIFY_ITEMS="${inferred}"
    fi
  fi

  if [[ -z "${VERIFY_ITEMS}" ]]; then
    VERIFY_RC=1
    NEED_HUMAN="1"
    echo "[verify_min] no verify_min entries or inferable recipes" >> "${VERIFY_MIN_LOG}"
  else
    while IFS= read -r entry; do
      [[ -n "${entry}" ]] || continue
      entry="${entry#${entry%%[![:space:]]*}}"
      entry="${entry%${entry##*[![:space:]]}}"
      entry="${entry#\"}"
      entry="${entry%\"}"
      [[ -n "${entry}" ]] || continue

      if is_recipe_token "${entry}"; then
        rid="$(normalize_recipe_id "${entry}")"
        echo "[verify_min] recipe=${rid}" >> "${VERIFY_MIN_LOG}"
        exp_cmd=(bash "${EXPAND_RECIPE_TOOL}" --recipe-id "${rid}" --project-root "${ROOT}")
        if [[ -n "${OVERRIDES_FILE}" ]]; then
          exp_cmd+=(--overrides-file "${OVERRIDES_FILE}")
        fi
        expanded="$("${exp_cmd[@]}" 2>> "${VERIFY_MIN_LOG}" || true)"
        while IFS= read -r cmd; do
          [[ -n "${cmd}" ]] || continue
          if [[ "${cmd}" == '# '* ]]; then
            echo "${cmd}" >> "${VERIFY_MIN_LOG}"
            continue
          fi
          echo "[verify_min] $cmd" >> "${VERIFY_MIN_LOG}"
          set +e
          bash -lc "${cmd}" >> "${VERIFY_MIN_LOG}" 2>&1
          rc=$?
          set -e
          VERIFY_COUNT=$((VERIFY_COUNT + 1))
          VERIFY_CMDS+="${cmd};"
          if [[ "${rc}" != "0" ]]; then
            VERIFY_RC=1
          fi
        done <<< "${expanded}"
      else
        echo "[verify_min] ${entry}" >> "${VERIFY_MIN_LOG}"
        set +e
        bash -lc "${entry}" >> "${VERIFY_MIN_LOG}" 2>&1
        rc=$?
        set -e
        VERIFY_COUNT=$((VERIFY_COUNT + 1))
        VERIFY_CMDS+="${entry};"
        if [[ "${rc}" != "0" ]]; then
          VERIFY_RC=1
        fi
      fi
    done <<< "${VERIFY_ITEMS}"

    if [[ "${VERIFY_COUNT}" == "0" ]]; then
      VERIFY_RC=1
      NEED_HUMAN="1"
      echo "[verify_min] no runnable commands produced" >> "${VERIFY_MIN_LOG}"
    fi
  fi
fi

UPDATED_STATUS="${OLD_STATUS}"
if [[ "${STATUS_HINT}" == "fixed" || "${STATUS_HINT}" == "partially_fixed" || "${STATUS_HINT}" == "open" ]]; then
  UPDATED_STATUS="${STATUS_HINT}"
else
  closure_ready="0"
  if [[ -n "${FINDING_CLOSURE}" ]] && ! echo "${FINDING_CLOSURE}" | grep -qi '^TODO'; then
    closure_ready="1"
  fi

  if [[ "${NEED_HUMAN}" == "1" ]]; then
    UPDATED_STATUS="open"
  elif [[ "${APPLY}" == "1" && "${ROUTE_RC}" == "0" && "${VERIFY_MIN}" == "1" && "${VERIFY_RC}" == "0" && "${closure_ready}" == "1" ]]; then
    UPDATED_STATUS="fixed"
  elif [[ "${ROUTE_RC}" == "0" || ( "${VERIFY_MIN}" == "1" && "${VERIFY_RC}" == "0" ) ]]; then
    UPDATED_STATUS="partially_fixed"
  else
    UPDATED_STATUS="open"
  fi
fi

if [[ "${VERIFY_MIN}" == "1" && "${VERIFY_RC}" != "0" && "${UPDATED_STATUS}" == "fixed" ]]; then
  UPDATED_STATUS="partially_fixed"
fi

cp "${FINDINGS_YAML_ABS}" "${ART_DIR}/findings.before.yaml"
update_finding_status "${FINDINGS_YAML_ABS}" "${FINDING_ID}" "${UPDATED_STATUS}"
cp "${FINDINGS_YAML_ABS}" "${ART_DIR}/findings.after.yaml"

stage 4 "update SSOT review"
REVIEW_MD_PATH="$(ssot_review_last_run_value review_md)"
NEXT_ACTIONS_PATH="$(ssot_review_last_run_value next_actions)"
BASELINE_COMMIT="$(ssot_review_value baseline_commit)"
GC_PASS_EXISTING="$(ssot_review_last_run_value gatecheck_pass)"
GC_ART_EXISTING="$(ssot_review_last_run_value gatecheck_artifact)"
GC_OMF_EXISTING="$(ssot_review_last_run_value open_must_fix_count)"

[[ -n "${REVIEW_MD_PATH}" ]] || REVIEW_MD_PATH="${PROJECT_ROOT}/artifacts/review/<unknown>/review.md"
[[ -n "${NEXT_ACTIONS_PATH}" ]] || NEXT_ACTIONS_PATH="${PROJECT_ROOT}/artifacts/review/<unknown>/next_actions.md"
[[ -n "${BASELINE_COMMIT}" ]] || BASELINE_COMMIT="${BASE_SHA}"

FINDINGS_YAML_REL="$(to_rel_path "${FINDINGS_YAML_ABS}")"
ART_DIR_REL="$(to_rel_path "${ART_DIR}")"

set +e
python3 "${SSOT_PATCH_REVIEW}" \
  --op ensure \
  --ssot "${SSOT_FILE}" \
  --base "${BASE_SHA}" \
  --head "${HEAD_SHA}" \
  --run-range "${RANGE}" \
  --artifacts-dir "${ART_DIR_REL}" \
  --review-md "${REVIEW_MD_PATH}" \
  --findings-yaml "${FINDINGS_YAML_REL}" \
  --next-actions "${NEXT_ACTIONS_PATH}" \
  --baseline-commit "${BASELINE_COMMIT}" \
  --review-run-id "${RUN_ID}" \
  --review-commit "${HEAD_SHA}" \
  --gatecheck-pass "${GC_PASS_EXISTING}" \
  --gatecheck-artifact "${GC_ART_EXISTING}" \
  --open-must-fix-count "${GC_OMF_EXISTING}" >> "${ROUTE_LOG}" 2>&1
rc_ensure=$?
python3 "${SSOT_PATCH_REVIEW}" \
  --op update \
  --ssot "${SSOT_FILE}" \
  --base "${BASE_SHA}" \
  --head "${HEAD_SHA}" \
  --run-range "${RANGE}" \
  --artifacts-dir "${ART_DIR_REL}" \
  --review-md "${REVIEW_MD_PATH}" \
  --findings-yaml "${FINDINGS_YAML_REL}" \
  --next-actions "${NEXT_ACTIONS_PATH}" \
  --baseline-commit "${BASELINE_COMMIT}" \
  --review-run-id "${RUN_ID}" \
  --review-commit "${HEAD_SHA}" \
  --gatecheck-pass "${GC_PASS_EXISTING}" \
  --gatecheck-artifact "${GC_ART_EXISTING}" \
  --open-must-fix-count "${GC_OMF_EXISTING}" >> "${ROUTE_LOG}" 2>&1
rc_update=$?
set -e

SSOT_RC=0
if [[ "${rc_ensure}" != "0" || "${rc_update}" != "0" ]]; then
  SSOT_RC=1
fi

RESULT_WORD="FAIL"
if [[ "${UPDATED_STATUS}" == "fixed" && "${SSOT_RC}" == "0" && ( "${VERIFY_MIN}" == "0" || "${VERIFY_RC}" == "0" ) ]]; then
  RESULT_WORD="PASS"
fi

NEXT_CMD="NONE"
if [[ "${RESULT_WORD}" == "PASS" ]]; then
  NEXT_CMD="PR_READY_INCLUDE_REVIEW=1 bash ai/projects/topru-ai/verify/run_pr_ready_oneclick.sh"
else
  if [[ "${NEED_HUMAN}" == "1" ]]; then
    if [[ "${STYLE_CLASS}" == "1" ]]; then
      NEXT_CMD="bash ai/projects/topru-ai/verify/run_fix_one_finding_oneclick.sh --id ${FINDING_ID} --apply --verify-min"
    else
      NEXT_CMD="bash ai/projects/topru-ai/verify/run_fix_one_finding_oneclick.sh --id ${FINDING_ID} --apply --impl-cmd '<your_command>' --verify-min"
    fi
  elif [[ "${VERIFY_MIN}" == "1" && "${VERIFY_RC}" != "0" ]]; then
    NEXT_CMD="Fix verify_min failures and rerun: bash ai/projects/topru-ai/verify/run_fix_one_finding_oneclick.sh --id ${FINDING_ID} --verify-min"
  else
    NEXT_CMD="bash ai/projects/topru-ai/verify/strict_review_fix_oneclick.sh --mode targeted --target ${FINDING_ID}"
  fi
fi

cat > "${MANIFEST_JSON}" <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${RUN_ID}"),
  "type": "run_fix_one_finding_oneclick",
  "time": $(json_quote "$(now_iso)"),
  "env": $(json_quote "$(env_string)"),
  "finding_id": $(json_quote "${FINDING_ID}"),
  "route": $(json_quote "${ROUTE}"),
  "route_rc": ${ROUTE_RC},
  "verify_min": $(json_quote "${VERIFY_MIN}"),
  "verify_rc": ${VERIFY_RC},
  "verify_count": ${VERIFY_COUNT},
  "recipe_only": $(json_quote "${RECIPE_ONLY}"),
  "overrides_file": $(json_quote "${OVERRIDES_FILE}"),
  "apply": $(json_quote "${APPLY}"),
  "need_human": $(json_quote "${NEED_HUMAN}"),
  "old_status": $(json_quote "${OLD_STATUS}"),
  "updated_status": $(json_quote "${UPDATED_STATUS}"),
  "base": $(json_quote "${BASE_SHA}"),
  "head": $(json_quote "${HEAD_SHA}"),
  "range": $(json_quote "${RANGE}"),
  "patch_id": $(json_quote "$(patch_id_of_head)"),
  "artifacts": {
    "plan": $(json_quote "${PLAN_MD}"),
    "patch": $(json_quote "${PATCH_DIFF}"),
    "verify_min_log": $(json_quote "${VERIFY_MIN_LOG}"),
    "route_log": $(json_quote "${ROUTE_LOG}"),
    "trace": $(json_quote "${TRACE_JSONL}"),
    "manifest": $(json_quote "${MANIFEST_JSON}"),
    "result": $(json_quote "${RESULT_JSON}")
  }
}
EOF

PATCH_BYTES="$(wc -c < "${PATCH_DIFF}" | tr -d ' ')"
TOUCHED_FILES="$(
  awk '
    /^diff --git / {
      f=$3
      sub(/^a\//, "", f)
      if (f != "" && !seen[f]++) print f
    }
  ' "${PATCH_DIFF}" 2>/dev/null || true
)"
TOUCHED_FILES_COUNT="$(echo "${TOUCHED_FILES}" | sed '/^$/d' | wc -l | tr -d ' ')"
VERIFY_CMDS="${VERIFY_CMDS%;}"
if [[ "${VERIFY_MIN}" == "1" && "${VERIFY_CMDS}" == "" ]]; then
  VERIFY_CMDS="TODO: add verify_min recipes/commands for ${FINDING_ID}"
fi

cat > "${RESULT_JSON}" <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${RUN_ID}"),
  "result": $(json_quote "${RESULT_WORD}"),
  "finding_id": $(json_quote "${FINDING_ID}"),
  "status_transition_proposal": {
    "before": $(json_quote "${OLD_STATUS}"),
    "after": $(json_quote "${UPDATED_STATUS}")
  },
  "route": $(json_quote "${ROUTE}"),
  "route_rc": ${ROUTE_RC},
  "verify_min": {
    "enabled": $(json_quote "${VERIFY_MIN}"),
    "rc": ${VERIFY_RC},
    "count": ${VERIFY_COUNT},
    "commands": $(json_quote "${VERIFY_CMDS}"),
    "overrides_file": $(json_quote "${OVERRIDES_FILE}")
  },
  "need_human": $(json_quote "${NEED_HUMAN}"),
  "next": $(json_quote "${NEXT_CMD}"),
  "base": $(json_quote "${BASE_SHA}"),
  "head": $(json_quote "${HEAD_SHA}"),
  "range": $(json_quote "${RANGE}"),
  "patch_bytes": ${PATCH_BYTES},
  "touched_files_count": ${TOUCHED_FILES_COUNT},
  "art_dir": $(json_quote "${ART_DIR}"),
  "paths": {
    "plan": $(json_quote "${PLAN_MD}"),
    "patch": $(json_quote "${PATCH_DIFF}"),
    "verify_min_log": $(json_quote "${VERIFY_MIN_LOG}"),
    "route_log": $(json_quote "${ROUTE_LOG}"),
    "trace": $(json_quote "${TRACE_JSONL}"),
    "manifest": $(json_quote "${MANIFEST_JSON}"),
    "result_json": $(json_quote "${RESULT_JSON}")
  }
}
EOF

if [[ "${NEED_HUMAN}" == "1" ]]; then
  echo "NEED_HUMAN: blocked on explicit decision for ${FINDING_ID}"
  if [[ "${STYLE_CLASS}" == "1" ]]; then
    echo "STEP1: rerun with --apply to allow style fix application"
    echo "STEP2: rerun --verify-min to confirm closure"
  else
    echo "STEP1: provide --impl-cmd '<safe command>' and optionally --apply"
    echo "STEP2: rerun --verify-min to validate closure"
  fi
fi

echo "RESULT=${RESULT_WORD} RUN_ID=${RUN_ID} ART_DIR=${ART_DIR} NEXT=\"${NEXT_CMD}\""
echo "UPDATED: ${FINDING_ID} status=${UPDATED_STATUS}"
echo "DETAIL plan.md=${PLAN_MD}"
echo "DETAIL patch.diff=${PATCH_DIFF}"
echo "DETAIL verify_min.log=${VERIFY_MIN_LOG}"
echo "DETAIL trace.jsonl=${TRACE_JSONL}"
echo "DETAIL manifest.json=${MANIFEST_JSON}"
echo "DETAIL result.json=${RESULT_JSON}"

if [[ "${RESULT_WORD}" == "PASS" ]]; then
  exit 0
fi
exit 1
