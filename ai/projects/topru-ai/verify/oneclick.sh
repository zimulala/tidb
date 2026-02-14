#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'EOF'
ONECLICK

Usage:
  bash ai/projects/topru-ai/verify/oneclick.sh [pr-ready] [--include-review] [--base <sha>] [--head <sha>] [args...]
  bash ai/projects/topru-ai/verify/oneclick.sh impl [args...]
  bash ai/projects/topru-ai/verify/oneclick.sh fix [args...]
  bash ai/projects/topru-ai/verify/oneclick.sh self-check [args...]

Default subcommand:
  pr-ready

Notes:
  - Keeps legacy scripts fully available.
  - Normalizes footer output to:
      RESULT=...
      NEXT: ...
      DETAILS: ...
  - If downstream has no JSON marker block, emits a minimal marker JSON.
EOF
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

extract_key_value() {
  # extract_key_value <file> <prefix>
  local file="$1"
  local prefix="$2"
  local line
  line="$(grep -E "^${prefix}" "${file}" | tail -n 1 || true)"
  if [[ -z "${line}" ]]; then
    echo ""
    return 0
  fi
  echo "${line#${prefix}}"
}

extract_result_field() {
  # extract_result_field <line> <field-name>
  local line="$1"
  local field="$2"
  echo "${line}" | sed -nE "s/.*${field}=([^ ]+).*/\\1/p" | head -n 1
}

extract_result_next() {
  # parse NEXT=... from RESULT line, supports quoted and unquoted value
  local line="$1"
  local next
  next="$(echo "${line}" | sed -nE 's/.* NEXT="([^"]*)".*/\1/p' | head -n 1)"
  if [[ -n "${next}" ]]; then
    echo "${next}"
    return 0
  fi
  echo "${line}" | sed -nE 's/.* NEXT=([^ ]+).*/\1/p' | head -n 1
}

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

VERIFY_DIR="ai/projects/topru-ai/verify"
PROJECT_ROOT="ai/projects/topru-ai"

if [[ $# -gt 0 ]]; then
  case "$1" in
    pr-ready|impl|fix|self-check)
      SUBCOMMAND="$1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      SUBCOMMAND="pr-ready"
      ;;
  esac
else
  SUBCOMMAND="pr-ready"
fi

ENV_OVERRIDES=()
FORWARD_ARGS=()
DOWNSTREAM=""

case "${SUBCOMMAND}" in
  pr-ready)
    DOWNSTREAM="${VERIFY_DIR}/run_pr_ready_oneclick.sh"
    INCLUDE_REVIEW=0
    BASE_SHA=""
    HEAD_SHA=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --include-review)
          INCLUDE_REVIEW=1
          shift
          ;;
        --base)
          BASE_SHA="$2"
          shift 2
          ;;
        --head)
          HEAD_SHA="$2"
          shift 2
          ;;
        *)
          FORWARD_ARGS+=("$1")
          shift
          ;;
      esac
    done
    if [[ "${INCLUDE_REVIEW}" == "1" ]]; then
      ENV_OVERRIDES+=("PR_READY_INCLUDE_REVIEW=1")
    fi
    if [[ -n "${BASE_SHA}" ]]; then
      ENV_OVERRIDES+=("ONECLICK_BASE_SHA=${BASE_SHA}")
    fi
    if [[ -n "${HEAD_SHA}" ]]; then
      echo "NOTICE: --head is not consumed by run_pr_ready_oneclick.sh; ignored by oneclick wrapper" >&2
    fi
    ;;
  impl)
    DOWNSTREAM="${VERIFY_DIR}/implement_oneclick.sh"
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --include-review)
          echo "NOTICE: --include-review is only for pr-ready; ignored for impl" >&2
          shift
          ;;
        *)
          FORWARD_ARGS+=("$1")
          shift
          ;;
      esac
    done
    ;;
  fix)
    DOWNSTREAM="${VERIFY_DIR}/run_fix_one_finding_oneclick.sh"
    FORWARD_ARGS=("$@")
    ;;
  self-check)
    DOWNSTREAM="${VERIFY_DIR}/self_check_oneclick.sh"
    FORWARD_ARGS=("$@")
    ;;
  *)
    echo "ERROR: unsupported subcommand: ${SUBCOMMAND}" >&2
    usage
    exit 2
    ;;
esac

[[ -f "${DOWNSTREAM}" ]] || { echo "ERROR: downstream script not found: ${DOWNSTREAM}" >&2; exit 2; }

TMP_OUT="$(mktemp)"
TMP_ERR="$(mktemp)"
trap 'rm -f "${TMP_OUT}" "${TMP_ERR}"' EXIT

set +e
if [[ ${#ENV_OVERRIDES[@]} -gt 0 ]]; then
  if [[ ${#FORWARD_ARGS[@]} -gt 0 ]]; then
    env ONECLICK_SUPPRESS_NOTICE=1 "${ENV_OVERRIDES[@]}" bash "${DOWNSTREAM}" "${FORWARD_ARGS[@]}" \
      > "${TMP_OUT}" \
      2> "${TMP_ERR}"
    DOWNSTREAM_RC=$?
  else
    env ONECLICK_SUPPRESS_NOTICE=1 "${ENV_OVERRIDES[@]}" bash "${DOWNSTREAM}" \
      > "${TMP_OUT}" \
      2> "${TMP_ERR}"
    DOWNSTREAM_RC=$?
  fi
else
  if [[ ${#FORWARD_ARGS[@]} -gt 0 ]]; then
    ONECLICK_SUPPRESS_NOTICE=1 bash "${DOWNSTREAM}" "${FORWARD_ARGS[@]}" \
      > "${TMP_OUT}" \
      2> "${TMP_ERR}"
    DOWNSTREAM_RC=$?
  else
    ONECLICK_SUPPRESS_NOTICE=1 bash "${DOWNSTREAM}" \
      > "${TMP_OUT}" \
      2> "${TMP_ERR}"
    DOWNSTREAM_RC=$?
  fi
fi
set -e

cat "${TMP_OUT}"
cat "${TMP_ERR}" >&2

RESULT_LINE="$(grep '^RESULT=' "${TMP_OUT}" | tail -n 1 || true)"
NEXT_LINE="$(grep '^NEXT: ' "${TMP_OUT}" | tail -n 1 || true)"
PR_READY_LINE="$(grep '^PR_READY=' "${TMP_OUT}" | tail -n 1 || true)"

RESULT_WORD="$(extract_result_field "${RESULT_LINE}" "RESULT")"
RUN_ID="$(extract_result_field "${RESULT_LINE}" "RUN_ID")"
ART_DIR="$(extract_result_field "${RESULT_LINE}" "ART_DIR")"
NEXT_CMD=""
if [[ -n "${NEXT_LINE}" ]]; then
  NEXT_CMD="${NEXT_LINE#NEXT: }"
else
  NEXT_CMD="$(extract_result_next "${RESULT_LINE}")"
fi

if [[ -z "${RESULT_WORD}" ]]; then
  if [[ "${DOWNSTREAM_RC}" == "0" ]]; then
    RESULT_WORD="PASS"
  else
    RESULT_WORD="FAIL"
  fi
fi
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ART_DIR="${ART_DIR:-<unknown>}"
NEXT_CMD="${NEXT_CMD:-NONE}"

DETAIL_RESULT_JSON="$(extract_key_value "${TMP_OUT}" 'RESULT_JSON: ')"
if [[ -z "${DETAIL_RESULT_JSON}" ]]; then
  DETAIL_RESULT_JSON="$(extract_key_value "${TMP_OUT}" 'DETAIL result.json=')"
fi
DETAIL_MANIFEST="$(extract_key_value "${TMP_OUT}" 'RUN_MANIFEST: ')"
if [[ -z "${DETAIL_MANIFEST}" ]]; then
  DETAIL_MANIFEST="$(extract_key_value "${TMP_OUT}" 'DETAIL manifest.json=')"
fi
DETAIL_TRACE="$(extract_key_value "${TMP_OUT}" 'TRACE: ')"
if [[ -z "${DETAIL_TRACE}" ]]; then
  DETAIL_TRACE="$(extract_key_value "${TMP_OUT}" 'DETAIL trace.jsonl=')"
fi

if [[ "${SUBCOMMAND}" == "pr-ready" && -n "${PR_READY_LINE}" ]]; then
  PR_READY_BOOL="$(echo "${PR_READY_LINE}" | sed -nE 's/.*PR_READY=([^ ]+).*/\1/p' | head -n 1)"
  PR_READY_RUN_ID="$(echo "${PR_READY_LINE}" | sed -nE 's/.* RUN_ID=([^ ]+).*/\1/p' | head -n 1)"
  if [[ -n "${PR_READY_RUN_ID}" ]]; then
    RUN_ID="${PR_READY_RUN_ID}"
  fi
  if [[ "${PR_READY_BOOL}" == "true" && "${DOWNSTREAM_RC}" == "0" ]]; then
    RESULT_WORD="PASS"
  else
    RESULT_WORD="FAIL"
  fi
fi

if [[ "${SUBCOMMAND}" == "pr-ready" && -n "${DETAIL_RESULT_JSON}" ]]; then
  ART_DIR="$(dirname "${DETAIL_RESULT_JSON}")"
elif [[ "${ART_DIR}" == "<unknown>" && -n "${DETAIL_RESULT_JSON}" ]]; then
  ART_DIR="$(dirname "${DETAIL_RESULT_JSON}")"
fi

echo "RESULT=${RESULT_WORD} RUN_ID=${RUN_ID} ART_DIR=${ART_DIR} NEXT=\"${NEXT_CMD}\""
echo "NEXT: ${NEXT_CMD}"
echo "DETAILS: result_json=${DETAIL_RESULT_JSON:-<none>} manifest=${DETAIL_MANIFEST:-<none>} trace=${DETAIL_TRACE:-<none>} downstream=${DOWNSTREAM}"

HAS_MARKER=0
if grep -q '^===ONECLICK_JSON_BEGIN===$' "${TMP_OUT}" && grep -q '^===ONECLICK_JSON_END===$' "${TMP_OUT}"; then
  HAS_MARKER=1
fi

if [[ "${HAS_MARKER}" == "0" ]]; then
  FALLBACK_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
  FALLBACK_DIR="${PROJECT_ROOT}/artifacts/oneclick/runs/${FALLBACK_RUN_ID}"
  FALLBACK_JSON="${FALLBACK_DIR}/result.json"
  mkdir -p "${FALLBACK_DIR}"
  cat > "${FALLBACK_JSON}" <<EOF
{
  "schema_version": "v1",
  "entry": "oneclick.sh",
  "run_id": $(json_quote "${RUN_ID}"),
  "subcommand": $(json_quote "${SUBCOMMAND}"),
  "downstream": $(json_quote "${DOWNSTREAM}"),
  "result": $(json_quote "${RESULT_WORD}"),
  "next": $(json_quote "${NEXT_CMD}"),
  "rc": ${DOWNSTREAM_RC},
  "art_dir": $(json_quote "${ART_DIR}"),
  "details": {
    "result_json": $(json_quote "${DETAIL_RESULT_JSON:-}"),
    "manifest": $(json_quote "${DETAIL_MANIFEST:-}"),
    "trace": $(json_quote "${DETAIL_TRACE:-}")
  }
}
EOF
  echo "===ONECLICK_JSON_BEGIN==="
  cat "${FALLBACK_JSON}"
  echo "===ONECLICK_JSON_END==="
fi

exit "${DOWNSTREAM_RC}"
