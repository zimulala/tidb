#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
JSON_OUT="0"
ASSERT_PROJECT="0"

ALLOWLIST_FILE="${HOME}/.config/ai-change-gates/trusted_roots"
ALLOWLIST_JSON="${HOME}/.config/ai-change-gates/trust.json"
TRUST_FILE=""
TRUST_TOKEN=""

PROJECT_ALLOWED="false"
REASON="default_global_only"

usage() {
  cat <<'EOF'
Usage:
  bash ai/ai-change-gates/tools/check_trust.sh [--json] [--assert-project] [--repo-root <path>]

Behavior:
  - Default policy: global skills only.
  - Project override is allowed only when trust file token passes local allowlist.
EOF
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

repo_hash() {
  printf '%s' "$1" | shasum -a 256 | awk '{print $1}'
}

contains_token() {
  # contains_token <token> <file>
  local token="$1"
  local file="$2"
  [[ -f "${file}" ]] || return 1

  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="$(trim "${line}")"
    [[ -n "${line}" ]] || continue
    [[ "${line}" =~ ^# ]] && continue
    if [[ "${line}" == "${token}" ]]; then
      return 0
    fi
  done < "${file}"
  return 1
}

json_has_token() {
  # json_has_token <token> <json-file>
  local token="$1"
  local json_file="$2"
  [[ -f "${json_file}" ]] || return 1
  command -v jq >/dev/null 2>&1 || return 1
  jq -e --arg t "${token}" '(.trusted_roots // []) + (.trusted_hashes // []) | index($t) != null' "${json_file}" >/dev/null
}

print_result() {
  local root_hash
  root_hash="$(repo_hash "${ROOT}")"
  if [[ "${JSON_OUT}" == "1" ]]; then
    cat <<EOF
{
  "repo_root": "${ROOT}",
  "repo_hash": "${root_hash}",
  "trust_file": "${TRUST_FILE}",
  "token": "${TRUST_TOKEN}",
  "project_skills_allowed": ${PROJECT_ALLOWED},
  "reason": "${REASON}",
  "allowlist": {
    "trusted_roots_file": "${ALLOWLIST_FILE}",
    "trust_json_file": "${ALLOWLIST_JSON}"
  }
}
EOF
  else
    echo "repo_root=${ROOT}"
    echo "repo_hash=${root_hash}"
    echo "trust_file=${TRUST_FILE:-<none>}"
    echo "token=${TRUST_TOKEN:-<none>}"
    echo "project_skills_allowed=${PROJECT_ALLOWED}"
    echo "reason=${REASON}"
    echo "allowlist_file=${ALLOWLIST_FILE}"
    echo "allowlist_json=${ALLOWLIST_JSON}"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --json)
      JSON_OUT="1"
      shift
      ;;
    --assert-project)
      ASSERT_PROJECT="1"
      shift
      ;;
    --repo-root)
      ROOT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[check_trust][FATAL] unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -f "${ROOT}/ai.trust" ]]; then
  TRUST_FILE="${ROOT}/ai.trust"
elif [[ -f "${ROOT}/.ai/trust" ]]; then
  TRUST_FILE="${ROOT}/.ai/trust"
fi

if [[ -z "${TRUST_FILE}" ]]; then
  PROJECT_ALLOWED="false"
  REASON="trust_file_missing"
  print_result
  if [[ "${ASSERT_PROJECT}" == "1" ]]; then
    exit 1
  fi
  exit 0
fi

TRUST_TOKEN="$(
  awk '
    /^[[:space:]]*#/ {next}
    /^[[:space:]]*$/ {next}
    {gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0); print; exit}
  ' "${TRUST_FILE}" 2>/dev/null || true
)"

if [[ -z "${TRUST_TOKEN}" ]]; then
  PROJECT_ALLOWED="false"
  REASON="empty_trust_token"
  print_result
  if [[ "${ASSERT_PROJECT}" == "1" ]]; then
    exit 1
  fi
  exit 0
fi

ROOT_HASH="$(repo_hash "${ROOT}")"
if [[ "${TRUST_TOKEN}" != "${ROOT}" && "${TRUST_TOKEN}" != "${ROOT_HASH}" ]]; then
  PROJECT_ALLOWED="false"
  REASON="token_not_repo_root_or_hash"
  print_result
  if [[ "${ASSERT_PROJECT}" == "1" ]]; then
    exit 1
  fi
  exit 0
fi

if contains_token "${TRUST_TOKEN}" "${ALLOWLIST_FILE}" || json_has_token "${TRUST_TOKEN}" "${ALLOWLIST_JSON}"; then
  PROJECT_ALLOWED="true"
  REASON="trusted_by_local_allowlist"
else
  PROJECT_ALLOWED="false"
  REASON="token_not_in_local_allowlist"
fi

print_result

if [[ "${ASSERT_PROJECT}" == "1" && "${PROJECT_ALLOWED}" != "true" ]]; then
  exit 1
fi
