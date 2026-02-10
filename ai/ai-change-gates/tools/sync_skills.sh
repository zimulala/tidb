#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
SRC_ROOT="${ROOT}/ai/skills_src"
CODEX_HOME_DIR="${CODEX_HOME:-${HOME}/.codex}"
CURSOR_HOME_DIR="${CURSOR_HOME:-${HOME}/.cursor}"
SYNC_CODEX="1"
SYNC_CURSOR="1"
CURSOR_OUT_DIR="${CURSOR_HOME_DIR}"

usage() {
  cat <<'EOF'
Usage:
  bash ai/ai-change-gates/tools/sync_skills.sh [--codex-home <path>] [--no-cursor] [--cursor-home <path>] [--cursor-out <path>]

Defaults:
  - Syncs to both Codex and Cursor.
  - Source of truth is ai/skills_src/*/{SPEC.yaml,BODY.md}.

Notes:
  - Do not edit ~/.codex/skills/... or ~/.cursor/skills/... manually.
  - Edit ai/skills_src/... and re-run this script.
EOF
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

read_spec_scalar() {
  # read_spec_scalar <spec-file> <key>
  local spec_file="$1"
  local key="$2"
  local line
  line="$(grep -E "^${key}:" "${spec_file}" | head -n 1 || true)"
  line="${line#${key}:}"
  line="$(trim "${line}")"
  line="${line%\"}"
  line="${line#\"}"
  printf '%s' "${line}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --codex-home)
      CODEX_HOME_DIR="$2"
      shift 2
      ;;
    --no-cursor)
      SYNC_CURSOR="0"
      shift
      ;;
    --cursor-home)
      CURSOR_HOME_DIR="$2"
      CURSOR_OUT_DIR="$2"
      shift 2
      ;;
    --cursor-out)
      CURSOR_OUT_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[sync_skills][FATAL] unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ ! -d "${SRC_ROOT}" ]]; then
  echo "[sync_skills][FATAL] source dir not found: ${SRC_ROOT}" >&2
  exit 1
fi

if [[ "${SYNC_CURSOR}" == "1" && -z "${CURSOR_OUT_DIR}" ]]; then
  echo "[sync_skills][FATAL] cursor output dir is empty" >&2
  exit 1
fi

generated=0

for skill_dir in "${SRC_ROOT}"/*; do
  [[ -d "${skill_dir}" ]] || continue
  spec_file="${skill_dir}/SPEC.yaml"
  body_file="${skill_dir}/BODY.md"
  [[ -f "${spec_file}" ]] || continue
  [[ -f "${body_file}" ]] || continue

  skill_name="$(read_spec_scalar "${spec_file}" "name")"
  skill_version="$(read_spec_scalar "${spec_file}" "version")"
  skill_description="$(read_spec_scalar "${spec_file}" "description")"
  triggers_line="$(grep -E '^triggers:' "${spec_file}" || true)"
  outputs_line="$(grep -E '^outputs:' "${spec_file}" || true)"
  safety_line="$(grep -E '^safety:' "${spec_file}" || true)"

  if [[ -z "${skill_name}" || -z "${skill_version}" || -z "${skill_description}" ]]; then
    echo "[sync_skills][FATAL] missing name/version/description in ${spec_file}" >&2
    exit 1
  fi
  if [[ -z "${triggers_line}" || -z "${outputs_line}" || -z "${safety_line}" ]]; then
    echo "[sync_skills][FATAL] SPEC.yaml missing required keys in ${spec_file}" >&2
    exit 1
  fi

  source_rel="ai/skills_src/$(basename "${skill_dir}")"
  generated_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  tmp_file="$(mktemp)"
  {
    echo "---"
    echo "name: ${skill_name}"
    echo "description: ${skill_description}"
    echo "version: ${skill_version}"
    echo "source: ${source_rel}"
    echo "generated_at_utc: ${generated_at}"
    echo "---"
    echo
    cat "${body_file}"
  } > "${tmp_file}"

  if [[ "${SYNC_CODEX}" == "1" ]]; then
    codex_target_dir="${CODEX_HOME_DIR}/skills/${skill_name}"
    mkdir -p "${codex_target_dir}"
    cp "${tmp_file}" "${codex_target_dir}/SKILL.md"
    echo "[sync_skills] codex -> ${codex_target_dir}/SKILL.md"
  fi

  if [[ "${SYNC_CURSOR}" == "1" ]]; then
    cursor_target_dir="${CURSOR_OUT_DIR}/skills/${skill_name}"
    mkdir -p "${cursor_target_dir}"
    cp "${tmp_file}" "${cursor_target_dir}/skill.md"
    echo "[sync_skills] cursor -> ${cursor_target_dir}/skill.md"
  else
    echo "[sync_skills] cursor sync disabled by --no-cursor."
  fi

  rm -f "${tmp_file}"
  generated=$((generated + 1))
done

if [[ "${generated}" == "0" ]]; then
  echo "[sync_skills][FATAL] no skills generated from ${SRC_ROOT}" >&2
  exit 1
fi

echo "[sync_skills] generated_skills=${generated}"
