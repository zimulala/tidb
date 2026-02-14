#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
DEFAULT_RECIPES="${ROOT}/ai/ai-change-gates/recipes/verify_recipes.yaml"
RECIPE_ID=""
PROJECT_ROOT=""
OVERRIDES_FILE=""

usage() {
  cat <<'USAGE'
Usage:
  bash ai/ai-change-gates/tools/expand_verify_recipe.sh \
    --recipe-id <ID> \
    --project-root <repo_root> \
    [--overrides-file <path>]

Output:
  Prints concrete commands (one per line).
USAGE
}

die() {
  echo "[expand_verify_recipe][ERROR] $*" >&2
  exit 1
}

normalize_path() {
  local p="$1"
  if [[ -z "${p}" ]]; then
    echo ""
    return 0
  fi
  if [[ "${p}" == /* ]]; then
    echo "${p}"
  else
    echo "${ROOT}/${p}"
  fi
}

extract_commands_from_yaml() {
  local file="$1"
  local recipe_id="$2"
  awk -v rid="${recipe_id}" '
    BEGIN { in_recipes=0; in_item=0; target=0; in_commands=0 }
    /^recipes:[[:space:]]*$/ { in_recipes=1; next }
    in_recipes==1 {
      if ($0 ~ /^  - id:[[:space:]]*/) {
        in_item=1
        in_commands=0
        target=0
        idline=$0
        sub(/^  - id:[[:space:]]*/, "", idline)
        gsub(/"/, "", idline)
        if (idline == rid) target=1
        next
      }
      if (in_item==1 && target==1 && $0 ~ /^    commands:[[:space:]]*$/) {
        in_commands=1
        next
      }
      if (in_item==1 && in_commands==1) {
        if ($0 ~ /^      - /) {
          cmd=$0
          sub(/^      -[[:space:]]*/, "", cmd)
          gsub(/^"|"$/, "", cmd)
          print cmd
          next
        }
        if ($0 ~ /^    [A-Za-z0-9_]+:[[:space:]]*/) {
          in_commands=0
          next
        }
      }
    }
  ' "${file}" 2>/dev/null || true
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --recipe-id)
      [[ $# -ge 2 ]] || die "--recipe-id requires a value"
      RECIPE_ID="$2"
      shift 2
      ;;
    --project-root)
      [[ $# -ge 2 ]] || die "--project-root requires a value"
      PROJECT_ROOT="$2"
      shift 2
      ;;
    --overrides-file)
      [[ $# -ge 2 ]] || die "--overrides-file requires a value"
      OVERRIDES_FILE="$2"
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

[[ -n "${RECIPE_ID}" ]] || die "--recipe-id is required"
[[ -n "${PROJECT_ROOT}" ]] || die "--project-root is required"

PROJECT_ROOT="$(normalize_path "${PROJECT_ROOT}")"
[[ -d "${PROJECT_ROOT}" ]] || die "project root not found: ${PROJECT_ROOT}"
[[ -f "${DEFAULT_RECIPES}" ]] || die "default recipes file not found: ${DEFAULT_RECIPES}"

commands=""
if [[ -n "${OVERRIDES_FILE}" ]]; then
  OVERRIDES_FILE="$(normalize_path "${OVERRIDES_FILE}")"
  [[ -f "${OVERRIDES_FILE}" ]] || die "overrides file not found: ${OVERRIDES_FILE}"
  commands="$(extract_commands_from_yaml "${OVERRIDES_FILE}" "${RECIPE_ID}")"
fi

if [[ -z "${commands}" ]]; then
  commands="$(extract_commands_from_yaml "${DEFAULT_RECIPES}" "${RECIPE_ID}")"
fi

if [[ -z "${commands}" ]]; then
  echo "# TODO: recipe not found: ${RECIPE_ID}"
  echo "# Add recipe mapping to --overrides-file or ai/ai-change-gates/recipes/verify_recipes.yaml"
  exit 0
fi

while IFS= read -r cmd; do
  [[ -n "${cmd}" ]] || continue
  cmd="${cmd//\$REPO_ROOT/${PROJECT_ROOT}}"
  cmd="${cmd//<repo_root>/${PROJECT_ROOT}}"
  echo "${cmd}"
done <<< "${commands}"
