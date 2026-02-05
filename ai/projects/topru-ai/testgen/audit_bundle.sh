#!/usr/bin/env bash
set -euo pipefail

BASE=""
SPEC=""
ORIG_SPEC=""
GEN=""
PKG="pkg/util/topsql/reporter"
OUT_DIR=""
PREFIX="TestTopRUGen"
GO_TEST_CMD=""
ONECLICK_CMD=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base) BASE="$2"; shift 2;;
    --spec) SPEC="$2"; shift 2;;
    --orig-spec) ORIG_SPEC="$2"; shift 2;;
    --gen) GEN="$2"; shift 2;;
    --pkg) PKG="$2"; shift 2;;
    --out-dir) OUT_DIR="$2"; shift 2;;
    --prefix) PREFIX="$2"; shift 2;;
    --go-test-cmd) GO_TEST_CMD="$2"; shift 2;;
    --oneclick-cmd) ONECLICK_CMD="$2"; shift 2;;
    *) echo "Unknown arg: $1" >&2; exit 2;;
  esac
done

[[ -n "$BASE" ]] || { echo "[audit_bundle] ERROR: --base is required" >&2; exit 2; }
[[ -n "$SPEC" && -f "$SPEC" ]] || { echo "[audit_bundle] ERROR: spec not found: $SPEC" >&2; exit 2; }
[[ -n "$GEN" && -f "$GEN" ]] || { echo "[audit_bundle] ERROR: generated file not found: $GEN" >&2; exit 2; }
[[ -n "$OUT_DIR" ]] || { echo "[audit_bundle] ERROR: --out-dir is required" >&2; exit 2; }

mkdir -p "$OUT_DIR"

PR_DIFF="${OUT_DIR}/pr_diff.txt"
TESTS_LIST="${OUT_DIR}/tests_list.txt"
GOALS_LIST="${OUT_DIR}/goals_list.txt"
COVERAGE_MAP="${OUT_DIR}/coverage_map.txt"
SUMMARY="${OUT_DIR}/summary.md"
DEDUP_REPORT="${OUT_DIR}/dedup_report.txt"
INVENTORY_LIST="${OUT_DIR}/inventory_tests_list.txt"
FILTERED_SPEC="${OUT_DIR}/filtered_spec.yml"
ALL_GOALS_SRC="${ORIG_SPEC:-$SPEC}"
AUTO_GOALS_ALL="${OUT_DIR}/.auto_goals_all.tmp"
NONAUTO_GOALS_ALL="${OUT_DIR}/.nonauto_goals_all.tmp"

: > "$TESTS_LIST"
: > "$AUTO_GOALS_ALL"
: > "$NONAUTO_GOALS_ALL"

if command -v rg >/dev/null 2>&1; then
  rg -n --no-heading "^func[[:space:]]+${PREFIX}[A-Za-z0-9_]+[[:space:]]*\\(" "$GEN" \
    | sed -E "s/^.*func[[:space:]]+(${PREFIX}[A-Za-z0-9_]+)[[:space:]]*\\(.*/\\1/" \
    | sed '/^$/d' \
    | sort -u > "$TESTS_LIST" || true
else
  grep -E "^func[[:space:]]+${PREFIX}[A-Za-z0-9_]+[[:space:]]*\\(" "$GEN" \
    | sed -E "s/^.*func[[:space:]]+(${PREFIX}[A-Za-z0-9_]+)[[:space:]]*\\(.*/\\1/" \
    | sed '/^$/d' \
    | sort -u > "$TESTS_LIST" || true
fi

awk '
  /^goals:[[:space:]]*$/ { in_goals=1; next }
  in_goals==1 && /^[^[:space:]]/ {
    if (cur_id != "") {
      if (auto_flag == "" || auto_flag == "true") print cur_id
    }
    exit
  }
  in_goals==0 { next }

  /^  - id:[[:space:]]*/ {
    if (cur_id != "") {
      if (auto_flag == "" || auto_flag == "true") print cur_id
    }
    cur_id=$0
    sub(/^  - id:[[:space:]]*/, "", cur_id)
    sub(/[[:space:]]*#.*/, "", cur_id)
    sub(/[[:space:]]*$/, "", cur_id)
    auto_flag=""
    next
  }

  /^    auto:[[:space:]]*/ {
    v=$0
    sub(/^    auto:[[:space:]]*/, "", v)
    gsub(/"/, "", v)
    sub(/[[:space:]]*#.*/, "", v)
    sub(/[[:space:]]*$/, "", v)
    auto_flag=v
    next
  }

  END {
    if (cur_id != "") {
      if (auto_flag == "" || auto_flag == "true") print cur_id
    }
  }
' "$SPEC" | sort -u > "$GOALS_LIST"

bash ai/projects/topru-ai/testgen/audit_goals.sh \
  --spec "$SPEC" \
  --gen "$GEN" \
  --prefix "$PREFIX" \
  --out "$COVERAGE_MAP" >/dev/null

changed_paths="$(
  git diff --name-only "${BASE}..HEAD" -- ai/projects/topru-ai/testgen "$PKG" \
    | awk '
      /^ai\/projects\/topru-ai\/testgen\// {print; next}
      /^pkg\/util\/topsql\/reporter\/.*_test\.go$/ {print; next}
    ' \
    | sort -u
)"

{
  echo "# base_rev"
  echo "$BASE"
  echo
  echo "# git status --porcelain"
  git status --porcelain || true
  echo
  echo "# git diff --name-status ${BASE}..HEAD (limited paths)"
  if [[ -n "$changed_paths" ]]; then
    while IFS= read -r f; do
      [[ -n "$f" ]] || continue
      git diff --name-status "${BASE}..HEAD" -- "$f"
    done <<< "$changed_paths"
  else
    echo "(no changed files in limited path set)"
  fi
  echo
  echo "# git diff ${BASE}..HEAD (limited paths, full patch)"
  if [[ -n "$changed_paths" ]]; then
    while IFS= read -r f; do
      [[ -n "$f" ]] || continue
      echo "## ${f}"
      git diff "${BASE}..HEAD" -- "$f"
      echo
    done <<< "$changed_paths"
  else
    echo "(no patch in limited path set)"
  fi
} > "$PR_DIFF"

total_goals="$(awk 'NF>0{n++} END{print n+0}' "$GOALS_LIST")"
total_tests="$(awk 'NF>0{n++} END{print n+0}' "$TESTS_LIST")"
dedup_total="$(awk -F= '/^goals_total=/{print $2}' "$DEDUP_REPORT" 2>/dev/null || true)"
dedup_kept="$(awk -F= '/^goals_kept=/{print $2}' "$DEDUP_REPORT" 2>/dev/null || true)"
dedup_skipped="$(awk -F= '/^goals_skipped=/{print $2}' "$DEDUP_REPORT" 2>/dev/null || true)"

awk '
  /^goals:[[:space:]]*$/ { in_goals=1; next }
  in_goals==1 && /^[^[:space:]]/ {
    if (cur_id != "") {
      if (auto_flag == "false") print cur_id > nonauto_out
      else print cur_id > auto_out
    }
    exit
  }
  in_goals==0 { next }

  /^  - id:[[:space:]]*/ {
    if (cur_id != "") {
      if (auto_flag == "false") print cur_id > nonauto_out
      else print cur_id > auto_out
    }
    cur_id=$0
    sub(/^  - id:[[:space:]]*/, "", cur_id)
    sub(/[[:space:]]*#.*/, "", cur_id)
    sub(/[[:space:]]*$/, "", cur_id)
    auto_flag=""
    next
  }

  /^    auto:[[:space:]]*/ {
    v=$0
    sub(/^    auto:[[:space:]]*/, "", v)
    gsub(/"/, "", v)
    sub(/[[:space:]]*#.*/, "", v)
    sub(/[[:space:]]*$/, "", v)
    auto_flag=v
    next
  }

  END {
    if (cur_id != "") {
      if (auto_flag == "false") print cur_id > nonauto_out
      else print cur_id > auto_out
    }
  }
' auto_out="$AUTO_GOALS_ALL" nonauto_out="$NONAUTO_GOALS_ALL" "$ALL_GOALS_SRC"

auto_all_count="$(awk 'NF{n++} END{print n+0}' "$AUTO_GOALS_ALL")"
nonauto_all_count="$(awk 'NF{n++} END{print n+0}' "$NONAUTO_GOALS_ALL")"

{
  echo "# E_testgen_topru Summary"
  echo
  echo "- base_rev: \`${BASE}\`"
  echo "- filtered_spec: \`${SPEC}\`"
  if [[ -n "$ORIG_SPEC" ]]; then
    echo "- original_spec: \`${ORIG_SPEC}\`"
  fi
  echo "- generated_file: \`${GEN}\`"
  echo "- goals_in_filtered_spec: ${total_goals}"
  echo "- generated_tests_with_prefix: ${total_tests}"
  if [[ -n "$dedup_total" || -n "$dedup_kept" || -n "$dedup_skipped" ]]; then
    echo "- dedup_stats: total=${dedup_total:-NA}, kept=${dedup_kept:-NA}, skipped=${dedup_skipped:-NA}"
  fi
  echo
  echo "## Auto goals vs Non-auto goals"
  echo "- auto goals (unit testgen scope): ${auto_all_count}"
  echo "- non-auto goals (evidence scope): ${nonauto_all_count}"
  echo "- this run generated tests: ${total_tests}"
  echo "- non-auto goals are expected to be covered by integration/perf/compat evidence (e.g. \`E_integ\`, \`E_perf\`, \`E_compat\`)."
  echo
  echo "### Non-auto Goal IDs"
  if [[ -s "$NONAUTO_GOALS_ALL" ]]; then
    while IFS= read -r g; do
      [[ -n "$g" ]] || continue
      echo "- \`${g}\`"
    done < "$NONAUTO_GOALS_ALL"
  else
    echo "- (none)"
  fi
  echo
  echo "## Key Artifacts"
  echo "- \`${OUT_DIR}/run.log\`"
  echo "- \`${SUMMARY}\`"
  echo "- \`${PR_DIFF}\`"
  echo "- \`${TESTS_LIST}\`"
  echo "- \`${GOALS_LIST}\`"
  echo "- \`${COVERAGE_MAP}\`"
  echo "- \`${DEDUP_REPORT}\`"
  echo "- \`${INVENTORY_LIST}\`"
  echo "- \`${FILTERED_SPEC}\`"
  echo
  echo "## Auto Goals"
  if [[ -s "$GOALS_LIST" ]]; then
    while IFS= read -r g; do
      [[ -n "$g" ]] || continue
      echo "- \`${g}\`"
    done < "$GOALS_LIST"
  else
    echo "- (none)"
  fi
  echo
  echo "## Generated Tests"
  if [[ -s "$TESTS_LIST" ]]; then
    while IFS= read -r t; do
      [[ -n "$t" ]] || continue
      echo "- \`${t}\`"
    done < "$TESTS_LIST"
  else
    echo "- (none)"
  fi
  echo
  echo "## Reproduce"
  echo "1. \`python3 ai/projects/topru-ai/testgen/generate_topru_cases.py --spec ${SPEC} --out ${GEN}\`"
  if [[ -n "$GO_TEST_CMD" ]]; then
    echo "2. \`${GO_TEST_CMD}\`"
  else
    echo "2. \`go test ./pkg/util/topsql/reporter -run '${PREFIX}' -count=1 -timeout 5m\`"
  fi
  if [[ -n "$ONECLICK_CMD" ]]; then
    echo "3. \`${ONECLICK_CMD}\`"
  else
    echo "3. \`BASE_REV=${BASE} bash ai/projects/topru-ai/testgen/run_testgen_oneclick.sh\`"
  fi
} > "$SUMMARY"

rm -f "$AUTO_GOALS_ALL" "$NONAUTO_GOALS_ALL"
