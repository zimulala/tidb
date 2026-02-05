#!/usr/bin/env bash
set -euo pipefail

SPEC=""
GEN=""
PKG="pkg/util/topsql/reporter"
BASE=""
OUT=""
OUT_DIR=""
MAPPING=""
NAMING_SCRIPT="ai/projects/topru-ai/testgen/goal_naming.py"
RUN_PREFIX=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --spec) SPEC="$2"; shift 2;;
    --gen) GEN="$2"; shift 2;;
    --pkg) PKG="$2"; shift 2;;
    --base) BASE="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --out-dir) OUT_DIR="$2"; shift 2;;
    --mapping) MAPPING="$2"; shift 2;;
    *) echo "Unknown arg: $1" >&2; exit 2;;
  esac
done

[[ -n "$SPEC" && -f "$SPEC" ]] || { echo "[dedup_filter] ERROR: spec not found: $SPEC" >&2; exit 2; }
[[ -n "$OUT" ]] || { echo "[dedup_filter] ERROR: --out is required" >&2; exit 2; }
[[ -n "$OUT_DIR" ]] || { echo "[dedup_filter] ERROR: --out-dir is required" >&2; exit 2; }
[[ -n "$BASE" ]] || { echo "[dedup_filter] ERROR: --base is required" >&2; exit 2; }
[[ -f "$NAMING_SCRIPT" ]] || { echo "[dedup_filter] ERROR: naming script not found: $NAMING_SCRIPT" >&2; exit 2; }

mkdir -p "$OUT_DIR"

INVENTORY_LIST="${OUT_DIR}/inventory_tests_list.txt"
DEDUP_REPORT="${OUT_DIR}/dedup_report.txt"
KEEP_IDS="${OUT_DIR}/.keep_goal_ids.tmp"

extract_goals() {
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
  ' "$SPEC"
}

extract_run_prefix() {
  awk '
    /^go_test:[[:space:]]*$/ { in_go=1; next }
    in_go==1 && /^[^[:space:]]/ { in_go=0 }
    in_go==1 && /^[[:space:]]*run_prefix:[[:space:]]*/ {
      p=$0
      sub(/^[[:space:]]*run_prefix:[[:space:]]*/, "", p)
      gsub(/"/, "", p)
      sub(/[[:space:]]*$/, "", p)
      print p
      exit
    }
  ' "$SPEC"
}

extract_test_funcs_in_pkg() {
  local gen_base=""
  gen_base="$(basename "$GEN" 2>/dev/null || true)"
  if command -v rg >/dev/null 2>&1; then
    if [[ -n "$gen_base" ]]; then
      rg -n --no-heading --glob '*_test.go' --glob "!${gen_base}" \
        '^func[[:space:]]+(TestTopRU_[A-Za-z0-9_]+|TestTopRUReporter_[A-Za-z0-9_]+|TestTopRUGen[A-Za-z0-9_]+|TestTopRU_Gen_[A-Za-z0-9_]+)[[:space:]]*\(' \
        "$PKG" \
        | sed -E 's/^.*func[[:space:]]+((TestTopRU_|TestTopRUReporter_|TestTopRUGen|TestTopRU_Gen_)[A-Za-z0-9_]+)[[:space:]]*\(.*/\1/'
    else
      rg -n --no-heading --glob '*_test.go' \
        '^func[[:space:]]+(TestTopRU_[A-Za-z0-9_]+|TestTopRUReporter_[A-Za-z0-9_]+|TestTopRUGen[A-Za-z0-9_]+|TestTopRU_Gen_[A-Za-z0-9_]+)[[:space:]]*\(' \
        "$PKG" \
        | sed -E 's/^.*func[[:space:]]+((TestTopRU_|TestTopRUReporter_|TestTopRUGen|TestTopRU_Gen_)[A-Za-z0-9_]+)[[:space:]]*\(.*/\1/'
    fi
    return
  fi
  if [[ -n "$gen_base" ]]; then
    find "$PKG" -type f -name '*_test.go' ! -name "$gen_base" -print0 \
      | xargs -0 grep -hE '^func[[:space:]]+(TestTopRU_[A-Za-z0-9_]+|TestTopRUReporter_[A-Za-z0-9_]+|TestTopRUGen[A-Za-z0-9_]+|TestTopRU_Gen_[A-Za-z0-9_]+)[[:space:]]*\(' \
      | sed -E 's/^.*func[[:space:]]+((TestTopRU_|TestTopRUReporter_|TestTopRUGen|TestTopRU_Gen_)[A-Za-z0-9_]+)[[:space:]]*\(.*/\1/'
    return
  fi
  find "$PKG" -type f -name '*_test.go' -print0 \
    | xargs -0 grep -hE '^func[[:space:]]+(TestTopRU_[A-Za-z0-9_]+|TestTopRUReporter_[A-Za-z0-9_]+|TestTopRUGen[A-Za-z0-9_]+|TestTopRU_Gen_[A-Za-z0-9_]+)[[:space:]]*\(' \
    | sed -E 's/^.*func[[:space:]]+((TestTopRU_|TestTopRUReporter_|TestTopRUGen|TestTopRU_Gen_)[A-Za-z0-9_]+)[[:space:]]*\(.*/\1/'
}

extract_test_funcs_in_diff() {
  git diff --unified=0 "${BASE}..HEAD" -- "$PKG" ":(exclude)$GEN" 2>/dev/null \
    | awk '
      /^[+-]func[[:space:]]+(TestTopRU_|TestTopRUReporter_|TestTopRUGen|TestTopRU_Gen_)[A-Za-z0-9_]+[[:space:]]*\(/ {
        line=$0
        sub(/^[+-]func[[:space:]]+/, "", line)
        sub(/[[:space:]]*\(.*/, "", line)
        if (line != "") print line
      }
    ' || true
}

GOALS="$(extract_goals | sed '/^$/d')"
RUN_PREFIX="$(extract_run_prefix)"
if [[ -z "$RUN_PREFIX" ]]; then
  RUN_PREFIX="TestTopRUGen"
fi
PKG_TESTS="$(extract_test_funcs_in_pkg | sed '/^$/d' || true)"
DIFF_TESTS="$(extract_test_funcs_in_diff | sed '/^$/d' || true)"
ALL_TESTS="$(
  {
    echo "$PKG_TESTS"
    echo "$DIFF_TESTS"
  } | sed '/^$/d' | sort -u
)"

printf "%s\n" "$ALL_TESTS" > "$INVENTORY_LIST"

# Optional explicit mapping file for hand-written test coverage.
# Format per line: <GoalID><space><TestFuncName>
# Example: G1_send_nonempty TestTopRUReporter_MockDataSinkStructured
mapping_cover_for_goal() {
  local goal_id="$1"
  if [[ -z "$MAPPING" || ! -f "$MAPPING" ]]; then
    echo ""
    return 0
  fi
  awk -v gid="$goal_id" '
    /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
    $1 == gid { print $2; exit }
  ' "$MAPPING"
}

KEEP=()
SKIP=()
SKIP_META="${OUT_DIR}/.skip_meta.tmp"
: > "$SKIP_META"

while IFS= read -r gid; do
  [[ -n "$gid" ]] || continue
  gen_test="$(python3 "$NAMING_SCRIPT" --prefix "$RUN_PREFIX" --goal "$gid")"
  if grep -Fxq "$gen_test" "$INVENTORY_LIST"; then
    SKIP+=("$gid")
    echo "${gid}|already_generated|${gen_test}" >> "$SKIP_META"
    continue
  fi

  mapped="$(mapping_cover_for_goal "$gid")"
  if [[ -n "$mapped" ]] && grep -Fxq "$mapped" "$INVENTORY_LIST"; then
    SKIP+=("$gid")
    echo "${gid}|covered_by_mapping|${mapped}" >> "$SKIP_META"
    continue
  fi

  KEEP+=("$gid")
done <<< "$GOALS"

{
  for gid in "${KEEP[@]-}"; do
    [[ -n "$gid" ]] || continue
    echo "$gid"
  done
} > "$KEEP_IDS"

# Keep header and only selected goals.
awk -v keep_file="$KEEP_IDS" '
BEGIN {
  while ((getline line < keep_file) > 0) {
    keep[line] = 1
  }
  close(keep_file)
  in_goals = 0
  in_block = 0
  block = ""
  goal_id = ""
}
{
  if (!in_goals) {
    print
    if ($0 ~ /^goals:[[:space:]]*$/) {
      in_goals = 1
    }
    next
  }

  if ($0 ~ /^  - id:[[:space:]]*/) {
    if (in_block && keep[goal_id]) {
      printf "%s", block
    }
    in_block = 1
    block = $0 ORS
    goal_id = $0
    sub(/^  - id:[[:space:]]*/, "", goal_id)
    sub(/[[:space:]]*#.*/, "", goal_id)
    sub(/[[:space:]]*$/, "", goal_id)
    next
  }

  if (in_block) {
    if ($0 ~ /^    / || $0 ~ /^[[:space:]]*$/) {
      block = block $0 ORS
      next
    }
    if (keep[goal_id]) {
      printf "%s", block
    }
    in_block = 0
    block = ""
    goal_id = ""
    print
    next
  }

  print
}
END {
  if (in_block && keep[goal_id]) {
    printf "%s", block
  }
}
' "$SPEC" > "$OUT"

goals_total="$(echo "$GOALS" | sed '/^$/d' | wc -l | tr -d ' ')"
goals_kept="$(awk 'NF{n++} END{print n+0}' "$KEEP_IDS")"
goals_skipped="$(awk -F'|' 'NF{n++} END{print n+0}' "$SKIP_META")"

{
  echo "BASE_REV=${BASE}"
  echo "mapping_file=${MAPPING:-<none>}"
  echo "generated_file_excluded_from_dedup=${GEN:-<none>}"
  echo "run_prefix=${RUN_PREFIX}"
  echo "goals_scope=auto:true"
  echo "goals_total=${goals_total}"
  echo "goals_kept=${goals_kept}"
  echo "goals_skipped=${goals_skipped}"
  echo
  if [[ "${goals_skipped}" -gt 0 ]]; then
    echo "[skipped]"
    awk -F'|' '{printf "%s reason=%s covered_by=%s\n", $1, $2, $3}' "$SKIP_META"
  else
    echo "[skipped]"
    echo "(none)"
  fi
  echo
  echo "[kept]"
  if [[ "${goals_kept}" -gt 0 ]]; then
    for gid in "${KEEP[@]-}"; do
      [[ -n "$gid" ]] || continue
      echo "$gid"
    done
  else
    echo "(none)"
  fi
} > "$DEDUP_REPORT"

rm -f "$KEEP_IDS" "$SKIP_META"

exit 0
