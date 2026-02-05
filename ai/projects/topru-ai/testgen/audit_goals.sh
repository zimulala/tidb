#!/usr/bin/env bash
set -euo pipefail

SPEC="ai/projects/topru-ai/testgen/specs/topru_coverage_goals.yml"
GEN="pkg/util/topsql/reporter/topru_generated_cases_test.go"
PREFIX="TestTopRUGen"
STRICT=0
ALLOW_EXTRA=1
OUT=""
NAMING_SCRIPT="ai/projects/topru-ai/testgen/goal_naming.py"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --spec) SPEC="$2"; shift 2;;
    --gen) GEN="$2"; shift 2;;
    --prefix) PREFIX="$2"; shift 2;;
    --strict) STRICT=1; shift;;
    --no-extra) ALLOW_EXTRA=0; shift;;
    --out) OUT="$2"; shift 2;;
    *) echo "Unknown arg: $1" >&2; exit 2;;
  esac
done

[[ -f "$SPEC" ]] || { echo "[audit_goals] ERROR: spec not found: $SPEC" >&2; exit 2; }
[[ -f "$GEN" ]] || { echo "[audit_goals] ERROR: generated file not found: $GEN" >&2; exit 2; }
[[ -f "$NAMING_SCRIPT" ]] || { echo "[audit_goals] ERROR: naming script not found: $NAMING_SCRIPT" >&2; exit 2; }

GOALS="$(
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
  ' "$SPEC" | sed '/^$/d'
)"

TESTS="$(
  if command -v rg >/dev/null 2>&1; then
    rg -n --no-heading "^func[[:space:]]+${PREFIX}[A-Za-z0-9_]+[[:space:]]*\\(" "$GEN" \
      | sed -E "s/^.*func[[:space:]]+(${PREFIX}[A-Za-z0-9_]+)[[:space:]]*\\(.*/\\1/"
  else
    grep -E "^func[[:space:]]+${PREFIX}[A-Za-z0-9_]+[[:space:]]*\\(" "$GEN" \
      | sed -E "s/^.*func[[:space:]]+(${PREFIX}[A-Za-z0-9_]+)[[:space:]]*\\(.*/\\1/"
  fi \
  | sed '/^$/d' \
  || true
)"

expected_tests="$(
  if [[ -n "$GOALS" ]]; then
    echo "$GOALS" \
      | python3 "$NAMING_SCRIPT" --prefix "$PREFIX" --goals-stdin \
      | awk -F'\t' '{print $2}'
  fi
)"

dup_goals="$(echo "$GOALS" | sed '/^$/d' | sort | uniq -d || true)"
dup_tests="$(echo "$TESTS" | sed '/^$/d' | sort | uniq -d || true)"
missing_tests="$(
  comm -23 \
    <(echo "$expected_tests" | sed '/^$/d' | sort) \
    <(echo "$TESTS" | sed '/^$/d' | sort) \
    || true
)"
extra_tests="$(
  comm -13 \
    <(echo "$expected_tests" | sed '/^$/d' | sort) \
    <(echo "$TESTS" | sed '/^$/d' | sort) \
    || true
)"

render_coverage_map() {
  local goal_count test_count
  goal_count="$(echo "$GOALS" | sed '/^$/d' | wc -l | tr -d ' ')"
  test_count="$(echo "$TESTS" | sed '/^$/d' | wc -l | tr -d ' ')"

  echo "[audit_goals] spec: $SPEC"
  echo "[audit_goals] gen: $GEN"
  echo "[audit_goals] prefix: $PREFIX"
  echo "[audit_goals] goals: ${goal_count}"
  echo "[audit_goals] tests: ${test_count}"
  echo
  echo "[mapping]"
  if [[ -n "$GOALS" ]]; then
    while IFS= read -r gid; do
      [[ -n "$gid" ]] || continue
      tname="$(python3 "$NAMING_SCRIPT" --prefix "$PREFIX" --goal "$gid")"
      if echo "$TESTS" | grep -qx "$tname"; then
        echo "${gid} -> ${tname} [present]"
      else
        echo "${gid} -> ${tname} [missing]"
      fi
    done <<< "$GOALS"
  else
    echo "(no goals)"
  fi
  echo
  echo "[duplicates.goals]"
  if [[ -n "$dup_goals" ]]; then echo "$dup_goals"; else echo "(none)"; fi
  echo
  echo "[duplicates.tests]"
  if [[ -n "$dup_tests" ]]; then echo "$dup_tests"; else echo "(none)"; fi
  echo
  echo "[missing]"
  if [[ -n "$missing_tests" ]]; then echo "$missing_tests"; else echo "(none)"; fi
  echo
  echo "[extra]"
  if [[ -n "$extra_tests" ]]; then echo "$extra_tests"; else echo "(none)"; fi
}

if [[ -n "$OUT" ]]; then
  render_coverage_map > "$OUT"
  cat "$OUT"
else
  render_coverage_map
fi

rc=0
if [[ -n "$dup_goals" || -n "$dup_tests" || -n "$missing_tests" ]]; then
  rc=1
fi
if [[ "$ALLOW_EXTRA" == "0" && -n "$extra_tests" ]]; then
  rc=1
fi

if [[ "$STRICT" == "1" ]]; then
  exit "$rc"
fi
exit 0
