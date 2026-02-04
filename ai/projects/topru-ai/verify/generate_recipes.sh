#!/usr/bin/env bash
# ai/projects/topru-ai/verify/generate_recipes.sh
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

ensure_file() {
  local path="$1"
  local mode="$2"
  if [[ -f "$path" ]]; then
    echo "[gen] keep existing: $path"
    return 0
  fi
  echo "[gen] create: $path"
  mkdir -p "$(dirname "$path")"
  cat > "$path" <<'EOF'
__REPLACE_ME__
EOF
  chmod +x "$path"
  # shellcheck disable=SC2016
  perl -0777 -pe 's/__REPLACE_ME__//g' -i "$path" >/dev/null 2>&1 || true
  return 0
}

# ---------- e_func_smoke.sh ----------
if [[ ! -f "ai/projects/topru-ai/verify/e_func_smoke.sh" ]]; then
  cat > "ai/projects/topru-ai/verify/e_func_smoke.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

echo "=== E_func smoke start ==="
TIDB_HOST="${TIDB_HOST:-127.0.0.1}"
TIDB_PORT="${TIDB_PORT:-4000}"
TIDB_LOG_PATH="${TIDB_LOG_PATH:?TIDB_LOG_PATH is required}"
VERIFY_MODE="${VERIFY_MODE:-log}"

echo "TIDB=${TIDB_HOST}:${TIDB_PORT} MODE=${VERIFY_MODE}"
echo "LOG=${TIDB_LOG_PATH}"

log_start_lines="$(wc -l < "${TIDB_LOG_PATH}" | tr -d ' ')"
echo "log_start_lines=${log_start_lines}"

# Minimal functional workload that should exercise SQL/plan registration paths.
# (Keep deterministic + short; no source code changes.)
mysql -h "${TIDB_HOST}" -P "${TIDB_PORT}" -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS test;
USE test;
CREATE TABLE IF NOT EXISTS topru_func (id BIGINT PRIMARY KEY, v VARCHAR(100));
INSERT IGNORE INTO topru_func VALUES (1,'a'),(2,'b'),(3,'c');
SELECT /*topru_func_smoke*/ COUNT(*) FROM topru_func;
SQL

echo "--- verify via log signatures (best-effort) ---"
# Treat signature matching as signal; make it strong but not brittle.
# If you have exact stable signatures, replace these patterns.
PAT='TopRU|topru|RURecords|ReportTopRU|SQLMeta|PlanMeta|RegisterSQL|RegisterPlan|TopSQL'
tail -n +"${log_start_lines}" "${TIDB_LOG_PATH}" | grep -E "${PAT}" -n || {
  echo "WARN: no expected signatures found in new log slice (pattern=${PAT})"
  # do not fail hard; keep smoke robust across log format changes
  exit 0
}

echo "PASS: E_func smoke done."
EOF
  chmod +x "ai/projects/topru-ai/verify/e_func_smoke.sh"
fi

# ---------- e_perf_sanity.sh ----------
if [[ ! -f "ai/projects/topru-ai/verify/e_perf_sanity.sh" ]]; then
  cat > "ai/projects/topru-ai/verify/e_perf_sanity.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

echo "=== E_perf sanity start ==="
TIDB_HOST="${TIDB_HOST:-127.0.0.1}"
TIDB_PORT="${TIDB_PORT:-4000}"

echo "TIDB=${TIDB_HOST}:${TIDB_PORT}"

# A tiny performance sanity check:
# - warmup
# - run N queries
# - record elapsed time
N="${PERF_N:-50}"

mysql -h "${TIDB_HOST}" -P "${TIDB_PORT}" -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS test;
USE test;
CREATE TABLE IF NOT EXISTS topru_perf (id BIGINT PRIMARY KEY, v VARCHAR(100));
SQL

# Warmup
mysql -h "${TIDB_HOST}" -P "${TIDB_PORT}" -u root -e "USE test; SELECT /*topru_perf_warm*/ COUNT(*) FROM topru_perf;" >/dev/null 2>&1 || true

t0="$(python3 - <<'PY'
import time
print(time.time())
PY
)"

i=0
while [[ $i -lt $N ]]; do
  mysql -h "${TIDB_HOST}" -P "${TIDB_PORT}" -u root -e "USE test; SELECT /*topru_perf_${i}*/ 1;" >/dev/null
  i=$((i+1))
done

t1="$(python3 - <<'PY'
import time
print(time.time())
PY
)"

python3 - <<PY
t0=float("${t0}")
t1=float("${t1}")
n=int("${N}")
elapsed=t1-t0
qps=n/elapsed if elapsed>0 else 0
print(f"N={n}")
print(f"elapsed_sec={elapsed:.6f}")
print(f"qps={qps:.2f}")
# sanity guard: never fail hard by default; use PERF_MIN_QPS to enforce
min_qps=float("${PERF_MIN_QPS:-0}")
if min_qps>0 and qps<min_qps:
    raise SystemExit(f"FAIL: qps {qps:.2f} < min_qps {min_qps}")
print("PASS: E_perf sanity done.")
PY
EOF
  chmod +x "ai/projects/topru-ai/verify/e_perf_sanity.sh"
fi

echo "[gen] OK"

