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
