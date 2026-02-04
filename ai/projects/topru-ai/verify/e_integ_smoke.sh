#!/usr/bin/env bash
set -euo pipefail

# E_integ smoke: minimal proof the TopRU path is exercised.
#
# This is NOT a correctness proof for all metrics.
# Minimal gate for "smoke" evidence:
#  - you can trigger a workload (SQL loop)
#  - the server walks through TopProfilingEnabled-related paths (indirectly evidenced by log signals)
#  - you can see TopRU OR SQL/Plan meta registration signatures in TiDB logs
#  - evidence is commit-bound (manifest.json)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"     # .../ai/projects/topru-ai
REPO_DIR="$(cd "${PROJECT_DIR}/../../.." && pwd)" # repo root

# =========================
# Minimal config (edit these)
# =========================
TIDB_LOG_PATH="${TIDB_LOG_PATH:-/Users/xia/workspace/src/github.com/pingcap/tidb/tidb.log}" # REQUIRED
SQL_RU="${SQL_RU:-SELECT /*topru_smoke*/ COUNT(*) FROM topru_smoke t1 JOIN topru_smoke t2 JOIN topru_smoke t3;}"
VERIFY_MODE="${VERIFY_MODE:-log}" # log only in this minimal version

# =========================
# Defaults (usually no need to edit)
# =========================
TIDB_HOST="${TIDB_HOST:-127.0.0.1}"
TIDB_PORT="${TIDB_PORT:-4000}"
TIDB_USER="${TIDB_USER:-root}"
TIDB_PASS="${TIDB_PASS:-}"
TIDB_DB="${TIDB_DB:-test}"

SQL_SETUP="${SQL_SETUP:-CREATE TABLE IF NOT EXISTS topru_smoke (id BIGINT PRIMARY KEY, v VARCHAR(100));}"
SQL_LOAD="${SQL_LOAD:-INSERT IGNORE INTO topru_smoke VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');}"

LONG_RUN_SECONDS="${LONG_RUN_SECONDS:-2}"
LOG_GREP_TOPRU="${LOG_GREP_TOPRU:-TopRU|topru|RURecords|ReportTopRU}"
LOG_GREP_META="${LOG_GREP_META:-SQLMeta|PlanMeta|RegisterSQL|RegisterPlan}"

EID="E_integ"
ROOT="${PROJECT_DIR}/artifacts/evidence/${EID}"
RUN_LOG="${ROOT}/run.log"
MANIFEST="${ROOT}/manifest.json"
mkdir -p "${ROOT}"

mysql_cmd() {
  local sql="$1"
  mysql -h "${TIDB_HOST}" -P "${TIDB_PORT}" -u "${TIDB_USER}" ${TIDB_PASS:+-p"${TIDB_PASS}"} "${TIDB_DB}" -e "${sql}"
}

echo "=== E_integ smoke start ===" | tee "${RUN_LOG}"
echo "TIDB=${TIDB_HOST}:${TIDB_PORT} DB=${TIDB_DB} MODE=${VERIFY_MODE}" | tee -a "${RUN_LOG}"
echo "LOG=${TIDB_LOG_PATH}" | tee -a "${RUN_LOG}"
echo "SQL_RU=${SQL_RU}" | tee -a "${RUN_LOG}"

if [[ "${VERIFY_MODE}" != "log" ]]; then
  echo "ERROR: minimal script supports VERIFY_MODE=log only" | tee -a "${RUN_LOG}"
  exit 2
fi
if ! command -v mysql >/dev/null 2>&1; then
  echo "ERROR: mysql client not found in PATH" | tee -a "${RUN_LOG}"
  exit 2
fi
if [[ -z "${TIDB_LOG_PATH}" || ! -f "${TIDB_LOG_PATH}" ]]; then
  echo "ERROR: TIDB_LOG_PATH not set or not found: ${TIDB_LOG_PATH}" | tee -a "${RUN_LOG}"
  exit 2
fi

# Only search new log lines (best-effort) to avoid matching historical runs.
START_LINES="$(wc -l <"${TIDB_LOG_PATH}" | tr -d ' ')"
echo "log_start_lines=${START_LINES}" | tee -a "${RUN_LOG}"

echo "--- setup/load ---" | tee -a "${RUN_LOG}"
mysql_cmd "${SQL_SETUP}" 2>&1 | tee -a "${RUN_LOG}"
mysql_cmd "${SQL_LOAD}" 2>&1 | tee -a "${RUN_LOG}"

echo "--- trigger RU query ---" | tee -a "${RUN_LOG}"
mysql_cmd "${SQL_RU}" 2>&1 | tee -a "${RUN_LOG}"

# Attempt a "long-running" behavior without sleep-based flake:
# We loop RU query for LONG_RUN_SECONDS seconds.
echo "--- trigger long-ish workload (${LONG_RUN_SECONDS}s) ---" | tee -a "${RUN_LOG}"
end=$(( $(date +%s) + LONG_RUN_SECONDS ))
while [[ $(date +%s) -lt $end ]]; do
  mysql_cmd "${SQL_RU}" >/dev/null 2>&1 || true
done

echo "--- verify via log signatures ---" | tee -a "${RUN_LOG}"
echo "grep TopRU signature: ${LOG_GREP_TOPRU}" | tee -a "${RUN_LOG}"
tail -n +"$((START_LINES+1))" "${TIDB_LOG_PATH}" | grep -E "${LOG_GREP_TOPRU}" | tail -n 50 | tee -a "${RUN_LOG}" || true

echo "grep Meta signature: ${LOG_GREP_META}" | tee -a "${RUN_LOG}"
tail -n +"$((START_LINES+1))" "${TIDB_LOG_PATH}" | grep -E "${LOG_GREP_META}" | tail -n 50 | tee -a "${RUN_LOG}" || true

# Minimal pass condition: at least one match for TopRU OR meta since this run started.
if ! tail -n +"$((START_LINES+1))" "${TIDB_LOG_PATH}" | grep -Eq "${LOG_GREP_TOPRU}|${LOG_GREP_META}"; then
  echo "FAIL: no TopRU/meta signatures found in new log lines (adjust LOG_GREP_* patterns)" | tee -a "${RUN_LOG}"
  exit 3
fi

# Manifest (commit-bound). This binds the evidence to the current workspace state.
cat > "${MANIFEST}" <<EOF
{
  "evidence_id": "${EID}",
  "status": "Captured",
  "type": "integration_smoke",
  "commit": "$(git -C "${REPO_DIR}" rev-parse HEAD)",
  "patch_id": "$(git -C "${REPO_DIR}" show HEAD | git patch-id --stable | awk '{print $1}')",
  "time": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "env": "$(uname -s)/$(uname -m)",
  "command": "bash ai/projects/topru-ai/verify/e_integ_smoke.sh",
  "artifact": "${RUN_LOG}"
}
EOF

echo "PASS: E_integ captured. run.log=${RUN_LOG} manifest=${MANIFEST}" | tee -a "${RUN_LOG}"
