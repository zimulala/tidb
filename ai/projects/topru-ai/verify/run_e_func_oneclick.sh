#!/usr/bin/env bash
set -Eeuo pipefail

EID="E_func"
ETYPE="test"         # keep aligned with SSOT
TIDB_HOST="${TIDB_HOST:-127.0.0.1}"
TIDB_PORT="${TIDB_PORT:-4000}"

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

# load common lib
source "ai/projects/topru-ai/verify/lib_oneclick.sh"

PROJECT_ROOT="ai/projects/topru-ai"
STATE_FILE="${PROJECT_ROOT}/PROJECT_STATE.md"
ART_DIR="${PROJECT_ROOT}/artifacts/evidence/${EID}"
RUN_LOG="${ART_DIR}/run.log"
MANIFEST="${ART_DIR}/manifest.json"
TIDB_LOG_PATH="${TIDB_LOG_PATH:-$ROOT/tidb.log}"

mkdir -p "$ART_DIR"

# auto-generate recipes if missing
bash "ai/projects/topru-ai/verify/generate_recipes.sh" >/dev/null 2>&1 || true

TIDB_PID=""
cleanup() {
  log "cleanup..."
  if [[ "${KEEP_PROCS:-0}" == "1" ]]; then
    log "KEEP_PROCS=1, skip cleanup"
    return 0
  fi
  if [[ -n "${TIDB_PID}" ]]; then
    kill "${TIDB_PID}" >/dev/null 2>&1 || true
    sleep 0.3 || true
    kill -9 "${TIDB_PID}" >/dev/null 2>&1 || true
  fi
  kill_listen_port "${TIDB_PORT}"
  lsof -nP -iTCP:${TIDB_PORT} -sTCP:LISTEN >/dev/null 2>&1 || true
}
trap cleanup EXIT

log "repo root: $ROOT"
log "evidence dir: $ART_DIR"

# preflight
kill_listen_port "${TIDB_PORT}"

# start TiDB
log "starting tidb-server ..."
: > "$TIDB_LOG_PATH"
./bin/tidb-server --log-file="$TIDB_LOG_PATH" >/dev/null 2>&1 &
TIDB_PID=$!
log "tidb-server pid=${TIDB_PID}"

log "waiting for TiDB ${TIDB_HOST}:${TIDB_PORT} ..."
if ! wait_port "$TIDB_HOST" "$TIDB_PORT" "${READY_TIMEOUT_SEC:-30}"; then
  warn "TiDB did not open port ${TIDB_PORT} in time"
  tail -n 120 "$TIDB_LOG_PATH" || true
  exit 1
fi

if [[ "${SLEEP_AFTER_TIDB:-0}" != "0" ]]; then
  log "sleep after tidb: ${SLEEP_AFTER_TIDB}s"
  sleep "${SLEEP_AFTER_TIDB}"
fi

# run func smoke
log "running E_func smoke ..."
: > "$RUN_LOG"

export TIDB_LOG_PATH="$TIDB_LOG_PATH"
export VERIFY_MODE="log"
export TIDB_HOST="$TIDB_HOST"
export TIDB_PORT="$TIDB_PORT"

set +e
bash "ai/projects/topru-ai/verify/e_func_smoke.sh" 2>&1 | tee -a "$RUN_LOG"
RC=${PIPESTATUS[0]}
set -e

log "smoke rc=${RC}"
[[ "${RC}" == "0" ]] || exit "${RC}"

# write manifest + patch SSOT
COMMIT="$(git rev-parse HEAD)"
PATCH_ID="$(patch_id_of_head)"
TIME="$(now_iso)"
ENVSTR="$(env_string)"
CMD="bash ai/projects/topru-ai/verify/e_func_smoke.sh (VERIFY_MODE=log, TIDB_LOG_PATH=$TIDB_LOG_PATH)"

write_manifest "$MANIFEST" "$EID" "$ETYPE" "Captured" "$COMMIT" "$PATCH_ID" "$TIME" "$ENVSTR" "$CMD" "$RUN_LOG"

log "patching SSOT evidence: ${EID} -> Captured"
ssot_patch_evidence "$STATE_FILE" "$EID" "Captured" "$ETYPE" "$COMMIT" "$PATCH_ID" "$TIME" "$ENVSTR" "$CMD" "$RUN_LOG"

log "DONE. artifacts:"
log "  - $RUN_LOG"
log "  - $MANIFEST"
log "  - tidb log: $TIDB_LOG_PATH"

