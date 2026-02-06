#!/usr/bin/env bash
set -Eeuo pipefail

EID="E_perf"
ETYPE="perf_sanity"
TIDB_HOST="${TIDB_HOST:-127.0.0.1}"
TIDB_PORT="${TIDB_PORT:-4000}"

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

source "ai/projects/topru-ai/verify/lib_oneclick.sh"

PROJECT_ROOT="ai/projects/topru-ai"
STATE_FILE="${PROJECT_ROOT}/PROJECT_STATE.md"
ART_DIR="${PROJECT_ROOT}/artifacts/evidence/${EID}"
RUN_LOG="${ART_DIR}/run.log"
MANIFEST="${ART_DIR}/manifest.json"
TIDB_LOG_PATH="${TIDB_LOG_PATH:-$ROOT/tidb.log}"

mkdir -p "$ART_DIR"
init_trace "$ART_DIR"

bash "ai/projects/topru-ai/verify/generate_recipes.sh" >/dev/null 2>&1 || true

TIDB_PID=""
cleanup() {
  trace "cleanup" "cleanup start" "ok" "{\"keep_procs\":${KEEP_PROCS:-0}}"
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
  trace "cleanup" "cleanup done" "ok" "{}"
}
trap cleanup EXIT

log "repo root: $ROOT"
log "evidence dir: $ART_DIR"
stage 0 "preflight: kill old listeners + init artifacts"

kill_listen_port "${TIDB_PORT}"
trace 0 "preflight done" "ok" "{\"killed_ports\":[${TIDB_PORT}]}"

stage 1 "start tidb-server"
log "starting tidb-server ..."
: > "$TIDB_LOG_PATH"
./bin/tidb-server --log-file="$TIDB_LOG_PATH" >/dev/null 2>&1 &
TIDB_PID=$!
log "tidb-server pid=${TIDB_PID}"
trace 1 "tidb started" "ok" "{\"pid\":${TIDB_PID},\"log\":\"${TIDB_LOG_PATH}\"}"

stage 2 "readiness: wait tidb port ${TIDB_HOST}:${TIDB_PORT}"
log "waiting for TiDB ${TIDB_HOST}:${TIDB_PORT} ..."
if ! wait_port "$TIDB_HOST" "$TIDB_PORT" "${READY_TIMEOUT_SEC:-30}"; then
  warn "TiDB did not open port ${TIDB_PORT} in time"
  tail -n 120 "$TIDB_LOG_PATH" || true
  trace 2 "tidb readiness failed" "fail" "{\"host\":\"${TIDB_HOST}\",\"port\":${TIDB_PORT}}"
  exit 1
fi
trace 2 "tidb ready" "ok" "{\"host\":\"${TIDB_HOST}\",\"port\":${TIDB_PORT}}"

if [[ "${SLEEP_AFTER_TIDB:-0}" != "0" ]]; then
  log "sleep after tidb: ${SLEEP_AFTER_TIDB}s"
  sleep "${SLEEP_AFTER_TIDB}"
fi

stage 3 "run perf sanity"
log "running E_perf sanity ..."
: > "$RUN_LOG"

export TIDB_HOST="$TIDB_HOST"
export TIDB_PORT="$TIDB_PORT"
# knobs (optional):
# PERF_N=100 PERF_MIN_QPS=200

set +e
bash "ai/projects/topru-ai/verify/e_perf_sanity.sh" 2>&1 | tee -a "$RUN_LOG"
RC=${PIPESTATUS[0]}
set -e

log "perf rc=${RC}"
trace 3 "perf finished" "ok" "{\"rc\":${RC}}"
[[ "${RC}" == "0" ]] || exit "${RC}"

COMMIT="$(git rev-parse HEAD)"
PATCH_ID="$(patch_id_of_head)"
TIME="$(now_iso)"
ENVSTR="$(env_string)"
CMD="bash ai/projects/topru-ai/verify/e_perf_sanity.sh (PERF_N=${PERF_N:-50}, PERF_MIN_QPS=${PERF_MIN_QPS:-0})"

stage 4 "write manifest.json"
write_manifest "$MANIFEST" "$EID" "$ETYPE" "Captured" "$COMMIT" "$PATCH_ID" "$TIME" "$ENVSTR" "$CMD" "$RUN_LOG"
trace 4 "manifest written" "ok" "{\"manifest\":\"${MANIFEST}\"}"

stage 5 "patch SSOT evidence ${EID} -> Captured"
log "patching SSOT evidence: ${EID} -> Captured"
ssot_patch_evidence "$STATE_FILE" "$EID" "Captured" "$ETYPE" "$COMMIT" "$PATCH_ID" "$TIME" "$ENVSTR" "$CMD" "$RUN_LOG"
trace 5 "ssot patched" "ok" "{\"evidence_id\":\"${EID}\",\"status\":\"Captured\"}"

log "DONE. artifacts:"
log "  - $RUN_LOG"
log "  - $MANIFEST"
log "  - trace: ${TRACE}"
