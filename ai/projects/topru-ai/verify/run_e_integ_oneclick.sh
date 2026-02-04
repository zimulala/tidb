#!/usr/bin/env bash
set -Eeuo pipefail

EID="E_integ"
ETYPE="integ_smoke"

SUB_ADDR="${SUB_ADDR:-127.0.0.1:10080}"
SUB_INTERVAL="${SUB_INTERVAL:-15s}"
TIDB_HOST="${TIDB_HOST:-127.0.0.1}"
TIDB_PORT="${TIDB_PORT:-4000}"

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

source "ai/projects/topru-ai/verify/lib_oneclick.sh"

PROJECT_ROOT="ai/projects/topru-ai"
STATE_FILE="${PROJECT_ROOT}/PROJECT_STATE.md"
ART_DIR="${PROJECT_ROOT}/artifacts/evidence/${EID}"
RUN_LOG="${ART_DIR}/run.log"
SUB_LOG="${ART_DIR}/subscriber.log"
MANIFEST="${ART_DIR}/manifest.json"
TIDB_LOG_PATH="${TIDB_LOG_PATH:-$ROOT/tidb.log}"

mkdir -p "$ART_DIR"

TIDB_PID=""
SUB_PID=""

cleanup() {
  log "cleanup..."
  if [[ "${KEEP_PROCS:-0}" == "1" ]]; then
    log "KEEP_PROCS=1, skip cleanup"
    return 0
  fi

  if [[ -n "${SUB_PID}" ]]; then
    kill "${SUB_PID}" >/dev/null 2>&1 || true
    sleep 0.2 || true
    kill -9 "${SUB_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${TIDB_PID}" ]]; then
    kill "${TIDB_PID}" >/dev/null 2>&1 || true
    sleep 0.2 || true
    kill -9 "${TIDB_PID}" >/dev/null 2>&1 || true
  fi

  kill_listen_port "10080"
  kill_listen_port "${TIDB_PORT}"
}
trap cleanup EXIT

log "repo root: $ROOT"
log "evidence dir: $ART_DIR"

# preflight: ensure clean ports
kill_listen_port "10080"
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

# start subscriber
log "starting topru subscriber on ${SUB_ADDR} ..."
SUB_PID="$(start_bg_redirect "$SUB_LOG" go run ./topru_subscriber.go -addr "$SUB_ADDR" -interval "$SUB_INTERVAL")"
log "subscriber pid=${SUB_PID}"

log "waiting for subscriber ${SUB_ADDR} ..."
if ! wait_port "${SUB_ADDR%:*}" "${SUB_ADDR##*:}" 20; then
  warn "subscriber did not open port ${SUB_ADDR} in time"
  tail -n 120 "$SUB_LOG" || true
  exit 1
fi

if [[ "${SLEEP_AFTER_SUBSCRIBER:-0}" != "0" ]]; then
  log "sleep after subscriber: ${SLEEP_AFTER_SUBSCRIBER}s"
  sleep "${SLEEP_AFTER_SUBSCRIBER}"
fi

# run integ smoke
log "running E_integ smoke ..."
: > "$RUN_LOG"

export TIDB_LOG_PATH="$TIDB_LOG_PATH"
export VERIFY_MODE="log"
export TIDB_HOST="$TIDB_HOST"
export TIDB_PORT="$TIDB_PORT"

set +e
bash ai/projects/topru-ai/verify/e_integ_smoke.sh 2>&1 | tee -a "$RUN_LOG"
RC=${PIPESTATUS[0]}
set -e

log "smoke rc=${RC}"
[[ "${RC}" == "0" ]] || exit "${RC}"

log "quick check: subscriber log signals:"
grep -E "recv|receive|record|topru|RU|digest|plan" -n "$SUB_LOG" | tail -n 20 || true

# write manifest with extra artifacts
COMMIT="$(git rev-parse HEAD)"
PATCH_ID="$(patch_id_of_head)"
TIME="$(now_iso)"
ENVSTR="$(env_string)"
CMD="bash ai/projects/topru-ai/verify/e_integ_smoke.sh (VERIFY_MODE=log, TIDB_LOG_PATH=$TIDB_LOG_PATH, subscriber=$SUB_ADDR)"

EXTRA=$(
  cat <<EOF
,
  "extra_artifacts": {
    "subscriber_log": "${SUB_LOG}",
    "tidb_log": "${TIDB_LOG_PATH}"
  },
  "result": {
    "smoke_rc": ${RC}
  }
EOF
)

write_manifest "$MANIFEST" "$EID" "$ETYPE" "Captured" "$COMMIT" "$PATCH_ID" "$TIME" "$ENVSTR" "$CMD" "$RUN_LOG" "$EXTRA"

log "patching SSOT evidence: ${EID} -> Captured"
ssot_patch_evidence "$STATE_FILE" "$EID" "Captured" "$ETYPE" "$COMMIT" "$PATCH_ID" "$TIME" "$ENVSTR" "$CMD" "$RUN_LOG"

log "DONE. artifacts:"
log "  - $RUN_LOG"
log "  - $SUB_LOG"
log "  - $MANIFEST"
log "  - tidb log: $TIDB_LOG_PATH"
