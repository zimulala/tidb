#!/usr/bin/env bash
# ai/projects/topru-ai/verify/lib_oneclick.sh
set -Eeuo pipefail

ts() { date "+%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)][oneclick] $*"; }
warn() { echo "[$(ts)][oneclick][WARN] $*" >&2; }
die() { echo "[$(ts)][oneclick][FATAL] $*" >&2; exit 1; }

on_err() {
  local ec=$?
  echo "[$(ts)][oneclick][ERR] exit_code=$ec line=${BASH_LINENO[0]} cmd=${BASH_COMMAND}" >&2
  exit $ec
}
# Allow callers (e.g. run_pr_ready_oneclick.sh) to manage error handling for rc capture.
if [[ "${ONECLICK_DISABLE_ERR_TRAP:-0}" != "1" ]]; then
  trap on_err ERR
fi

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

git_root() {
  git rev-parse --show-toplevel 2>/dev/null || true
}

now_iso() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

env_string() {
  # keep it short but useful (avoid long multiline)
  local gover=""
  if command -v go >/dev/null 2>&1; then
    gover="$(go env GOVERSION 2>/dev/null || true)"
  fi
  echo "$(uname -s)/$(uname -m) ${gover:-goUNKNOWN} $(uname -s | tr '[:upper:]' '[:lower:]')/$(uname -m)"
}

patch_id_of_head() {
  (git show HEAD | git patch-id --stable | awk '{print $1}') 2>/dev/null || echo "UNKNOWN"
}

init_trace() {
  # init_trace <art_dir_optional>
  #
  # Default TRACE path is: ${ART_DIR}/trace.jsonl
  # Behavior:
  # - sets TRACE if unset
  # - truncates TRACE (best-effort)
  local art_dir="${1:-${ART_DIR:-}}"
  if [[ -z "${art_dir}" ]]; then
    return 0
  fi
  if [[ -z "${TRACE:-}" ]]; then
    TRACE="${art_dir}/trace.jsonl"
  fi
  mkdir -p "${art_dir}" >/dev/null 2>&1 || true
  : > "${TRACE}" 2>/dev/null || true
}

trace() {
  # trace <stage> <msg> <status> <json_details_raw_or_empty>
  local st="$1"
  local msg="$2"
  local status="$3"
  local details="${4:-}"
  local t

  if [[ -z "${TRACE:-}" ]]; then
    return 0
  fi

  t="$(now_iso)"
  # details should be JSON object string like {"k":"v"} or empty
  if [[ -z "${details}" ]]; then
    details="{}"
  fi
  # msg is best kept short; avoid unescaped quotes
  msg="${msg//\"/\' }"
  echo "{\"time\":\"${t}\",\"stage\":\"${st}\",\"status\":\"${status}\",\"msg\":\"${msg}\",\"details\":${details}}" >> "${TRACE}" 2>/dev/null || true
}

stage() {
  # stage <num> <message>
  echo "[oneclick][stage $1] $2"
  trace "$1" "$2" "ok" ""
}

kill_listen_port() {
  local port="$1"
  local pid=""
  pid="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "${pid}" ]]; then
    log "killing listener on port ${port}: pid=${pid}"
    kill "${pid}" >/dev/null 2>&1 || true
    sleep 0.2 || true
    kill -9 "${pid}" >/dev/null 2>&1 || true
  fi
}

wait_port() {
  local host="$1"
  local port="$2"
  local max_sec="${3:-30}"
  local i=0
  while [[ $i -lt $max_sec ]]; do
    if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    i=$((i+1))
    sleep 1
  done
  return 1
}

start_bg_redirect() {
  # usage: start_bg_redirect <logfile> <cmd...>
  local logfile="$1"; shift
  : > "${logfile}"
  "$@" >>"${logfile}" 2>&1 &
  echo $!
}

write_manifest() {
  # write_manifest <manifest_path> <eid> <type> <status> <commit> <patch_id> <time> <env> <command> <run_log> <extra_json_kv_optional>
  local manifest="$1"
  local eid="$2"
  local etype="$3"
  local status="$4"
  local commit="$5"
  local patch_id="$6"
  local time="$7"
  local envstr="$8"
  local cmd="$9"
  local run_log="${10}"
  local extra="${11:-}"

  cat > "${manifest}" <<EOF
{
  "id": "${eid}",
  "status": "${status}",
  "type": "${etype}",
  "commit": "${commit}",
  "patch_id": "${patch_id}",
  "time": "${time}",
  "env": "${envstr}",
  "command": "${cmd}",
  "artifacts": {
    "run_log": "${run_log}",
    "manifest": "${manifest}"
  }${extra}
}
EOF
}

ssot_patch_evidence() {
  # ssot_patch_evidence <project_state_md> <eid> <status> <type> <commit> <patch_id> <time> <env> <command> <artifact_path>
  local state="$1"
  local eid="$2"
  local status="$3"
  local etype="$4"
  local commit="$5"
  local patch_id="$6"
  local time="$7"
  local envstr="$8"
  local cmd="$9"
  local artifact="${10}"

  python3 ai/projects/topru-ai/verify/ssot_patch_evidence.py \
    --project_state "${state}" \
    --evidence_id "${eid}" \
    --status "${status}" \
    --type "${etype}" \
    --commit "${commit}" \
    --patch_id "${patch_id}" \
    --time "${time}" \
    --env "${envstr}" \
    --command "${cmd}" \
    --artifact "${artifact}"
}
