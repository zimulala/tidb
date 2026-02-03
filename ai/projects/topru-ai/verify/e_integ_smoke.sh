#!/usr/bin/env bash
set -euo pipefail

# Integration smoke recipe (draft):
# Goal: end-to-end proof that TopRU records + meta are generated and observable from a sink.
# Notes:
# - This script MUST NOT modify source code.
# - It's OK if this is not runnable tonight; keep TODOs explicit.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"             # .../ai/projects/topru-ai
REPO_DIR="$(cd "${PROJECT_DIR}/../../.." && pwd)"         # repo root

EID="E_integ"
OUT_DIR="${PROJECT_DIR}/artifacts/evidence/${EID}"
RUN_LOG="${OUT_DIR}/run.log"
MANIFEST="${OUT_DIR}/manifest.json"

mkdir -p "${OUT_DIR}"

TIME_UTC="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
COMMIT="$(git -C "${REPO_DIR}" rev-parse HEAD 2>/dev/null || echo "UNKNOWN")"
if git -C "${REPO_DIR}" diff --quiet --no-ext-diff 2>/dev/null; then
  PATCH_ID="${COMMIT}"
else
  PATCH_ID="$(git -C "${REPO_DIR}" diff --no-color | shasum -a 256 | awk '{print $1}')"
fi

GO_VER="$(go version 2>/dev/null || echo "go:UNKNOWN")"
ENV_STR="$(uname -s)/$(uname -m) ${GO_VER}"

# TODO: Replace with a real end-to-end harness.
# Ideas (pick one):
# 1) tests/integrationtest: add a targeted test that enables TopRU and asserts sink output.
# 2) realtikvtest: run a minimal cluster and verify TopRU output under real TiKV.
# 3) local tidb-server: start server with TopRU enabled + pubsub/single_target sink, run queries, parse output.
COMMAND="TODO: integration smoke harness (end-to-end TopRU record+meta path)"

{
  echo "EID=${EID}"
  echo "time=${TIME_UTC}"
  echo "commit=${COMMIT}"
  echo "patch_id=${PATCH_ID}"
  echo "env=${ENV_STR}"
  echo "command=${COMMAND}"
  echo
  echo "TODO: implement one of the harness options listed in this script."
} >"${RUN_LOG}"

cat >"${MANIFEST}" <<EOF
{
  "evidence_id": "${EID}",
  "status": "Planned",
  "type": "integration_smoke",
  "commit": "${COMMIT}",
  "patch_id": "${PATCH_ID}",
  "time": "${TIME_UTC}",
  "env": "${ENV_STR}",
  "command": "${COMMAND}",
  "artifacts": [
    "run.log",
    "manifest.json"
  ]
}
EOF

echo "wrote ${RUN_LOG}"
echo "wrote ${MANIFEST}"

