#!/usr/bin/env bash
set -euo pipefail

# Compat matrix recipe (draft):
# Goal: document and (eventually) execute a basic compatibility matrix for TopRU.
# Notes:
# - This script MUST NOT modify source code.
# - It's OK if this is not runnable tonight; keep TODOs explicit.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"             # .../ai/projects/topru-ai
REPO_DIR="$(cd "${PROJECT_DIR}/../../.." && pwd)"         # repo root

EID="E_compat"
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

# TODO: Replace with a real compat runner.
# For tonight: log the intended matrix + a couple of low-cost sanity checks.
COMMAND="TODO: compat matrix (Resource Control on/off, upgrade expectations, sink compatibility)"

{
  echo "EID=${EID}"
  echo "time=${TIME_UTC}"
  echo "commit=${COMMIT}"
  echo "patch_id=${PATCH_ID}"
  echo "env=${ENV_STR}"
  echo "command=${COMMAND}"
  echo
  echo "Matrix (draft):"
  echo "  - Resource Control: on/off"
  echo "  - TopSQL: on/off"
  echo "  - TopRU: on/off"
  echo "  - Sink: pubsub/single_target (if applicable)"
  echo "  - Upgrade: same-version restart, rolling upgrade expectations (TODO)"
  echo
  echo "Low-cost checks (no cluster):"
  echo "  - go test -tags=intest ./pkg/util/topsql/... -count=1"
  echo "  - go test -tags=intest ./pkg/executor -run TestObserveStmtBeginForTopSQL_RegisterSQLPlanMeta_WhenTopRUEnabledAndTopSQLDisabled -count=1"
  echo
  echo "TODO: implement cluster-backed cases and protobuf forward/backward expectations."
} >"${RUN_LOG}"

cat >"${MANIFEST}" <<EOF
{
  "evidence_id": "${EID}",
  "status": "Planned",
  "type": "compat_matrix",
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

