#!/usr/bin/env bash
set -euo pipefail

# Perf sanity recipe (draft):
# Goal: a quick, repeatable perf sanity signal for TopRU overhead under high cardinality.
# Notes:
# - This script MUST NOT modify source code.
# - It's OK if this is not runnable tonight; keep TODOs explicit.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"             # .../ai/projects/topru-ai
REPO_DIR="$(cd "${PROJECT_DIR}/../../.." && pwd)"         # repo root

EID="E_perf"
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

# TODO: Replace with a TopRU-focused perf harness.
# Suggestions:
# - Add a new benchmark for TopRU collecting/reporting under high cardinality.
# - Or run existing TopSQL benchmarks with TopRU enabled in the same process and compare allocations/CPU.
COMMAND="TODO: perf sanity (high cardinality TopRU overhead)"

{
  echo "EID=${EID}"
  echo "time=${TIME_UTC}"
  echo "commit=${COMMIT}"
  echo "patch_id=${PATCH_ID}"
  echo "env=${ENV_STR}"
  echo "command=${COMMAND}"
  echo
  echo "Existing benchmarks (TopSQL):"
  echo "  go test -tags=intest ./pkg/util/topsql/reporter -run '^$' -bench 'BenchmarkTopSQL_CollectAndIncrementFrequency|BenchmarkTopSQL_CollectAndEvict' -count=5"
  echo
  echo "TODO: implement a TopRU-specific high-cardinality benchmark/harness."
} >"${RUN_LOG}"

cat >"${MANIFEST}" <<EOF
{
  "evidence_id": "${EID}",
  "status": "Planned",
  "type": "perf_sanity",
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

