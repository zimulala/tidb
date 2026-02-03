#!/usr/bin/env bash
set -euo pipefail

EID="E_func"
ROOT="ai/projects/topru-ai/artifacts/evidence/${EID}"
mkdir -p "${ROOT}"

CMD="go test -tags=intest ./pkg/util/topsql/stmtstats -run 'TestMergeRUIntoInFlightSamplingAndFinishDedup|TestExecCountBeginBased_.*' -count=1"
echo "${CMD}" | tee "${ROOT}/run.log"
bash -lc "${CMD}" 2>&1 | tee -a "${ROOT}/run.log"

# manifest.json (minimal)
cat > "${ROOT}/manifest.json" <<EOF
{
  "evidence_id": "${EID}",
  "status": "Captured",
  "type": "test",
  "commit": "$(git rev-parse HEAD)",
  "patch_id": "$(git show HEAD | git patch-id --stable | awk '{print $1}')",
  "time": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "env": "$(uname -s)/$(uname -m)",
  "command": "${CMD}",
  "artifact": "${ROOT}/run.log"
}
EOF
