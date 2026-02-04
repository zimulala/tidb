#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

STATE="ai/projects/topru-ai/PROJECT_STATE.md"
AUDIT="ai/projects/topru-ai/artifacts/audit/last_audit.txt"
OUT="ai/projects/topru-ai/artifacts/audit/evidence_summary.md"

mkdir -p "$(dirname "$OUT")"

extract_ssot() {
  awk '
    /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {inblk=1; next}
    /<!-- NAVIGATOR:END SSOT_V2 -->/ {inblk=0}
    inblk==1 {print}
  ' "$STATE"
}

SSOT="$(extract_ssot)"

evidence_row() {
  local id="$1"
  echo "$SSOT" | awk -v id="$id" '
    $0 ~ "^[[:space:]]*-[[:space:]]*id:[[:space:]]*"id"([[:space:]]*$|[[:space:]]*#)" {found=1; next}
    found==1 && $0 ~ "^[[:space:]]*-[[:space:]]*id:" {exit}
    found==1 && $0 ~ "^[[:space:]]*(status|type|commit|patch_id|time|env|command|artifact):" {print}
  '
}

{
  echo "# TopRU Evidence Summary"
  echo
  echo "Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "## PR-ready audit"
  if [[ -f "$AUDIT" ]]; then
    echo '```'
    cat "$AUDIT"
    echo '```'
  else
    echo "_No audit report found at $AUDIT_"
  fi
  echo
  echo "## Evidence items"
  for id in E_func E_integ E_perf; do
    echo "### $id"
    echo '```yaml'
    evidence_row "$id" || true
    echo '```'
    echo
  done
  echo "## Local artifact locations"
  echo "- E_func: ai/projects/topru-ai/artifacts/evidence/E_func/"
  echo "- E_integ: ai/projects/topru-ai/artifacts/evidence/E_integ/"
  echo "- E_perf: ai/projects/topru-ai/artifacts/evidence/E_perf/"
} > "$OUT"

echo "[summary] wrote $OUT"

