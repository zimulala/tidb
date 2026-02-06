#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
STRICT_REVIEW_FIX_ONECLICK

Usage:
  bash ai/projects/topru-ai/verify/strict_review_fix_oneclick.sh [--base <hash>] [--head <hash>] \
    [--mode first|incr|targeted] [--target R1,R2,...] [--no-patch-ssot]

Defaults:
  --head: git rev-parse HEAD
  --base: best-effort merge-base vs origin/master|origin/main|master|main
  --mode:
    - if PROJECT_STATE SSOT has review.baseline_commit -> incr
    - else -> first

This script:
  - collects review evidence (changed_files, diff, commits)
  - generates review.md + findings.yaml + next_actions.md (best-effort)
  - writes artifacts under ai/projects/topru-ai/artifacts/review/<RUN_ID>/
  - patches SSOT_V2 review section in ai/projects/topru-ai/PROJECT_STATE.md (unless --no-patch-ssot)

Safety:
  - no GitHub comments, no auto-merge, no auto-fix code.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"     # .../ai/projects/topru-ai
REPO_DIR="$(cd "${PROJECT_DIR}/../../.." && pwd)" # repo root
SSOT_FILE="${PROJECT_DIR}/PROJECT_STATE.md"

BASE=""
HEAD=""
MODE=""
TARGET_IDS=""
PATCH_SSOT="1"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base) BASE="$2"; shift 2;;
    --head) HEAD="$2"; shift 2;;
    --mode) MODE="$2"; shift 2;;
    --target) TARGET_IDS="$2"; shift 2;;
    --no-patch-ssot) PATCH_SSOT="0"; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

if [[ -z "${HEAD}" ]]; then
  HEAD="$(git -C "${REPO_DIR}" rev-parse HEAD)"
fi

best_effort_base() {
  local head="$1"
  local candidates=("origin/master" "origin/main" "master" "main")
  for c in "${candidates[@]}"; do
    if git -C "${REPO_DIR}" rev-parse --verify "${c}" >/dev/null 2>&1; then
      git -C "${REPO_DIR}" merge-base "${c}" "${head}" && return 0
    fi
  done
  echo ""
  return 0
}

if [[ -z "${BASE}" ]]; then
  BASE="$(best_effort_base "${HEAD}")"
fi
if [[ -z "${BASE}" ]]; then
  echo "ERROR: could not determine --base (please provide --base <hash>)" >&2
  exit 2
fi

if [[ ! -f "${SSOT_FILE}" ]]; then
  echo "ERROR: SSOT not found: ${SSOT_FILE}" >&2
  exit 2
fi

if [[ -z "${MODE}" ]]; then
  if grep -Eq "^review:\\s*$" "${SSOT_FILE}" && grep -q "baseline_commit:" "${SSOT_FILE}"; then
    MODE="incr"
  else
    MODE="first"
  fi
fi

TIME_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
HEAD_SHORT="$(echo "${HEAD}" | cut -c1-7)"
RUN_ID="${TIME_UTC}_${HEAD_SHORT}_${MODE}"
OUT_DIR="${PROJECT_DIR}/artifacts/review/${RUN_ID}"
mkdir -p "${OUT_DIR}"

RUN_LOG="${OUT_DIR}/run.log"
CHANGED_FILES="${OUT_DIR}/changed_files.txt"
DIFF_PATCH="${OUT_DIR}/diff.patch"
COMMITS_TXT="${OUT_DIR}/commits.txt"
SUMMARY_TXT="${OUT_DIR}/summary.txt"
REVIEW_MD="${OUT_DIR}/review.md"
FINDINGS_YAML="${OUT_DIR}/findings.yaml"
NEXT_ACTIONS_MD="${OUT_DIR}/next_actions.md"
MANIFEST_JSON="${OUT_DIR}/manifest.json"

# Log everything (including stderr) into run.log while still echoing to terminal.
exec > >(tee "${RUN_LOG}") 2>&1

echo "STRICT_REVIEW_FIX_ONECLICK"
echo "time_utc: ${TIME_UTC}"
echo "mode: ${MODE}"
echo "base: ${BASE}"
echo "head: ${HEAD}"
echo "range: ${BASE}..${HEAD}"
echo "out_dir: ${OUT_DIR}"
echo "target: ${TARGET_IDS:-<none>}"
echo

git -C "${REPO_DIR}" diff --name-only "${BASE}..${HEAD}" >"${CHANGED_FILES}"
git -C "${REPO_DIR}" diff "${BASE}..${HEAD}" >"${DIFF_PATCH}"
git -C "${REPO_DIR}" log --oneline --decorate "${BASE}..${HEAD}" >"${COMMITS_TXT}" || true

CHANGED_COUNT="$(wc -l <"${CHANGED_FILES}" | tr -d ' ')"
COMMIT_COUNT="$(wc -l <"${COMMITS_TXT}" | tr -d ' ' || echo 0)"

{
  echo "STRICT_REVIEW_SUMMARY"
  echo "base: ${BASE}"
  echo "head: ${HEAD}"
  echo "range: ${BASE}..${HEAD}"
  echo "mode: ${MODE}"
  echo "changed_files_count: ${CHANGED_COUNT}"
  echo "commits_count: ${COMMIT_COUNT}"
  echo
  echo "changed_files:"
  sed -n '1,200p' "${CHANGED_FILES}"
  echo
  echo "commits:"
  sed -n '1,200p' "${COMMITS_TXT}" || true
} >"${SUMMARY_TXT}"

# -------------------------
# Step 2: findings + review
# -------------------------

# Best-effort auto findings:
# - R_fmt: gofmt drift on changed .go files (must-fix)
CHANGED_GO_FILES="$(grep -E "\\.go$" "${CHANGED_FILES}" || true)"
GOFMT_BAD=""
if [[ -n "${CHANGED_GO_FILES}" ]] && command -v gofmt >/dev/null 2>&1; then
  # shellcheck disable=SC2086
  GOFMT_BAD="$(cd "${REPO_DIR}" && echo "${CHANGED_GO_FILES}" | xargs -n 50 gofmt -l 2>/dev/null || true)"
fi

ssot_review_open_ids() {
  # Extract review.open from SSOT_V2 if present: `open: [R1, R2]` -> `R1,R2`
  awk '
    /<!-- NAVIGATOR:BEGIN SSOT_V2 -->/ {in=1; next}
    /<!-- NAVIGATOR:END SSOT_V2 -->/ {in=0}
    in && /^review:\s*$/ {rev=1; next}
    in && rev && /^[A-Za-z0-9_]+:\s*$/ {rev=0}
    in && rev && /^  open:\s*/ {print; exit}
  ' "${SSOT_FILE}" | sed -E 's/.*\\[([^\\]]*)\\].*/\\1/; s/[[:space:]]//g' || true
}

contains_id() {
  local hay="$1"
  local needle="$2"
  [[ ",${hay}," == *",${needle},"* ]]
}

OPEN_IDS_CSV=""
case "${MODE}" in
  first)
    OPEN_IDS_CSV="R_manual"
    ;;
  incr)
    OPEN_IDS_CSV="$(ssot_review_open_ids)"
    if [[ -z "${OPEN_IDS_CSV}" ]]; then
      OPEN_IDS_CSV="R_manual"
    fi
    ;;
  targeted)
    if [[ -z "${TARGET_IDS}" ]]; then
      echo "ERROR: --mode targeted requires --target R1,R2,..." >&2
      exit 2
    fi
    OPEN_IDS_CSV="${TARGET_IDS}"
    ;;
  *)
    echo "ERROR: unknown mode: ${MODE}" >&2
    exit 2
    ;;
esac

# If gofmt issues exist in current diff, ensure R_fmt is included.
if [[ -n "${GOFMT_BAD}" ]] && ! contains_id "${OPEN_IDS_CSV}" "R_fmt"; then
  if [[ -z "${OPEN_IDS_CSV}" ]]; then
    OPEN_IDS_CSV="R_fmt"
  else
    OPEN_IDS_CSV="${OPEN_IDS_CSV},R_fmt"
  fi
fi

{
  echo "baseline: \"${BASE}\""
  echo "head: \"${HEAD}\""
  echo "mode: \"${MODE}\""
  echo "findings:"
  IFS=',' read -r -a OPEN_IDS_ARR <<<"${OPEN_IDS_CSV}"
  for rid in "${OPEN_IDS_ARR[@]}"; do
    [[ -z "${rid}" ]] && continue
    case "${rid}" in
      R_fmt)
        if [[ -n "${GOFMT_BAD}" ]]; then
          cat <<EOF
  - id: R_fmt
    must_fix: true
    type: "Style"
    impact: "DeveloperExperience"
    scope: "formatting"
    likelihood: "High"
    location: "$(echo "${GOFMT_BAD}" | head -n 1)"
    title: "Changed Go files are not gofmt'ed"
    advice: "Run gofmt on the listed files (do not change semantics)."
    verify_min:
      - "gofmt -l <files> (expect empty)"
    verify_opt: []
    cleanup: []
    status: open
EOF
        else
          cat <<'EOF'
  - id: R_fmt
    must_fix: true
    type: "Style"
    impact: "DeveloperExperience"
    scope: "formatting"
    likelihood: "High"
    location: "(no gofmt drift detected)"
    title: "gofmt drift fixed"
    advice: "N/A"
    verify_min:
      - "gofmt -l <files> (expect empty)"
    verify_opt: []
    cleanup: []
    status: fixed
EOF
        fi
        ;;
      R_manual)
        cat <<'EOF'
  - id: R_manual
    must_fix: true
    type: "Risk"
    impact: "Correctness"
    scope: "TopRU-path"
    likelihood: "Med"
    location: "TODO: fill from diff review"
    title: "Manual strict review required for TopRU changes"
    advice: "Review key correctness/concurrency/perf/compat aspects; add/adjust tests as needed."
    verify_min:
      - "bash ai/projects/topru-ai/verify/e_integ_smoke.sh (log-based smoke)"
    verify_opt:
      - "bash ai/projects/topru-ai/verify/e_perf_sanity.sh"
      - "bash ai/projects/topru-ai/verify/e_compat_matrix.sh"
    cleanup: []
    status: open
EOF
        ;;
      *)
        cat <<EOF
  - id: ${rid}
    must_fix: true
    type: "TODO"
    impact: "TODO"
    scope: "TODO"
    likelihood: "TODO"
    location: "TODO"
    title: "TODO: fill finding ${rid}"
    advice: "TODO"
    verify_min: []
    verify_opt: []
    cleanup: []
    status: open
EOF
        ;;
    esac
  done
} >"${FINDINGS_YAML}"

# Human-readable review.md (Chinese) that mirrors findings.yaml.
{
  echo "# STRICT_REVIEW — TopRU (auto-generated)"
  echo
  echo "## Snapshot"
  echo "- mode: ${MODE}"
  echo "- range: ${BASE}..${HEAD}"
  echo "- changed_files_count: ${CHANGED_COUNT}"
  echo "- artifacts_dir: ${OUT_DIR}"
  echo
  echo "## Evidence Collected"
  echo "- changed_files: ${CHANGED_FILES}"
  echo "- diff_patch: ${DIFF_PATCH}"
  echo "- commits: ${COMMITS_TXT}"
  echo "- summary: ${SUMMARY_TXT}"
  echo
  echo "## Findings (machine source: findings.yaml)"
  echo
  echo "说明：本文件是严格 review 的中文输出；机读来源为 findings.yaml。"
  echo
  IFS=',' read -r -a REVIEW_IDS_ARR <<<"${OPEN_IDS_CSV}"
  for rid in "${REVIEW_IDS_ARR[@]}"; do
    [[ -z "${rid}" ]] && continue
    echo "### ${rid}"
    case "${rid}" in
      R_fmt)
        if [[ -n "${GOFMT_BAD}" ]]; then
          echo "- 类型: Style"
          echo "- Must fix: Yes"
          echo "- 影响面: DeveloperExperience"
          echo "- 范围: formatting"
          echo "- 概率: High"
          echo "- 位置: ${GOFMT_BAD}"
          echo "- 建议: 对以上文件执行 gofmt（不改语义）。"
          echo "- 最小验证: gofmt -l <files> (expect empty)"
          echo "- 推荐验证: (none)"
          echo "- 清理: (none)"
        else
          echo "- 状态: fixed（本次 diff 未检测到 gofmt drift）"
        fi
        ;;
      R_manual)
        echo "- 类型: Risk"
        echo "- Must fix: Yes"
        echo "- 影响面: Correctness"
        echo "- 范围: TopRU-path"
        echo "- 概率: Med"
        echo "- 位置: TODO（请从 diff.patch 中补充具体 hunk/file）"
        echo "- 建议: 做一次严格 review：并发/边界条件/兼容性/性能；必要时补 UT/集成证据。"
        echo "- 最小验证:"
        echo "  - bash ai/projects/topru-ai/verify/e_integ_smoke.sh"
        echo "- 推荐验证:"
        echo "  - bash ai/projects/topru-ai/verify/e_perf_sanity.sh"
        echo "  - bash ai/projects/topru-ai/verify/e_compat_matrix.sh"
        echo "- 清理: (none)"
        ;;
      *)
        echo "- TODO: fill ${rid} details"
        ;;
    esac
    echo
  done

  echo "## Notes"
  echo "- TODO: 将 R_manual 拆分为更细的 R#（带明确 location/verify_min）。"
} >"${REVIEW_MD}"

# next_actions.md (must-fix first)
{
  echo "# Next Actions (from strict review)"
  echo
  echo "Artifacts: ${OUT_DIR}"
  echo
  echo "1) Fix open must-fix findings first:"
  awk '
    /^\s*-\s+id:\s*/ {id=$3}
    /^\s*must_fix:\s*true\s*$/ {must=1}
    /^\s*status:\s*open\s*$/ { if (id != "" && must == 1) { print "   - " id }; id=""; must=0 }
  ' "${FINDINGS_YAML}" | sed -n '1,10p'
  echo
  echo "2) Re-run (incremental re-review):"
  echo "   bash ai/projects/topru-ai/verify/strict_review_fix_oneclick.sh --base ${BASE} --head ${HEAD} --mode incr"
} >"${NEXT_ACTIONS_MD}"

# review run manifest (commit-bound)
cat >"${MANIFEST_JSON}" <<EOF
{
  "run_id": "${RUN_ID}",
  "type": "strict_review_fix_oneclick",
  "mode": "${MODE}",
  "base": "${BASE}",
  "head": "${HEAD}",
  "time": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "commit": "$(git -C "${REPO_DIR}" rev-parse HEAD)",
  "patch_id": "$(git -C "${REPO_DIR}" show HEAD | git patch-id --stable | awk '{print $1}')",
  "artifacts": [
    "run.log",
    "changed_files.txt",
    "diff.patch",
    "commits.txt",
    "summary.txt",
    "review.md",
    "findings.yaml",
    "next_actions.md",
    "manifest.json"
  ]
}
EOF

# -------------------------
# Step 3: patch SSOT review
# -------------------------
if [[ "${PATCH_SSOT}" == "1" ]]; then
  python3 "${PROJECT_DIR}/verify/ssot_patch_review.py" \
    --ssot "${SSOT_FILE}" \
    --base "${BASE}" \
    --head "${HEAD}" \
    --run-range "${BASE}..${HEAD}" \
    --findings-yaml "${FINDINGS_YAML}" \
    --review-md "ai/projects/topru-ai/artifacts/review/${RUN_ID}/review.md" \
    --run-log "ai/projects/topru-ai/artifacts/review/${RUN_ID}/run.log" \
    --changed-files "ai/projects/topru-ai/artifacts/review/${RUN_ID}/changed_files.txt" \
    --diff-patch "ai/projects/topru-ai/artifacts/review/${RUN_ID}/diff.patch" \
    --mode "${MODE}"
else
  echo "SSOT patch disabled (--no-patch-ssot)"
fi

# -------------------------
# Step 5 (best-effort): PR-ready check (audit + review-open)
# -------------------------
echo
echo "=== PR-ready check (best-effort) ==="
AUDIT_FILE="${PROJECT_DIR}/artifacts/audit/last_audit.txt"
if [[ -x "${PROJECT_DIR}/verify/audit_ssot.sh" ]]; then
  bash "${PROJECT_DIR}/verify/audit_ssot.sh" --project "ai/projects/topru-ai" --track "resource-observability-topru" --protocol-root "ai/ai-change-gates" || true
else
  echo "NOTE: audit_ssot.sh not found/executable; skipped"
fi

OPEN_COUNT="$(grep -E '^[[:space:]]*status:[[:space:]]*open[[:space:]]*$' "${FINDINGS_YAML}" | wc -l | tr -d ' ')"
if [[ "${OPEN_COUNT}" == "0" ]]; then
  echo "REVIEW_OPEN=0"
else
  echo "REVIEW_OPEN=${OPEN_COUNT}"
fi

AUDIT_STATUS="unknown"
if [[ -f "${AUDIT_FILE}" ]]; then
  AUDIT_STATUS="$(awk -F': ' '/^pr_ready_status: /{print $2; exit}' "${AUDIT_FILE}" || true)"
fi
echo "AUDIT_PR_READY=${AUDIT_STATUS}"

PR_READY="false"
if [[ "${OPEN_COUNT}" == "0" && "${AUDIT_STATUS}" == "true" ]]; then
  PR_READY="true"
fi
echo "PR_READY=${PR_READY}"
echo "Artifacts: ${OUT_DIR}"

if [[ "${PR_READY}" == "true" ]]; then
  exit 0
fi
exit 3
