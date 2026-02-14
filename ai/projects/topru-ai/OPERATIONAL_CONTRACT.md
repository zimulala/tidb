# TopRU Operational Contract

This document locks operational behavior for TopRU oneclick governance flows.

## Modes Semantics

### strict_review_fix_oneclick
- `--mode first`
  - Full strict review for an explicit or inferred `base..head`.
- `--mode incr`
  - Incremental strict review. Defaults to `SSOT.review.open` as processing scope.
  - New findings are only allowed when newly touched files are detected against last review artifacts.
- `--mode followup`
  - Always uses `SSOT.review.last_run.range` as `base..head`.
  - Ignores user-provided `--base/--head`.
  - Must print: `FOLLOWUP_BASE=<sha> FOLLOWUP_HEAD=<sha> (from SSOT.review.last_run)`.
  - Missing/invalid `last_run.range` is a hard failure.
- `--mode targeted --target Rn,...`
  - Re-check selected findings only.

## PR_READY Decision Contract

`run_pr_ready_oneclick.sh` returns `PR_READY=true` only when all are true:
- Evidence audit reports no missing MUST evidence.
- If `PR_READY_INCLUDE_REVIEW=1`, strict review result has no open findings and review run itself succeeded.
- `PROJECT_STATE.md` is patched with latest pr_ready audit/review state.

Default safety constraints:
- No auto-apply patch unless explicitly requested by command flags.
- No git push / remote side effects.
- No destructive filesystem actions.

## Gatecheck Contract

`ai/ai-change-gates/gatecheck/review_pack.sh` enforces:
- Required files present: `review.md`, `findings.yaml`.
- 3-layer headings exist (Summary / Review Strategy / Findings).
- `findings.yaml` entries include required keys.
- Open `must_fix=true` findings fail gate by default.

Fail codes are defined in:
- `ai/ai-change-gates/contracts/fail_codes.json`
- `ai/ai-change-gates/contracts/fail_codes.md`

Current review-pack fail codes:
- `REVIEW_PACK_MISSING_FILES`
- `REVIEW_PACK_MISSING_LAYERS`
- `REVIEW_PACK_FINDINGS_SCHEMA_INVALID`
- `REVIEW_PACK_OPEN_MUST_FIX`

## Recipes Contract

Verification command tokens use stable recipe IDs (`V_*`) and are expanded by:
- `ai/ai-change-gates/tools/expand_verify_recipe.sh`

Platform defaults:
- `ai/ai-change-gates/recipes/verify_recipes.yaml`

TopRU overrides:
- `ai/projects/topru-ai/verify/verify_recipes.override.yaml`

Rule:
- Findings should reference `V_*` recipes when possible instead of embedding long command lines.

## Pointers and Auditability

Strict review pointers:
- `ai/projects/topru-ai/artifacts/review/latest`
- `ai/projects/topru-ai/artifacts/review/latest.<branch>`
- `ai/projects/topru-ai/artifacts/review/latest.<worktree>`

PR-ready pointers:
- `ai/projects/topru-ai/artifacts/pr_ready/latest`
- `ai/projects/topru-ai/artifacts/pr_ready/latest.<branch>`
- `ai/projects/topru-ai/artifacts/pr_ready/latest.<worktree>`

Every run must keep auditable artifacts (`manifest.json`, `trace.jsonl`, `result.json`) and compare outputs (`compare_last_run.md`) where supported.
