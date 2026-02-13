# START HERE — AI-assisted Change Protocol Quick Path

## When to use this

Use Quick Path when you want the protocol benefits but with minimal overhead.
You write one short brief; Codex generates Phase 0/1/2/3B drafts and guides you through approvals.

## Activation phrase (opt-in)

Only use the automation flow if you explicitly say:

- 半自动化功能实现

If you do not say the phrase, Codex should behave normally (no forced protocol flow).

Normal mode reminder:
- If you do not opt in, I will not auto-generate Phase docs or enforce the protocol gates.

## Step 1 — Fill the brief

Open and fill:

- templates/CHANGE_BRIEF.md

If you want the 3-line version (opt-in only), use:

- templates/CHANGE_BRIEF_MINI.md

## Step 2 — Generate Phase drafts

Use the prompt:

- prompts/quickstart.prompt.md

For the 3-line version:

- prompts/quickstart-mini.prompt.md

Input:
- Your filled templates/CHANGE_BRIEF.md

Output (drafts to paste into templates):
- PHASE_0_INTAKE.md
- PHASE_1_RISK_REGISTER.md
- PHASE_2_DESIGN_RECORD.md
- PHASE_3B_VALIDATION_RECORD.md

## Step 3 — Approve or STOP (gates)

Hard STOP if any gate fails:
- Open assumptions exist in templates/ASSUMPTIONS_REGISTER.md
- Semantic decisions lack explicit maintainer GO in templates/SEMANTIC_SPEC.md
- Any “Verified-by-test” claim is made without an Evidence ID in templates/EVIDENCE_INDEX.md
- If the feature is pipeline-like: templates/DATA_PATH_MAP.md is required but not concretely filled
- If a track is selected: track required artifacts/gates are missing (see tracks/README.md)

When all items are approved, you can enter Phase 3C (Implementation).

## Step 4 — Implementation + Review

- Phase 3C: templates/PHASE_3C_IMPLEMENTATION_LOG.md
- Phase 3D: templates/PHASE_3D_REVIEW_REPORT.md
- Failure assumptions: templates/FAILURE_ASSUMPTIONS.md
- Evidence (if any): templates/EVIDENCE_INDEX.md

## Optional — Use a track

Tracks add domain templates + gates without changing core governance.

- tracks/README.md

## Rules to remember

- No scope expansion unless explicitly approved.
- No "PASS" / "MERGE APPROVED" without evidence.
- STOP is a valid outcome.

## Navigator mode (recommended)

Use the protocol without memorizing phases.

### One-line usage
Say to your agent:

NAVIGATE: project=<path> track=<optional>

Examples:
- NAVIGATE: project=topru-ai track=resource-observability-topru
- NAVIGATE: project=diagnostics-bundle-ai track=observability-pipeline
- NAVIGATE: project=my-new-project

### What the agent must do
1) Read project governance files if present:
- <project>/PROJECT_STATE.md
- <project>/SEMANTIC_SPEC.md and/or <project>/TOPRU_SEMANTIC_SPEC.md
- <project>/ASSUMPTIONS_REGISTER.md
- <project>/EVIDENCE_INDEX.md
- <project>/DATA_PATH_MAP.md (optional; generate on-demand)
- <project>/PHASE_3D_REVIEW_REPORT.md (optional; generate on-demand)

2) Determine exactly one state:
- LOCK_SEMANTICS
- CLOSE_ASSUMPTIONS
- COLLECT_EVIDENCE
- FINAL_REVIEW
- IMPLEMENT

3) Output MUST be short:
- State
- Gate blockers (if any)
- Next actions (max 3): each with exact file path + exact edit intent
- Run record path (`ai/ai-change-gates/runs/YYYY-MM-DD_run-<run_id>.json`)
- STOP (no long explanations)

4) Write one black-box run record for every run:
- Generate:
  - `ai/ai-change-gates/runs/YYYY-MM-DD_run-<run_id>.json`
  - `ai/ai-change-gates/runs/YYYY-MM-DD_run-<run_id>.md`
- Update `<project>/PROJECT_STATE.md` with `last_run: ai/ai-change-gates/runs/<...>.json`

## NAVIGATE+PATCH mode

Say:

NAVIGATE+PATCH: project=<path> track=<optional>

Behavior:
- Same as NAVIGATE, plus:
  - Create missing governance files under <project>/ by copying protocol templates
  - Insert placeholder TODO blocks for missing required slots (do NOT invent facts)
  - Update <project>/PROJECT_STATE.md with current state and next actions
  - MUST generate run record json+md under `ai/ai-change-gates/runs/`
  - MUST update `<project>/PROJECT_STATE.md` `last_run` pointer to latest run record json

Safety (hard):
- MUST NOT modify any source code files.
- MUST NOT modify any build/config files (Makefile/BUILD/WORKSPACE/etc).
- Only allowed to create/edit markdown files under <project>/, plus run record files under ai/ai-change-gates/runs (.json/.md).
