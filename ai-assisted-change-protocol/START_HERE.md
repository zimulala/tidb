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
