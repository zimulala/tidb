# Role Contracts (A/B/C)

This document defines hard role separation for ai-change-gates governance workflow.

## Global hard rules
- Deterministic pass/fail: script rules decide outcome, not free-form AI judgment.
- Output must be machine-parseable and match fixed formats.
- Patch scope is restricted to SSOT placeholders and `ai/ai-change-gates/**`.
- No business/source code changes in Navigator/Patch roles.

## A) Navigator (Role A)

### Purpose
Produce a deterministic, machine-parseable plan from SSOT and governance artifacts.

### Inputs (allowed sources)
- SSOT path(s) placeholder(s) provided by caller.
- Governance files under `ai/ai-change-gates/**`.
- Optional track-level governance docs under `ai/ai-change-gates/tracks/**`.

### Outputs (exact format)
- Exactly one JSON object following `navigator_output.template.json` and `navigator_output.schema.json`.

### Forbidden actions
- No code/source/build edits.
- No patch generation.
- No non-deterministic free-form output outside fixed JSON.

### Quality bar
- `next_actions` length is 1..3.
- Every action has acceptance + evidence plan.
- Assumptions cannot be implicit; unknowns must be `needs_confirmation` with proof plan.

### Handoff artifact(s)
- `navigator_output` JSON (path determined by caller, usually under artifacts/runs).
- `run_id` for Patch role continuation.

## B) Patch (Role B)

### Purpose
Apply governance/SSOT-only patch planning with auditable diff and run-record reference.

### Inputs (allowed sources)
- Navigator output JSON.
- SSOT file placeholder path(s).
- Governance templates/contracts under `ai/ai-change-gates/**`.

### Outputs (exact format)
- Exactly two fenced blocks in order:
  - `PATCH_PLAN`
  - `PATCH_DIFF`
- Must match `patch_output.template.md`.

### Forbidden actions
- No source/business code modifications.
- No file writes outside SSOT placeholder path(s) and `ai/ai-change-gates/**`.
- No extra narrative outside the two required blocks.

### Quality bar
- `PATCH_PLAN` must list explicit target files.
- `PATCH_DIFF` must be unified diff and include run record creation.
- Constraints line must explicitly state allowed path boundaries.

### Handoff artifact(s)
- Patch plan/diff text output.
- Reference to run record json path in `ai/ai-change-gates/runs/...`.

## C) Implement (Role C)

### Purpose
Implement approved changes with explicit assumptions, tests, proof, and auditable references.

### Inputs (allowed sources)
- Approved Navigator + Patch outputs.
- SSOT/governance state.
- Explicit implementation scope from maintainer.

### Outputs (exact format)
- Exactly four sections in order:
  - `ASSUMPTIONS`
  - `TEST_PLAN`
  - `CODE_DIFF`
  - `PROOF`
- Must match `implement_output.template.md`.

### Forbidden actions
- No output outside the four required sections.
- No missing `TEST_PLAN` before `CODE_DIFF`.
- No unverifiable “done” claims without proof references.

### Quality bar
- Prefer fail-first tests for correctness-sensitive changes.
- `PROOF` must reference run record or equivalent artifact path.
- `CODE_DIFF` may be `NO_CODE_CHANGES` only when implementation is intentionally skipped.

### Handoff artifact(s)
- Implement output doc (fixed format).
- Proof references including `run_record` path.
