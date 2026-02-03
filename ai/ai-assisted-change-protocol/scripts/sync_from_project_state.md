# sync_from_project_state (spec)

## Purpose
Single source of truth is PROJECT_STATE.md SSOT_V2.
This tool generates or verifies display-only documents from SSOT to avoid manual sync drift.

## Inputs
- project: path to a project workspace (e.g., ai/projects/topru-ai)
- required: <project>/PROJECT_STATE.md containing SSOT_V2 block

## Outputs (display-only)
Generate these files under <project>/ (unless disabled by flags):
1) ASSUMPTIONS_REGISTER.md
    - Derived from SSOT claims where legacy includes A* or claim type=assumption (if present).
2) EVIDENCE_INDEX.md
    - Derived from SSOT evidence list.
3) PHASE_3D_REVIEW_REPORT.md
    - Derived from SSOT findings list (+ optional summary header).
4) OPTIONAL: SEMANTIC_SPEC.md / TOPRU_SEMANTIC_SPEC.md
    - Derived from SSOT claims that map to legacy S* (if you keep them for human readability).

## Modes
- generate (default): overwrite the display files to match SSOT
- check: verify display files match SSOT (CI friendly); exit non-zero on mismatch

## Parsing rules
- Read SSOT_V2 block only:
  between <!-- NAVIGATOR:BEGIN SSOT_V2 --> and <!-- NAVIGATOR:END SSOT_V2 -->
- SSOT format is YAML-like; treat it as a structured block. (Implement a simple parser or require strict key=value format in V3.)

## Evidence rules
- Evidence must be commit-bound:
  commit + time + env + command + artifact
- If record_patch_id is enabled in SSOT policy:
  patch_id must be present too.

## Deterministic output
- Generated files must use stable ordering:
    - claims sorted by id
    - findings sorted by id
    - evidence sorted by id
- Do not include timestamps in generated files (except where SSOT explicitly contains them), to keep diffs stable.

## CLI
sync_from_project_state --project <path> [--mode generate|check] [--out assumptions,evidence,review] [--dry-run]

## Exit codes
- 0: success
- 2: SSOT missing/invalid
- 3: check mode mismatch detected
- 4: IO error

## Recommended CI usage
- Run in check mode:
  sync_from_project_state --project ai/projects/topru-ai --mode check
- Fail PR if mismatch to enforce SSOT-only editing.
