# ssot_patch_evidence (spec)

## Goal
Update one evidence entry inside <project>/PROJECT_STATE.md SSOT_V2 block safely and deterministically.

## Inputs
- --project <path> (e.g., ai/projects/topru-ai)
- --evidence-id <EID> (e.g., E_integ)
- --manifest <path> (e.g., ai/projects/topru-ai/artifacts/evidence/E_integ/manifest.json)
- --mode patch|print (default patch)

## Behavior
1) Read <project>/PROJECT_STATE.md
2) Extract SSOT_V2 block between:
   <!-- NAVIGATOR:BEGIN SSOT_V2 --> and <!-- NAVIGATOR:END SSOT_V2 -->
3) Parse manifest.json fields:
   - status, type, commit, patch_id, time, env, command, artifact
4) Find matching evidence item by id within SSOT_V2 evidence list:
   - If exists: update fields to match manifest (status=Captured, etc.)
   - If missing: append a new evidence item with those fields
5) Write back PROJECT_STATE.md with SSOT_V2 block updated only (no other edits)

## Constraints
- Must not modify any other SSOT sections (claims/findings/next_actions) unless explicitly requested.
- Must preserve stable formatting and ordering:
  - evidence entries sorted by id (optional but recommended)
- Exit codes:
  - 0 success
  - 2 invalid/missing SSOT
  - 3 invalid manifest
  - 4 IO error

## Notes
- Prefer Go for robustness.
- This tool enables verify scripts to be fully autonomous:
  run recipe -> generate manifest -> ssot_patch_evidence -> evidence becomes Captured automatically.

