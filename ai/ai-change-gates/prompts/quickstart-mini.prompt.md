TL: TL1

Task:
Given templates/CHANGE_BRIEF_MINI.md:
1) First expand it into a draft templates/CHANGE_BRIEF.md (do NOT invent missing details).
2) Then run the same outputs as quickstart to produce drafts for:
   - templates/PHASE_0_INTAKE.md
   - templates/PHASE_1_RISK_REGISTER.md
   - templates/PHASE_2_DESIGN_RECORD.md
   - templates/PHASE_3B_VALIDATION_RECORD.md
   - templates/FAILURE_ASSUMPTIONS.md
   - templates/ASSUMPTIONS_REGISTER.md
   - templates/SEMANTIC_SPEC.md
   - templates/DATA_PATH_MAP.md (ONLY if pipeline-like)

Activation rule:
- Only run this flow if the user explicitly says: 半自动化功能实现
- If not activated: respond normally (no forced protocol artifacts).

Rules:
- Do not invent missing details.
- If key details are missing, STOP and ask up to 5 questions.
- All conclusions must be labeled as Design-level assessment or Assumption-based.

Track classifier (optional, non-blocking):
- Recommend track + reasons + extra artifacts, but maintainer can override.
- Reverse exclusion rule: internal refactor with no data-flow/schema/drop/timing/SOP change ⇒ recommend "none".

Governance gates (must be surfaced in output):
- Assumption Closure Gate: list assumptions in ASSUMPTIONS_REGISTER.md with closure requirements; mark which must close before Phase 3C.
- Semantic GO Gate: list semantic decisions in SEMANTIC_SPEC.md; mark GO-required ones.
- SEMANTIC_SPEC completeness requirement:
  - For each major area (toggles/timing/drop/compat/privacy), include:
    - Default behavior / fallback
    - User-visible signals (metric/log/trace)
- Evidence readiness + immutability:
  - Plan Evidence IDs for Phase 3D in EVIDENCE_INDEX.md.
  - Evidence must bind commit hash + time + environment + command/params.
  - Diff change invalidates evidence; new Evidence ID required.

Input:
<PASTE templates/CHANGE_BRIEF_MINI.md>

Output:
1) Draft templates/CHANGE_BRIEF.md
2) Paste-ready drafts for Phase 0/1/2/3B + FAILURE_ASSUMPTIONS + ASSUMPTIONS_REGISTER + SEMANTIC_SPEC (+ DATA_PATH_MAP if needed)
3) Track recommendation (optional) + maintainer override line
4) Gate checklist (explicit) + STOP section if needed
