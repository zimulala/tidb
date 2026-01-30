TL: TL1

Task:
Given templates/CHANGE_BRIEF.md, produce drafts to paste into:
- templates/PHASE_0_INTAKE.md
- templates/PHASE_1_RISK_REGISTER.md
- templates/PHASE_2_DESIGN_RECORD.md
- templates/PHASE_3B_VALIDATION_RECORD.md
- templates/FAILURE_ASSUMPTIONS.md (draft)
- templates/ASSUMPTIONS_REGISTER.md (draft)
- templates/SEMANTIC_SPEC.md (draft)
- templates/DATA_PATH_MAP.md (draft; ONLY if pipeline-like)

Rules:
- Do not invent missing details.
- If key details are missing, STOP and ask up to 5 questions.
- All conclusions must be labeled as:
  - Design-level assessment, or
  - Assumption-based (list assumptions).

Track classifier (optional, non-blocking):
- Recommend a track to load (or "none") based on the change type.
- Output:
  - Recommended track: none / observability-pipeline / other
  - Why (2–3 bullets)
  - Extra required templates/artifacts if the track is used
- Reverse exclusion rule:
  - If the change is only an internal refactor and does not change any data flow / schema / drop policy / SOP,
    recommend "none" (avoid over-design).
- This recommendation must never block the core protocol; the maintainer can override.

Governance gates (must be surfaced in output):
- Assumption Closure Gate:
  - Register assumptions in templates/ASSUMPTIONS_REGISTER.md with Closure requirement.
  - Mark which must be closed before Phase 3C.
- Semantic GO Gate:
  - Record semantic decisions in templates/SEMANTIC_SPEC.md.
  - If any semantic decision cannot be resolved without maintainer GO, mark it as "GO required".
- Evidence readiness + immutability (planning only at TL1):
  - If any later Phase 3D claim is intended to be "Verified-by-test", it must reference an Evidence ID in templates/EVIDENCE_INDEX.md.
  - Evidence must bind commit hash + time + environment + command/params.
  - If diff changes after evidence collection, evidence is stale and must not be used to close review findings.

Input:
<PASTE templates/CHANGE_BRIEF.md>

Output:
1) Drafts for the required templates (paste-ready).
2) Track recommendation (optional, non-blocking):
   - recommended track:
   - why:
   - extra required artifacts:
   - maintainer override (explicit): use recommended / use none / use other
3) Gate checklist (explicit):
   - Open assumptions (A#) + closure requirement
   - Semantic decisions requiring GO + current status (GO obtained? / GO plan?)
   - Pipeline-like? If yes: DATA_PATH_MAP.md must be filled; if no: explain why not
   - Planned Evidence IDs (E#) for Phase 3D (what will be produced + how)
4) STOP section if needed (max 5 questions).
