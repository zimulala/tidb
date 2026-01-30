TL: TL1 or TL3


Task:
Validate that design + scope are implementable and verifiable.
Produce PHASE_3B_VALIDATION_RECORD.md.


Rules:
- If verification plan is weak, propose improvements.
- If ambiguity exists, STOP.
- Gate checks are mandatory (hard STOP on failure).


Input:
<PASTE PHASE_2_DESIGN_RECORD.md>


Output:
<PHASE_3B_VALIDATION_RECORD.md filled>

Gate checks:
- Assumption Closure Gate:
  - Review templates/ASSUMPTIONS_REGISTER.md and list any Open A#.
  - If any Open exists ⇒ STOP (do not enter Phase 3C).
- Semantic GO Gate:
  - Review templates/SEMANTIC_SPEC.md and list semantic decisions + whether maintainer GO is recorded.
  - If any semantic decision lacks explicit GO ⇒ STOP.
- Evidence readiness:
  - List what evidence artifacts will be produced in Phase 3D (tests/bench/metrics/logs) and how they will be recorded in templates/EVIDENCE_INDEX.md.
- Pipeline artifacts (if applicable):
  - If templates/DATA_PATH_MAP.md is required, confirm it is filled and coherent; otherwise STOP.
