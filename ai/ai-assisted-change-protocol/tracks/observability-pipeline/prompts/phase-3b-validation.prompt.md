TL: TL1 or TL3

Task:
Validate that the observability pipeline track artifacts are coherent, gated, and verifiable.
Produce PHASE_3B_VALIDATION_RECORD.md (plus track-specific notes).

Rules:
- Gate checks are mandatory (hard STOP on failure).

Input:
<PASTE PHASE_2_DESIGN_RECORD.md + templates/ASSUMPTIONS_REGISTER.md + templates/SEMANTIC_SPEC.md + templates/DATA_PATH_MAP.md>

Output:
- <PHASE_3B_VALIDATION_RECORD.md filled>
- Track notes:
  - Schema drift risks and mitigation
  - Silent drop risks and required observability
  - Correlation ambiguity risks

Gate checks:
- Assumption Closure Gate: any Open A# ⇒ STOP
- Semantic GO Gate: missing maintainer GO ⇒ STOP
- Track artifacts: SCHEMA_SPEC / SOP_SPEC / CAPABILITY_MATRIX / DROP_METRICS_SPEC must be present and non-empty
- Evidence readiness: list expected Evidence IDs for Phase 3D
