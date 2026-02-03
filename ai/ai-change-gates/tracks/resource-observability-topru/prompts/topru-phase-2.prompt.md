TL: TL1

Task:
For a TopRU-like resource observability feature, produce/verify the additional resource-observability-topru track artifacts.

Rules:
- Do not invent missing details.
- Follow governance gates:
  - contracts/assumption-closure-gate.md
  - contracts/semantic-go-gate.md
- If any semantic decision requires maintainer GO and no GO plan exists ⇒ STOP.

Input:
<PASTE PHASE_0_INTAKE.md + PHASE_1_RISK_REGISTER.md + PHASE_2_DESIGN_RECORD.md (+ relevant design doc excerpts)>

Output:
- Filled tracks/resource-observability-topru/templates/TOPRU_SEMANTIC_SPEC.md
- Filled tracks/resource-observability-topru/templates/TOPRU_COMPAT_SPEC.md
- Filled tracks/resource-observability-topru/templates/TOPRU_DROP_SPEC.md
- A short list of semantic decisions requiring maintainer GO
- STOP section if needed
