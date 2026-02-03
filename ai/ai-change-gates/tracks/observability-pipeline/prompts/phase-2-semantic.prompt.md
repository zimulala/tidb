TL: TL1

Task:
For a pipeline-like feature, produce/verify the additional observability-pipeline track artifacts to eliminate silent drops, schema drift, and correlation ambiguity.

Rules:
- Do not invent missing details.
- Follow governance gates:
  - contracts/assumption-closure-gate.md
  - contracts/semantic-go-gate.md
- If required artifacts cannot be filled concretely, STOP.

Input:
<PASTE PHASE_0_INTAKE.md + PHASE_1_RISK_REGISTER.md + PHASE_2_DESIGN_RECORD.md>

Output:
- Filled templates/DATA_PATH_MAP.md
- Filled tracks/observability-pipeline/templates/SCHEMA_SPEC.md
- Filled tracks/observability-pipeline/templates/SOP_SPEC.md
- Filled tracks/observability-pipeline/templates/CAPABILITY_MATRIX.md
- Filled tracks/observability-pipeline/templates/DROP_METRICS_SPEC.md
- STOP section if needed
