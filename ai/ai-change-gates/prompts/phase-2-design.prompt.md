TL: TL1


Task:
Draft PHASE_2_DESIGN_RECORD.md with frozen scope, invariants, and verification plan.


Rules:
- Every invariant must be testable or observable.
- If acceptance criteria cannot be verified, STOP and propose a verifiable plan.
- Follow governance gates:
  - contracts/assumption-closure-gate.md
  - contracts/semantic-go-gate.md


Input:
<PASTE PHASE_0_INTAKE.md + PHASE_1_RISK_REGISTER.md>


Output:
<PHASE_2_DESIGN_RECORD.md filled>
+ STOP section if necessary

Additional mandatory outputs:
- Fill templates/ASSUMPTIONS_REGISTER.md with A# items. Mark which must be closed before Phase 3C.
- Fill templates/SEMANTIC_SPEC.md. Identify semantic decisions requiring explicit maintainer GO (record GO plan if not yet granted).
- If the feature is pipeline-like (event/data pipeline), fill templates/DATA_PATH_MAP.md with concrete modules/functions/artifacts.

Gate rules:
- STOP if any semantic decision lacks an explicit maintainer GO plan.
- STOP if the feature is pipeline-like but DATA_PATH_MAP.md cannot be concretely filled.
