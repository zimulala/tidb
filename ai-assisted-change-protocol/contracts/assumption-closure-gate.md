# Assumption Closure Gate

## Purpose
Assumptions are allowed early to unblock exploration, but must be closed before Phase 3C execution or merge.

## Assumption lifecycle
Every assumption must be in exactly one state:
- Open
- Verified (with evidence)
- Rejected (design updated accordingly)
- Accepted-Risk (explicit maintainer sign-off + mitigation + monitoring)

## Gate rule (hard)
- You MUST NOT enter Phase 3C execution if any assumption is Open.
- You MUST NOT merge if any assumption is Open.

## Recording
All assumptions must be registered in templates/ASSUMPTIONS_REGISTER.md.

## Evidence requirement
To mark an assumption Verified, you must reference a concrete artifact (Evidence ID in templates/EVIDENCE_INDEX.md):
- unit/integration test output
- benchmark report
- logs/metrics/traces with time range + version + query key
- reproducible repro steps

## STOP format
If the gate fails, output:
- STOP reason
- open assumptions list
- what evidence/decision is needed (max 5 questions)
