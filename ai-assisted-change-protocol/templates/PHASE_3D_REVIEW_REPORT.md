# Phase 3D — Review Report (TL3/TL4)


## Declared trust level
- TL3 by default
- TL4 only if Evidence Index is complete


## Review scope
- What commits / PRs:
- What modules:


## Invariant checks (AI-assisted allowed)
- INV1: <status + epistemic label>
- INV2: <status + epistemic label>


## Risk register closure
- R1: open/closed + reason + evidence id if closed
- R2: ...


## Findings
### Evidence Slots for Review (NEW)
- Every finding must include “Closure requirement”:
  - What evidence closes it (unit test / integration test / bench / metric / log / trace)?
  - If evidence is missing: status must remain Open (no “PASS by reasoning”).
- Review output format required:
  - Finding
  - Severity
  - Closure requirement
  - Evidence ID (if closed)
  - Status: Open / Closed
### Must-fix
- Finding:
  - Severity:
  - Closure requirement:
  - Evidence ID (if closed):
  - Status: Open / Closed


### Nice-to-have (do NOT sneak into scope)
- Finding:
  - Severity:
  - Closure requirement:
  - Evidence ID (if closed):
  - Status: Open / Closed

## Review checklist (must answer)
- [ ] Correctness: critical paths are exercised or justified
- [ ] Resource usage: hot paths bounded (CPU/memory/allocs)
- [ ] Concurrency: lifecycle + races considered
- [ ] Compatibility: protocol/API changes are additive or guarded
- [ ] Observability: errors/metrics/logs exist for regressions
- [ ] Tests: missing tests identified; evidence listed if run

## Failure assumptions (link to FAILURE_ASSUMPTIONS.md)
- Linked file:
- Summary of top 2–3 assumptions:

## STOP reminder
- If any checklist item is unresolved or evidence is missing for a required claim, STOP and request clarification.


## Epistemic labeling summary
- Design-level assessments:
- Assumption-based:
- Verified-by-test (must reference Evidence IDs):
  - Claim: ...
    - Evidence: E?


## Maintainer decision
- [ ] Merge
- [ ] Do not merge
- [ ] Needs more evidence
