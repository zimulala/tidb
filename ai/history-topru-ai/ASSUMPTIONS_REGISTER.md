# Assumptions Register (TopRU)

| ID | Assumption | Impact if false | Owner | Status (Open/Verified/Rejected/Accepted-Risk) | Closure requirement | Evidence ID | Deadline |
|----|------------|-----------------|-------|----------------------------------------------|---------------------|------------|----------|
| A1 | TopRU must support in-flight 1s delta sampling (align with design doc). | If false, near-real-time triage for long-running SQL is weakened; design/implementation drift causes repeated review churn. |       | Open | Maintainer GO on S1 + design doc alignment (choose A/B) + tests verifying delta sampling behavior (no double count, abnormal exit handling). |            |          |
| A2 | ExecCount definition (begin vs finish) and its coupling/decoupling with RU/Duration is acceptable and documented. | If false, users observe ExecCount skew (e.g. RU>0 but count=0), causing semantic confusion and incorrect downstream interpretation. |       | Open | Maintainer GO on S2 + unit tests covering long-running SQL, RU=0 behavior, and window/cadence interaction; update user-facing docs/notes. |            |          |
| A3 | When TopSQL is disabled but TopRU is enabled, SQL/Plan metadata availability is sufficient for usability (at least SQLMeta). | If false, digests become hard to interpret, reducing usability and making diagnostics non-actionable. |       | Open | Maintainer GO on S6 + implementation plan (TopProfilingEnabled gate) + tests verifying SQLMeta/PlanMeta emission/fallback. |            |          |
| A4 | Report interval coupling between TopRU and TopSQL cadence is acceptable (or explicitly separated) and does not create unexpected overhead/regressions. | If false, enabling TopRU changes TopSQL cadence unexpectedly or causes performance/traffic regressions, increasing rollback risk. |       | Open | Maintainer GO on S5 + tests (cadence behavior) + evidence for overhead (bench/profile/metrics). |            |          |

## Closure rules
- Before entering Implementation or claiming “end-to-end functional”, every assumption must be either:
  - Verified (with evidence), or
  - Rejected (design updated), or
  - Accepted-Risk (explicit maintainer sign-off + mitigation + monitoring)
- If any assumption stays Open ⇒ STOP.

## Notes
- Link semantic decisions S1–S6 in topru-ai/SEMANTIC_SPEC.md.
