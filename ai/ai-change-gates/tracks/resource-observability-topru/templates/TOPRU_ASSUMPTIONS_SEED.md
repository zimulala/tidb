# TOPRU_ASSUMPTIONS_SEED.md

> Purpose: provide a reusable seed list of TopRU high-risk assumptions.
> Usage: copy the rows below into templates/ASSUMPTIONS_REGISTER.md for any TopRU-like work.
> All are Open by default; close via evidence or explicit maintainer acceptance.

| ID | Assumption | Impact if false | Closure requirement | Links |
|----|------------|-----------------|---------------------|-------|
| A1 | TopRU must support in-flight 1s delta sampling (align with design doc). | If false, near-real-time triage for long-running SQL is weakened; design/implementation drift causes repeated review churn. | Maintainer GO on S1 + design doc alignment (choose A/B) + tests verifying delta sampling behavior (no double count, abnormal exit handling). | S1 |
| A2 | ExecCount definition (begin vs finish) and its coupling/decoupling with RU/Duration is acceptable and documented. | If false, users observe ExecCount skew (e.g. RU>0 but count=0), causing semantic confusion and incorrect downstream interpretation. | Maintainer GO on S2 + unit tests covering long-running SQL, RU=0 behavior, and window/cadence interaction; update user-facing docs/notes. | S2 |
| A3 | When TopSQL is disabled but TopRU is enabled, SQL/Plan metadata availability is sufficient for usability (at least SQLMeta). | If false, digests become hard to interpret, reducing SOP effectiveness and making diagnostics non-actionable. | Maintainer GO on S6 + implementation plan (TopProfilingEnabled gate) + tests verifying SQLMeta/PlanMeta emission/fallback. | S6 |
| A4 | Report interval coupling between TopRU and TopSQL cadence is acceptable (or explicitly separated) and does not create unexpected overhead/regressions. | If false, enabling TopRU changes TopSQL cadence unexpectedly or causes performance/traffic regressions, leading to rollback risk. | Maintainer GO on S5 + tests (cadence behavior) + evidence for overhead (bench/profile/metrics). | S5 |
