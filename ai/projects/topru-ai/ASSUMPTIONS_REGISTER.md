# Assumptions Register

rules:
- each assumption is an A#
- Status must be one of: Open | Verified | Rejected | Accepted-Risk
- Open assumptions MUST block implementation (CLOSE_ASSUMPTIONS)
- Verified must link to commit-bound evidence (E#)

<!-- NAVIGATOR:BEGIN AUTO_ASSUMPTIONS -->

| ID | Assumption | Impact if false | Closure requirement | Links |
|----|------------|-----------------|---------------------|-------|
| A1 | In-flight 1s delta sampling can be implemented without double count and with safe edge-case handling. | Long-running SQL RU visibility is wrong; repeated review churn due to design/impl mismatch; potential double count bugs. | S1 is GO. Close via: implement execCtx + LastRUSample + 1s tick + final flush; add tests proving (a) long query yields >=N samples, (b) sum(delta) matches final within tolerance, (c) abnormal exit/cancel does not double count or leak state. | S1, E1 |
| A2 | Begin-based ExecCount remains interpretable and does not produce misleading aggregates with RU/Duration. | Users see confusing records (e.g., RU>0 but count=0), breaking downstream interpretation and SOP. | S2 is GO. Close via: document semantics (begin-count vs RU attribution) + add tests covering long query + drain/tick behavior + toggle changes + RU=0 behavior; ensure output is either consistent or explicitly marked per policy. | S2, E1 |
| A3 | When TopSQL is disabled but TopRU enabled, registering SQL/Plan meta is feasible and does not break performance/limits. | Digest-only output becomes non-actionable; usability drops. | S3 is GO. Close via: change gate to TopProfilingEnabled for meta registration + define fallback limits for plan meta + tests verifying SQLMeta/PlanMeta presence (or documented fallback). | S3, E1 |
| A4 | Interval coupling (min interval) is acceptable and does not cause unacceptable overhead. | Enabling TopRU unexpectedly increases TopSQL cadence/traffic; regression/rollback risk. | S5 is GO. Close via: doc comment + minimal test verifying coupling behavior + overhead evidence (bench/profile/metrics) on representative workload. | S5, E1, E2 |

<!-- NAVIGATOR:END AUTO_ASSUMPTIONS -->

## Notes (human)
- TODO
