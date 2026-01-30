Issue 1: Subscriber Isolation (state.go)

Validation Result: PASS

Phase 3B Approved Design:
•  Replace global enableTopRU boolean with reference-counted ruConsumerCount
•  TopRU enabled iff ruConsumerCount > 0
•  EnableTopRU(): increment counter
•  DisableTopRU(): decrement counter, reset report interval on last subscriber exit
•  Report interval follows "smaller interval prevails" CAS semantics

Implementation Verification:

| Requirement | Implemented | Location |
|-------------|-------------|----------|
| ruConsumerCount as *atomic.Int64 | ✓ | state.go:47, 70 |
| EnableTopRU() increments counter | ✓ | state.go:98-100 — uses Inc() |
| DisableTopRU() decrements with CAS | ✓ | state.go:109-125 — CAS loop with defensive guard |
| Reset interval on last subscriber exit | ✓ | state.go:117-119 — calls ResetTopRUReportInterval() when current == 1 |
| TopRUEnabled() returns count > 0 | ✓ | state.go:133-135 |
| "Smaller interval prevails" CAS | ✓ | state.go:144-157 — CAS loop with intervalSeconds >= current check |

Invariants Preserved:
•  ruConsumerCount ≥ 0: Enforced by defensive guard at line 112-115 (checks current <= 0 before decrement)
•  TopRUEnabled() reflects ruConsumerCount > 0: Direct implementation at line 134

Call Sites (pubsub.go):
•  EnableTopRU() called at subscription start: line 101
•  DisableTopRU() called in defer block: lines 136-138
•  Assumptions A1/A2 (exactly-once calls) satisfied by defer-guaranteed cleanup



Issue 2: Pre-TopN Memory Bounding (ru_datamodel.go)

Validation Result: PASS

Phase 3B Approved Design:
•  maxPreTopNUsers = 2 × maxTopUsers (400)
•  maxPreTopNSQLsPerUser = 2 × maxTopSQLsPerUser (400)
•  Pre-aggregated "others" fields
•  Bounds checks with early eviction to "others"
•  Merge of pre-aggregated others with TopN-evicted at report time

Implementation Verification:

| Requirement | Implemented | Location |
|-------------|-------------|----------|
| maxPreTopNUsers = 400 | ✓ | ru_datamodel.go:42 |
| maxPreTopNSQLsPerUser = 400 | ✓ | ru_datamodel.go:43 |
| othersUser *userRUCollecting field | ✓ | ru_datamodel.go:264 |
| othersRec *ruRecord field | ✓ | ru_datamodel.go:157 |
| User cap check in ruCollecting.add() | ✓ | ru_datamodel.go:283-291 |
| SQL cap check in userRUCollecting.add() | ✓ | ru_datamodel.go:180-188 |
| Merge pre-aggregated others at report | ✓ | ru_datamodel.go:355-359 (users), ru_datamodel.go:215-219 (SQLs) |
| Evicted users merged to others | ✓ | ru_datamodel.go:361-380 |
| Evicted SQLs merged to others | ✓ | ru_datamodel.go:221-229 |
| Evicted user's othersRec also merged | ✓ | ru_datamodel.go:373-378 |

Invariants Preserved:
•  len(c.users) ≤ maxPreTopNUsers: Enforced by check at line 283 before adding new user
•  len(u.records) ≤ maxPreTopNSQLsPerUser: Enforced by check at line 180 before adding new SQL
•  No unbounded memory growth: All overflow routes to pre-aggregated "others" buckets

RU Conservation (A7):
•  All overflow data is merged into "others" — no silent drops
•  totalRU accumulation occurs even when merging to others (lines 186, 289)



Issue 3: exec_count Source Correctness

Validation Result: PASS (Accepted Limitation)

Per Phase 3B validation: This is explicitly out of Phase 3 scope. No code changes required. The limitation is documented and accepted (A8, A9).



Phase 1/2 Interface Preservation

| Check | Result |
|-------|--------|
| Phase 1 interfaces modified | NO — state.go added new functions but did not modify existing TopSQL interfaces |
| Phase 2 semantics altered | NO — TopN filtering logic unchanged; pre-TopN bounding is additive |
| New global coupling introduced | NO — Reference counting is self-contained within GlobalState |



Concurrency Assessment

Lifecycle Correctness:
•  ruConsumerCount uses atomic.Int64 with CAS operations — thread-safe
•  Defensive guard prevents negative count (line 112-115)
•  Reset only on last subscriber (atomic check current == 1 inside CAS success)

Memory Safety:
•  Pre-TopN caps are checked before map insertion — no race between check and insert within single-threaded add calls
•  take() swaps maps atomically for reporter consumption (lines 307-315)



Silent Data Loss Assessment

| Scenario | Behavior | Documented |
|----------|----------|------------|
| User overflow (> 400) | Merged to othersUser | Yes (Phase 3B A7) |
| SQL overflow (> 400/user) | Merged to othersRec | Yes (Phase 3B A7) |
| TopN eviction | Merged to "others" buckets | Yes (Phase 2 design) |

No silent data loss beyond documented behavior.



Final Decision

MERGE APPROVED

All Phase 3B validated designs have been implemented exactly as specified:
1. Issue 1: Reference-counted subscriber isolation — complete
2. Issue 2: Pre-TopN memory bounding with 2× caps and "others" aggregation — complete
3. Issue 3: Accepted limitation documented — no changes required
