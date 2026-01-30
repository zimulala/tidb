summary:
Issue 1: Subscriber Isolation (state.go) — ✅ Complete
•  Reference-counted ruConsumerCount replaces boolean
•  EnableTopRU() increments counter
•  DisableTopRU() decrements with CAS, resets interval when last subscriber exits
•  TopRUEnabled() returns ruConsumerCount > 0
•  SetTopRUReportInterval() uses "smaller interval prevails" CAS semantics

Issue 2: Pre-TopN Memory Bounding (ru_datamodel.go) — ✅ Complete
•  maxPreTopNUsers = 400 (2× TopN limit)
•  maxPreTopNSQLsPerUser = 400 (2× TopN limit)
•  Pre-aggregated othersUser and othersRec fields
•  Bounds checks in add() methods with early eviction to "others"
•  Merge of pre-aggregated others with TopN-evicted entries at report time

All invariants from PHASE_3B_VALIDATION.md are maintained:
•  ruConsumerCount ≥ 0 at all times
•  TopRUEnabled() reflects ruConsumerCount > 0
•  len(c.users) ≤ maxPreTopNUsers during collection
•  len(u.records) ≤ maxPreTopNSQLsPerUser during collection

