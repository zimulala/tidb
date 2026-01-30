# Phase 3B Validation Record

Scope:
- Based on phase_3_issues.md ("Design-Fixed Issues" section)
- Phase: Phase 3B — Validation Gate (Design-Fixed, Pre-Implementation)

Rules:
- No alternative designs allowed
- No new features introduced
- Validation only; implementation requires explicit GO

---

## Issue 1: Subscriber Isolation — enable_topru lifecycle

Validation Result: APPROVED AS-IS

Validated Design:
- Replace global enableTopRU boolean with reference-counted ruConsumerCount
- TopRU enabled iff ruConsumerCount > 0
- EnableTopRU(): increment counter
- DisableTopRU(): decrement counter, reset report interval on last subscriber exit
- Report interval follows "smaller interval prevails" CAS semantics

Validated Files / Functions:
- pkg/util/topsql/state/state.go
  - GlobalState struct initialization
  - State struct
  - EnableTopRU()
  - DisableTopRU()
  - TopRUEnabled()
  - SetTopRUReportInterval()
  - ResetTopRUReportInterval()
- pkg/util/topsql/reporter/pubsub.go
  - Existing call sites validated; no modification required

Assumptions Accepted:
- A1: Each subscriber calls EnableTopRU() exactly once
- A2: Each subscriber calls DisableTopRU() exactly once (defer-guaranteed)
- A3: "Smaller interval prevails" semantics are correct and intentional
- A4: Resetting interval on last subscriber exit is required to avoid stale global state

Invariants:
- ruConsumerCount ≥ 0 at all times
- TopRUEnabled() reflects ruConsumerCount > 0

Blocking Conditions:
- None

---

## Issue 2: Reporter Pre-TopN Memory Bounding

Validation Result: APPROVED WITH DOCUMENTED ASSUMPTIONS

Validated Design:
- Introduce pre-TopN caps during collection phase
  - maxPreTopNUsers = 2 × maxTopUsers
  - maxPreTopNSQLsPerUser = 2 × maxTopSQLsPerUser
- When caps exceeded:
  - Merge new entries into pre-aggregated "others user" / "others SQL"
- At report time:
  - Merge pre-aggregated "others" with TopN-evicted entries
- Ensure bounded memory usage regardless of cardinality spikes

Validated Files / Functions:
- pkg/util/topsql/reporter/ru_datamodel.go
  - ruCollecting (struct, add, take, getReportRecords)
  - userRUCollecting (struct, add, getReportRecords)
  - New constants for pre-TopN caps

Assumptions Accepted:
- A5: 2× TopN limits provide sufficient headroom before early eviction
- A6: Pre-aggregated "others" uses nil digests, consistent with report-time semantics
- A7: RU conservation holds — no silent drops; all overflow merges into "others"

Invariants:
- len(c.users) ≤ maxPreTopNUsers during collection
- len(u.records) ≤ maxPreTopNSQLsPerUser during collection
- No unbounded memory growth prior to TopN filtering

Blocking Conditions:
- None (assumptions documented and accepted)

---

## Issue 3: Verification-Only — exec_count Source Correctness

Validation Result: ACCEPTED AS OUT-OF-SCOPE LIMITATION

Validated Understanding:
- Phase 2 design specifies exec_count increments at execution begin
- Current code shows:
  - finishedRUBuffer exists
  - No visible executor-level integration populating it
  - TODO(M4) explicitly marks executor integration as future work

Decision:
- exec_count correctness depends on executor integration (M4)
- This is explicitly out of Phase 3 scope
- No correctness regression introduced by Phase 2 / Phase 3 changes

Action:
- Document limitation
- No code changes required in Phase 3

Assumptions Accepted:
- A8: exec_count accuracy requires executor-layer hooks
- A9: Missing exec_count population is an accepted limitation, not a Phase 3 bug

---

## Phase 3B Decision

Decision: GO

Approved Issues:
- Issue 1: Subscriber Isolation — enable_topru lifecycle
- Issue 2: Reporter Pre-TopN Memory Bounding
- Issue 3: exec_count Verification (accepted limitation)

Notes:
- Implementation may proceed strictly according to validated designs
- Any deviation requires reopening Phase 3B

