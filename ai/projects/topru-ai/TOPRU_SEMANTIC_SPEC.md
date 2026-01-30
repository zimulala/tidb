# TOPRU_SEMANTIC_SPEC

rules:
- machine-readable fields:
  Status: GO | NO_GO | ACCEPTED_RISK
  Chosen:
  Approved: <name> @ <YYYY-MM-DD>
  Fallback:
  Signals:
- missing required GO blocks progress (LOCK_SEMANTICS)

<!-- NAVIGATOR:BEGIN AUTO_DECISIONS -->
## S1: in-flight sampling
Status: GO
Chosen: in-flight 1s delta sampling
Options:
- finish-only
- in-flight 1s delta sampling
  Impact:
- Enables near-real-time visibility for long-running SQL by emitting RU deltas during execution.
- Requires execCtx state (LastRUSample) to avoid double count; must handle abnormal exit and final flush.
  Fallback: If sampling cannot run (e.g., missing exec context), fall back to finish-only RU attribution and mark record as degraded (implementation should avoid double count).
  Signals: Optional debug/trace logs for sampling path; unit/integration tests must verify delta correctness and no double count. (No runtime metrics required unless explicitly added later.)
  Approved: zimulala @ 2026-01-30
  Evidence: E1  # expected: E1 correctness tests; optional E2 overhead bench

## S2: ExecCount semantics
Status: GO
Chosen: begin-based
Options:
- begin-based
- finish-based
  Impact:
- Aligns with TopSQL semantics (counts execution starts).
- Can de-couple from RU/Duration across ticks/windows; consumer interpretation must be documented and validated to avoid RU>0 with count=0 confusion.
  Fallback: If begin-based produces misleading aggregates, introduce explicit record typing or adjust aggregation so begin count cannot be misinterpreted as “completed RU record”.
  Signals: Unit tests for long-running queries, toggle changes mid-execution, and RU=0 behavior; documentation note clarifying interpretation.
  Approved: zimulala @ 2026-01-30
  Evidence: E1  # expected: E1 tests for semantic consistency

## S3: SQL/Plan meta when TopRU enabled
Status: GO
Chosen: register SQL+Plan meta
Options:
- digest-only
- register SQL meta
- register SQL+Plan meta
  Impact:
- Improves usability: TopRU becomes actionable (not a digest-only black box).
- May introduce overhead for plan capture; needs lightweight policy/fallback if plan is expensive.
  Fallback: Always register SQL meta; register Plan meta with limits (rate/size) and allow “plan missing” while keeping bundle usable.
  Signals: Tests verifying TopSQL disabled + TopRU enabled still produces SQLMeta/PlanMeta (or documented fallback behavior).
  Approved: zimulala @ 2026-01-30
  Evidence: E1  # expected: E1 tests

## S4: disable behavior
Status: GO
Chosen: no-op (do not record)
Options:
- no-op (do not record)
- drop on drain + metric
  Impact:
- When TopRU is disabled, TopRU collection and reporting should be inactive; no RU should be collected or emitted.
- Explicitly avoids adding drop metrics because that would require RU collection and the requirement does not ask for it.
  Fallback: If housekeeping drains occur, they must not require RU collection and must not emit TopRU records; treat any buffered RU as non-authoritative.
  Signals: Documentation + code comments near gates; optional unit test confirming no TopRU output when disabled.
  Approved: zimulala @ 2026-01-30
  Evidence: E_TBD  # doc-only acceptable; add E1 if you write a test

## S5: interval coupling
Status: GO
Chosen: coupled (min interval)
Options:
- coupled (min interval)
- separated tickers
  Impact:
- Enabling TopRU may reduce effective report interval by taking min(TopSQL interval, TopRU interval).
- This cross-feature coupling is explicitly accepted; must be documented to avoid future “is this a bug?” reviews.
  Fallback: If coupling causes unexpected overhead, revisit to separated tickers in a follow-up change (would require new semantic GO).
  Signals: Documentation + minimal test verifying effective interval selection behavior.
  Approved: zimulala @ 2026-01-30
  Evidence: E_TBD  # E1 minimal unit test recommended
<!-- NAVIGATOR:END AUTO_DECISIONS -->

## Notes (human)
- TODO
