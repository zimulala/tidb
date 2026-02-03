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
Status: NO_GO
Chosen: TODO
Options:
- finish-only
- in-flight 1s delta sampling
Impact:
- TODO
Fallback: TODO
Signals: TODO
Approved: TODO_NAME @ TODO_DATE
Evidence: TODO_E#

## S2: ExecCount semantics
Status: NO_GO
Chosen: TODO
Options:
- begin-based
- finish-based
Impact:
- TODO
Fallback: TODO
Signals: TODO
Approved: TODO_NAME @ TODO_DATE
Evidence: TODO_E#

## S3: SQL/Plan meta when TopRU enabled
Status: NO_GO
Chosen: TODO
Options:
- digest-only
- register SQL meta
- register SQL+Plan meta
Impact:
- TODO
Fallback: TODO
Signals: TODO
Approved: TODO_NAME @ TODO_DATE
Evidence: TODO_E#

## S4: disable behavior
Status: NO_GO
Chosen: TODO
Options:
- no-op (do not record)
- drop on drain + metric
Impact:
- TODO
Fallback: TODO
Signals: TODO
Approved: TODO_NAME @ TODO_DATE
Evidence: TODO_E#

## S5: interval coupling
Status: NO_GO
Chosen: TODO
Options:
- coupled (min interval)
- separated tickers
Impact:
- TODO
Fallback: TODO
Signals: TODO
Approved: TODO_NAME @ TODO_DATE
Evidence: TODO_E#
<!-- NAVIGATOR:END AUTO_DECISIONS -->

## Notes (human)
- TODO
