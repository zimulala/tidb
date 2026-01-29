# TOPRU_DROP_SPEC.md

> Purpose: explicitly document all drop/backpressure behaviors and require observability (no silent drops beyond accepted policy).

## Drop / aggregation points (must enumerate)
- Collection-time drops:
  - invalid delta (<=0 / nil):
  - missing digests / missing meta:
- In-memory caps:
  - key cap policy (session-level / aggregator-level):
  - TopN bounds:
- Channel/backpressure drops:
  - collector channel full:
  - reporter send/backpressure:

## Policy per drop point
For each drop/aggregation point, specify:
- What gets dropped/aggregated:
- Deterministic policy (Yes/No):
- If aggregated: “others” semantics:
- User-visible signals:
  - metrics (names + labels):
  - logs (signatures):
- Evidence required to validate behavior (Evidence IDs):

## No-silent-loss assertion
- Explicitly list any intentional silent loss (should be rare):
- Everything else must have observability signals.

## Maintainer GO (required)
- [ ] Approved by maintainer: <name/date>
- Notes:
