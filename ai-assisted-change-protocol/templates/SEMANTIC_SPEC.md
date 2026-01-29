# Semantic Spec

> Goal: make user-visible / operational semantics explicit and reviewable (no implicit approval).
> Any semantic decision requires explicit maintainer GO (see contracts/semantic-go-gate.md).

## Feature flags / toggles
- Flag:
  - Enabled behavior:
  - Disabled behavior:
  - Default behavior / fallback:
  - User-visible signals (metrics/logs/traces):
  - Failure mode:

## Correlation semantics (keys + time_window)
- Required correlation keys (must be stable):
- time_window definition:
  - source of time (event time / observation time):
  - rounding/alignment rule:
  - drift/jitter handling:
- plan_digest availability:
  - when can plan_digest be missing?
  - behavior if missing (drop/degrade/mark unknown):
- Default behavior / fallback:
- User-visible signals (metrics/logs/traces):

## Timing semantics (sampling vs reporting vs window)
- sampling interval (if any):
- report interval / window:
- coupling with other features (Yes/No):
- if coupled: explicit acceptance + rationale:
- interval/window change semantics (if supported):
- Default behavior / fallback:
- User-visible signals (metrics/logs/traces):

## Drop / Backpressure policy
- What can be dropped:
- Deterministic policy (required if reproducibility matters):
- Drop observability (metrics/logs) required:
- Default behavior / fallback:
- User-visible signals (metrics/logs/traces):

## Compatibility
- Old client behavior:
- strategy: ignore Unimplemented / capability detect / hard require upgrade:
- symmetric behavior across paths (must be documented):
- Default behavior / fallback:
- User-visible signals (metrics/logs/traces):

## Privacy / redaction
- SQL text / plan / params:
- redaction policy:
- User-visible signals (auditability):

## Maintainer GO (required for semantic decisions)
- [ ] Approved by maintainer: <name/date>
- Notes:
