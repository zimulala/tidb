# Semantic GO Gate

## Purpose
Prevent implicit decisions on user-visible or operational semantics.

## Semantic Decisions (must be explicit GO)
Examples:
- enable/disable behavior (drop/keep/drain semantics)
- compatibility strategy (Unimplemented handling, capability negotiation)
- backpressure/drop policy and what gets dropped
- timing semantics (intervals, windows, coupling between features)
- schema requirements (required vs optional fields)
- privacy/redaction policy
- default behavior / fallback (timeouts, missing keys, downstream unavailable)

## Mechanical Decisions (can be implicit)
Examples:
- internal refactor that does not change external behavior
- file layout, naming, minor logging format (non-breaking)
- small performance micro-optimizations ONLY if explicitly allowed by the Execution Contract

## Gate rule (hard)
No semantic decision may be “implicitly approved”.
If any semantic decision is not explicitly accepted by maintainer ⇒ STOP.

## Recording
All semantic decisions must be recorded in:
- templates/SEMANTIC_SPEC.md
and referenced in Phase 3D review.
