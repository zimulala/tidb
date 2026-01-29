# Tracks

Tracks are reusable “domain packs” that extend the core AI-assisted Change Protocol.
They do NOT change governance. They only add domain templates, gates, and prompt helpers.

## Why tracks exist
Some project types repeatedly fail in predictable ways:
- Observability pipelines: silent drops, schema drift, correlation ambiguity
- Resource accounting: semantic coupling, compatibility, high-cardinality pressure
- Performance changes: regression risk, benchmarking ambiguity

Tracks encode the minimum additional artifacts to prevent these failures.

## How to use a track
1) Pick the closest track (or none).
2) During Phase 2:
- Fill the track’s templates (in addition to core templates like SEMANTIC_SPEC.md / DATA_PATH_MAP.md).
- Apply track-specific gates.
3) During Phase 3D:
- Use the track’s review prompt for stricter, domain-aware review.

## Track selection rule
Use `observability-pipeline/` if the main risk is:
- semantic ambiguity + silent drops + evidence correlation / SOP routing
- schema drift or capability negotiation across producer/transport/consumer
- multiple paths where “missing field / missing key / timeout” fallback behavior matters

Use a TopRU-like / resource-observability track if the main risk is:
- resource accounting correctness + compatibility + high-cardinality aggregation

Available resource-observability track:
- tracks/resource-observability-topru/

Reverse exclusion rule (avoid over-design):
- If your change is only an internal refactor and does NOT change the data flow, schema, drop policy, timing semantics, or SOP,
  do NOT use `observability-pipeline` (track = none).
