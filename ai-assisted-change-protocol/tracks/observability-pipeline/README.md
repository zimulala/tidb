# Track: observability-pipeline

This track extends the core protocol for pipeline-like features where the system must be diagnosable end-to-end.

## Required artifacts
Core (mandatory):
- templates/SEMANTIC_SPEC.md
- templates/ASSUMPTIONS_REGISTER.md
- templates/DATA_PATH_MAP.md
- templates/EVIDENCE_INDEX.md (if making Verified-by-test claims)

Track (additional):
- tracks/observability-pipeline/templates/SCHEMA_SPEC.md
- tracks/observability-pipeline/templates/SOP_SPEC.md
- tracks/observability-pipeline/templates/CAPABILITY_MATRIX.md
- tracks/observability-pipeline/templates/DROP_METRICS_SPEC.md

## Track gates
Phase 2 gate additions:
- Schema Spec marks required vs optional fields and versioning plan.
- Drop metrics spec defines where drops can happen and how they’re observable.
- SOP spec defines who consumes signals and routing rules.
- Capability matrix defines old/new compatibility expectations.

Phase 3D review additions:
- Check silent drop paths: every drop/degrade must have user-visible signals.
- Check correlation: time window + keys + ambiguity handling is explicit.
- Check schema evolution: additive changes, compatibility, default/fallback behavior.

## What this track does NOT change
- Trust Levels, Evidence Policy, STOP rules, and gate contracts remain the same as core.
- This track only adds domain artifacts + extra checks.
