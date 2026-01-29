# Track: resource-observability-topru

This track extends the core protocol for resource accounting / observability features in the style of TopRU/TopSQL.
It is designed to prevent semantic drift and review failures commonly seen in TopRU-like work (sampling semantics, exec_count coupling, meta completeness, drop/backpressure semantics).

## Required artifacts
Core (mandatory):
- templates/SEMANTIC_SPEC.md
- templates/ASSUMPTIONS_REGISTER.md
- templates/EVIDENCE_INDEX.md (if making Verified-by-test claims)

Track (additional):
- tracks/resource-observability-topru/templates/TOPRU_SEMANTIC_SPEC.md
- tracks/resource-observability-topru/templates/TOPRU_COMPAT_SPEC.md
- tracks/resource-observability-topru/templates/TOPRU_DROP_SPEC.md

## Track gates
Phase 2 gate additions:
- Sampling semantics are explicit and testable (e.g. “1s delta sampling” vs finish-path semantics).
- exec_count semantics are explicitly defined and checked for coupling/decoupling risk with TotalRU/ExecDuration.
- Compatibility is explicit for old clients / Unimplemented behavior / PubSub vs SingleTarget parity.
- Drop/backpressure semantics are explicit and observable (no undocumented silent drops).

Phase 3D review additions:
- Check semantic drift vs design doc (sampling cadence, delta definition, time/window alignment).
- Check meta completeness when TopRU is enabled (SQL/Plan meta availability and fallback strategy).
- Check drop paths are documented and have signals (metrics/logs) and evidence.

## What this track does NOT change
- Trust Levels, Evidence Policy, STOP rules, and gate contracts remain the same as core.
- This track only adds domain artifacts + extra checks.
