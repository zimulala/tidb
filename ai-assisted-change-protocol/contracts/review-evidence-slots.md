# Review Evidence Slots

## Purpose
Force reviews to be evidence-driven and closeable, not “confidence-based”.

## Rule
Every review finding MUST include:
- severity
- statement of issue
- closure requirement (what evidence closes it)
- status: Open / Closed
- evidence id (if closed)

## Evidence immutability (hard)
Any evidence used to close a finding MUST bind:
- commit hash
- time
- environment (hardware/OS/runtime versions as relevant)
- command + parameters (or dashboard query)

If the reviewed diff/commit changes after evidence was collected, that evidence is stale and must not be used to close findings.
Collect new evidence with a new Evidence ID.

## Forbidden
- “PASS” without evidence
- “seems fine” as closure
- claiming tests ran without artifacts

## Minimal evidence guidance (non-exhaustive)
- correctness: unit/integration test or reproducible repro + log signature
- compatibility: old-client simulation OR explicit compatibility policy (SEMANTIC_SPEC)
- performance: benchmark/profiling signal with baseline
