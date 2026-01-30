# Phase 3 Validation Plan — TopRU

## 0. Scope & Freeze Point

This document defines the validation plan for Phase 3 of the TopRU project.

- Phase 2 implementation is feature-complete and frozen.
- No new features, APIs, or protocol changes are allowed in Phase 3.
- Phase 3 focuses exclusively on correctness, scalability, performance, and operational safety.
- All validation is performed against the current Phase 2 codebase.

---

## 1. System Design Invariants

The following invariants must hold for TopRU to be considered correct and production-ready.

### Invariant 1 — RU Conservation within a Reporting Window
Within a single report_interval window, total RU consumption must be conserved,
except for intentional aggregation into "others user" or "others SQL".

Loss of RU is acceptable only due to explicit backpressure policies.

### Invariant 2 — Bounded Memory Usage
Memory usage must be bounded regardless of input cardinality.

Specifically:
- The number of distinct RU keys held by the aggregator must not exceed the configured hard cap (e.g. 10,000).
- Exceeding the cap must not cause unbounded allocations or OOM.

### Invariant 3 — Correct In-flight Execution Accounting
- exec_count must be incremented exactly once per execution, at execution begin.
- deltaRU may be accumulated during execution and at execution finish.
- Partial executions must not cause double counting or negative deltas.

### Invariant 4 — Window Isolation
RU data must not leak across report_interval windows.
Each window must be logically isolated in aggregation and reporting.

### Invariant 5 — Subscriber Isolation
Each subscriber controls its own enable/disable state and report_interval.
One subscriber’s configuration or backpressure must not affect others.

---

## 2. Risk Matrix

| Risk ID | Scenario | Potential Impact | Trigger Condition | Current Mitigation |
|-------|---------|------------------|------------------|-------------------|
| R1 | High-cardinality RU keys | OOM / memory blow-up | Adversarial user/sql patterns | Aggregator hard cap (10k keys or (1000user * 500sql)) |
| R2 | Heavy skew (single user / SQL) | Loss of visibility for long tail | Hybrid TopN (200×200) | Aggregation into "others" |
| R3 | Long-running executions | exec_count / deltaRU mismatch | Execution spans multiple windows | In-flight accounting model |
| R4 | Report interval jitter | Double count or missing RU | Timing drift / scheduling delay | Wall-clock window model |
| R5 | Multiple subscribers | Cross-subscriber interference | Different intervals / load | Per-subscriber ownership model |

---

## 3. Validation Strategy

### 3.1 Invariant-based Validation

- Invariant 1:
  - Validate via unit tests on TopN + "others" aggregation behavior.
  - Reason about RU conservation under eviction.

- Invariant 2:
  - Stress tests with synthetic workloads generating >10k distinct RU keys.
  - Memory profiling to confirm bounded growth.

- Invariant 3:
  - Targeted tests for long-running executions.
  - Code inspection of execution begin / finish paths.

- Invariant 4:
  - Time-based tests simulating boundary conditions around report_interval.
  - Verify no cross-window accumulation.

- Invariant 5:
  - Multi-subscriber tests with mixed enable/disable states.

### 3.2 Non-functional Validation

- Performance:
  - Validate aggregator hot paths under high RU throughput.
- Stability:
  - Ensure no goroutine leaks or channel backpressure deadlocks.

---

## 4. Accepted Trade-offs & Non-goals

The following behaviors are explicitly accepted by design:

- "Others user" and "others SQL" reduce per-entity diagnosability.
- TopN selection is approximate and biased toward heavy users.
- exec_count may differ slightly from real execution counts under sampling.
- Backpressure may drop new RU keys once limits are reached.

These trade-offs are intentional and documented.

---

## 5. Exit Criteria

Phase 3 is considered complete when:

- All listed invariants have been validated or reasoned about.
- No unmitigated high-risk scenarios remain.
- Memory usage remains bounded under stress.
- No correctness violations are identified in windowing or accounting logic.

---

## 6. Evidence Slots for Review (NEW)
- Every finding must include “Closure requirement”:
  - What evidence closes it (unit test / integration test / bench / metric / log / trace)?
  - If evidence is missing: status must remain Open (no “PASS by reasoning”).
- Review output format required:
  - Finding
  - Severity
  - Closure requirement
  - Evidence ID (if closed)
  - Status: Open / Closed


