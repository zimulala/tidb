# Phase 3 Issues — Design-Fixed

Status: UNDER VALIDATION (Phase 3B)
Owner: xia
Last Updated: 2026-01-21

Rules:
- Issues listed here are design-fixed.
- No alternative designs allowed.
- No implementation allowed without explicit GO.


## Accepted / Non-Issues
- Window snapshot semantics (arrival-time stamping; no per-window filtering)
- Backpressure key drops without per-key metrics (accepted diagnostic trade-off)
- ruRecord.items bounded growth (bounded by report_interval)
- Executor-level RU integration (explicit Phase 2/3 non-goal)

## Design-Fixed Issues (Phase 3 Implementation Required)

### 1. Subscriber Isolation — enable_topru lifecycle

**Problem**
- enable_topru is currently controlled by a global boolean.
- Multiple subscribers can interfere with each other:
  - One subscriber disabling TopRU may unintentionally disable it for others.
  - report_interval is globally mutable and overwritten by last subscriber.

**Design Decision (Frozen)**
- Use reference-counted subscriber tracking.
- Introduce `ruConsumerCount`:
  - `enable_topru == (ruConsumerCount > 0)`
- enable_topru is activated when the first subscriber enables TopRU.
- enable_topru is deactivated only when the last subscriber unsubscribes.
- report_interval is fixed at subscription registration time and not mutated during subscription lifetime. However, if a is set to 30s and b is set to 60s, the later one can be considered to override the earlier one, or the smaller interval will prevail. You can think about that

**Rationale**
- Preserves Phase 1 global collection model.
- Avoids per-subscriber execution overhead.
- Ensures subscriber lifecycle isolation without API changes.
- Aligns with common shared-resource ownership patterns.

**Status**
- Design frozen
- Implementation pending (Phase 3)

---

### 2. Reporter Pre-TopN Memory Bounding

**Problem**
- `ruCollecting.users` and per-user record maps can grow unbounded
  before TopN filtering is applied.

**Decision**
- Apply bounded caps consistent with Phase 2 TopN intent.
- Ensure no unbounded growth during a report_interval window.

**Status**
- Design frozen
- Implementation pending (Phase 3)

---

## Verification-Only Items
- exec_count source correctness (verify reuse vs independent path)

