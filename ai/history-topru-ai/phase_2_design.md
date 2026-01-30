# PHASE_2_DESIGN_DECISIONS.md

## Scope

This document freezes all **Phase 2 design decisions** for the TopRU subsystem.

* Phase 1 is treated as a **fully frozen behavioral and interface contract**.
* Phase 2 introduces **bounded TopN aggregation, windowing semantics, and backpressure handling**.
* No Phase 1 interface, protocol, or semantic changes are allowed.

This document serves as the **only execution contract** for Phase 2 implementation.

同时生成（NEW）：
- ASSUMPTIONS_REGISTER.md（列出 A#，标注 Closure evidence / 进入实现前必须关闭）
- SEMANTIC_SPEC.md（明确：disabled 行为、drop/backpressure 策略、interval 语义、compat 策略，并标注需要 maintainer GO 的条目）

---

## Design Principles

1. **Strict backward compatibility**

   * No reinterpretation of Phase 1 semantics.
   * No modification of existing interfaces or data structures.

2. **Bounded memory and predictable load**

   * All aggregation structures must have explicit upper bounds.

3. **Diagnosability over completeness**

   * Preserve the most impactful RU contributors.
   * Accept loss of long-tail visibility as an explicit trade-off.

4. **Separation from TopSQL CPU pipeline**

   * TopRU remains logically independent, even if implementation patterns are similar.

---

## Frozen Design Decisions

### A. TopN + Others Semantics

**Decision:**
Adopt **Hybrid TopN (Option 3)**.

**Definition:**

* Maintain a global TopN of users by TotalRU.
* For each selected user, maintain a per-user TopN of SQL statements.
* Users outside the global TopN are aggregated into a single **"others user"**.
* SQLs outside per-user TopN are aggregated into a per-user **"others SQL"**.

**Trade-offs (Accepted):**

* Bias toward heavy users.
* Long-tail users and statements lose individual visibility.
* Diagnostic clarity prioritized over completeness.

---

### B. Window Model

**Decision:**
Use **fixed wall-clock aligned windows** (Option 1), consistent with TopSQL behavior.

**Definition:**

* Aggregation windows align to absolute time boundaries.
* `report_interval` determines window size.
* Partial windows may exist at subscription start.

**Rationale:**

* Deterministic behavior.
* Easier cross-node and cross-metric comparison.
* No strong requirement for sliding-window semantics.

---

### C. In-flight Execution Handling

**Decision:**

* `exec_count` increments **only at `OnExecutionBegin`**, consistent with TopSQL.
* `deltaRU` is accumulated:

  * Periodically during execution (sampling).
  * At execution finish.

**Implications:**

* Long-running statements contribute RU progressively.
* `exec_count` reflects execution starts, not completions.
* Consumers must interpret RU and exec_count independently.

---

### D. Enable / Disable / Ownership Semantics

**Decision:**
Use **per-subscriber control**.

**Definition:**

* Each subscriber independently enables or disables TopRU collection.
* `report_interval` and enable flags are provided **only at registration time**.
* No dynamic reconfiguration during an active subscription.

**Implications:**

* No global ownership or reference counting.
* Aggregation may remain active even if some subscribers are disabled.

---

### E. Backpressure and Load Shedding

**Decision:**
Adopt **multi-level backpressure** (Option 2 + 3).

**Session-level:**

* Enforce a hard cap on distinct RU keys (e.g., user/sql/plan combinations).
* Example: limit of 10,000 distinct active keys (exact value implementation-defined).
* Excess keys are dropped early to protect hot paths.

**Aggregator-level:**

* Enforce TopN bounds:

  * Global Top users (e.g., 200).
  * Per-user Top SQLs (e.g., 200).

**Rationale:**

* Early shedding protects execution hot paths.
* Aggregator-level TopN preserves diagnostic value.

---

## Explicit Non-Goals (Phase 2)

* No new RUKey dimensions (e.g., resource group, keyspace).
* No cross-node aggregation.
* No billing-grade RU accuracy guarantees.
* No alerting or anomaly detection.
* No changes to protocol compatibility guarantees.

---

## Compatibility Guarantees

Phase 2 MUST preserve:

* RUKey semantics defined in Phase 1.
* Delta-based RU sampling behavior.
* RUIncrement fields and meanings.
* Aggregator → Reporter data contracts.
* Protocol additivity only (no breaking changes).

---

## Phase 2 Exit Criteria (Design Freeze)

Phase 2 is considered **design-frozen** when:

* This document is committed.
* No unresolved design options remain.
* All Phase 2 implementation work strictly follows this document.
* Any deviation requires explicit Phase 2 re-opening.

---

**Status:** DESIGN FROZEN
**Next Step:** Explicit authorization required to begin Phase 2 implementation.

