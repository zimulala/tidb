# TopRU Phase 1 Freeze Document

## 1. Scope of Phase 1

Phase 1 的目标不是「把事情做对」，而是：
- 打通数据流（结构闭环 + 可编译）
- 确认模块边界（谁产出/谁聚合/谁上报/谁消费）
- 证明该方向“可携带数据”并产生最小可观测信号（smoke / injection / finish-path 均可）
- 为未来变化留钩子

注意：Phase 1 的“能跑/能收/能送”不等于语义完备；
in-flight sampling / drop 策略 / interval 语义 / 兼容策略若未证据化，必须进入 Assumption Register 并在后续关闭。

---

## 2. What Phase 1 Explicitly Does NOT Do

The following are **out of scope** for Phase 1 and intentionally deferred:

* ❌ TopN buffering or ranking logic
* ❌ Memory bounding or eviction strategy
* ❌ ResourceGroup-level RU attribution
* ❌ Executor-level RU instrumentation
* ❌ Performance optimization
* ❌ Behavior tuning under high cardinality

Any attempt to add the above is considered a **Phase 2+ change**.

---

## 3. Frozen Interfaces and Invariants

The following interfaces and behaviors are frozen in Phase 1:

* RUKey semantics:

  * User-level aggregation only
* ExecutionContext RU delta calculation model
* RUIncrement structure:

  * TotalRU, ExecCount, ExecDuration
* Aggregator → Reporter contract
* PubSub data sink protocol compatibility

No behavior changes are permitted without exiting the Phase 1 freeze.

---

## 4. Phase 2 Extension Points

Phase 2 work MUST attach only at explicitly marked extension points:

* TODO(M3): TopN buffering and memory control
* TODO(M4): Executor-level RU integration
* RUKey extension for ResourceGroup attribution

These extension points are documented inline in code comments.

---

## 5. Definition of Done (Verified)

* ✅ All code builds successfully
* ✅ No behavior changes after freeze
* ✅ Design intent documented in all RU-related modules
* ✅ Phase 2 extension points explicitly marked

Phase 1 is considered **complete and frozen**.

---

## 6. Change Policy

Any future modification must declare:

* Target phase
* Behavioral impact
* Backward compatibility considerations

No silent evolution is allowed beyond this point.

