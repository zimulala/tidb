# Phase 2 — Design & Invariants（设计冻结）

Trust Level：TL1

产物：templates/PHASE_2_DESIGN_RECORD.md + templates/ASSUMPTIONS_REGISTER.md + templates/SEMANTIC_SPEC.md + templates/DATA_PATH_MAP.md（pipeline-like 时必须）

### Decision Escalation Rule (patched)

当存在多个实现路径且未收到人工确认时，先对决策分级：

A) Semantic Decisions（语义/兼容/降级/数据丢弃/节奏耦合）
- 例：TopRU disabled 时 drain+drop 是否允许；cap 下丢弃策略；TopRU interval 是否影响 TopSQL cadence；
  Unimplemented/旧 agent 兼容策略；PubSub 与 SingleTarget 语义一致性。
- 规则：必须显式获得 maintainer “GO”。没有 GO ⇒ STOP（不得 implicit）。

B) Mechanical Decisions（机械/局部/可回滚且不改变外部语义）
- 规则：允许给出默认推荐方案并继续推进，但必须记录为「Implicit Approval」并在下一次 Review 中显式提请确认。

### 你要做的

- 冻结 scope
- 写不变量（invariants）
- 写验收标准（acceptance criteria）
- 明确 non-goals
- 明确接口/数据结构/迁移策略（如有）

