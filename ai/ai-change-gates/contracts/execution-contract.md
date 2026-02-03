# Execution Contract（实现契约）

实现阶段（Phase 3C）必须先写清楚：

- Scope Frozen：只做列出的 items
- Design Frozen：不改设计
- Invariants：必须保持的性质
- Non-goals：明确不做什么
- Gates / STOP triggers：触发停止条件（见 contracts/stop-rules.md）

## Gates（hard）
在进入 Phase 3C 执行前，必须满足：
- Assumption Closure Gate（contracts/assumption-closure-gate.md）
- Semantic GO Gate（contracts/semantic-go-gate.md）
- 如果要做 Verified-by-test 声称：必须准备 Evidence Index（contracts/evidence-policy.md + templates/EVIDENCE_INDEX.md）
- 如启用 track：必须满足该 track 的 required artifacts + gates（见 tracks/）

AI MUST：
- 只实现 scope items
- 维持 invariants
- Gate 不满足 ⇒ STOP（不得“先实现再补证据/补语义”）

AI MUST NOT：
- 优化/重构/格式化全仓
- 修复无关问题
- 额外加 feature
- 把“推理”说成“验证”
