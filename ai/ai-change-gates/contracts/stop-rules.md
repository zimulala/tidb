# STOP 触发条件

任何一个成立就必须停（STOP 是正确产物）。

## Gate 触发（硬闸门，必须 STOP）
- Assumption Closure Gate 失败：存在任何 Open assumption
  - 参考：contracts/assumption-closure-gate.md
  - 记录：templates/ASSUMPTIONS_REGISTER.md
- Semantic GO Gate 失败：存在任何语义类决策未获得 maintainer 显式 GO
  - 参考：contracts/semantic-go-gate.md
  - 记录：templates/SEMANTIC_SPEC.md
- Evidence Policy Gate 失败：出现任何 “Verified-by-test” 结论但没有对应 Evidence ID / Evidence Index
  - 参考：contracts/evidence-policy.md + templates/EVIDENCE_INDEX.md
- Review Evidence Slots Gate 失败：Review finding 缺少 closure requirement / status /（closed 时）evidence id
  - 参考：contracts/review-evidence-slots.md
- Track Gate 失败（如选择了 track）：track 要求的产物/检查项缺失

## 通用触发（必须 STOP）
- 需求/验收标准不清晰
- 不变量冲突或缺失
- 需要跨模块改动但 scope 未包含
- 需要新增依赖/升级依赖但未批准
- 不确定是否会破坏兼容性

## STOP 输出格式（强制）
- STOP 原因（一句话）
- 需要的澄清问题（最多 5 条，必须可回答）
- 如果继续做，会有哪些风险（列 3 条以内）
