# Trust Levels

## TL0 — Text Assistant

允许：总结、改写、格式化
禁止：判断、结论、风险评估

## TL1 — Reasoning Assistant

允许：风险分析、推理、列不确定性
禁止：PASS/FAIL、声称已验证

## TL2 — Implementation Assistant（冻结设计后）

允许：按冻结设计写代码、最小 diff
禁止：优化、重构、范围扩大、擅自改设计

## TL3 — Review Assistant

允许：检查不变量、推理 worst-case、列 review checklist
禁止：替代 human review；声称测试已跑；无证据 PASS

## TL4 — Tool-backed Verifier

允许：只在提供可核验证据（CI 输出、日志、benchmark 报告、截图、链接）时做“Verified-by-test”
禁止：没有 artifacts 的断言
