# Repository Layout（建议）

ai-assisted-change-protocol/
├── START_HERE.md
├── README.md
├── rfc/
│   └── AI-assisted-Change-Protocol-v1.md
├── contracts/
│   ├── trust-levels.md
│   ├── execution-contract.md
│   ├── evidence-policy.md
│   ├── stop-rules.md
│   ├── assumption-closure-gate.md
│   ├── semantic-go-gate.md
│   └── review-evidence-slots.md
├── templates/
│   ├── CHANGE_BRIEF.md
│   ├── CHANGE_BRIEF_MINI.md
│   ├── PHASE_0_INTAKE.md
│   ├── PHASE_1_RISK_REGISTER.md
│   ├── PHASE_2_DESIGN_RECORD.md
│   ├── ASSUMPTIONS_REGISTER.md
│   ├── SEMANTIC_SPEC.md
│   ├── DATA_PATH_MAP.md
│   ├── PHASE_3B_VALIDATION_RECORD.md
│   ├── PHASE_3C_IMPLEMENTATION_LOG.md
│   ├── PHASE_3D_REVIEW_REPORT.md
│   ├── FAILURE_ASSUMPTIONS.md
│   ├── PR_AI_DISCLOSURE.md
│   └── EVIDENCE_INDEX.md
├── prompts/
│   ├── quickstart.prompt.md
│   ├── quickstart-mini.prompt.md
│   ├── prompt-library.md
│   ├── system-prompt.md
│   ├── phase-0-intake.prompt.md
│   ├── phase-1-risk.prompt.md
│   ├── phase-2-design.prompt.md
│   ├── phase-3b-validation.prompt.md
│   ├── phase-3c-implementation.prompt.md
│   ├── phase-3d-review.prompt.md
│   └── tool-claims.prompt.md
├── phases/
│   ├── phase-0-intake.md
│   ├── phase-1-risk.md
│   ├── phase-2-design.md
│   ├── phase-3b-validation.md
│   ├── phase-3c-implementation.md
│   └── phase-3d-review.md
├── tracks/
│   ├── README.md
│   ├── observability-pipeline/
│   │   ├── README.md
│   │   ├── templates/
│   │   │   ├── SCHEMA_SPEC.md
│   │   │   ├── SOP_SPEC.md
│   │   │   ├── CAPABILITY_MATRIX.md
│   │   │   └── DROP_METRICS_SPEC.md
│   │   └── prompts/
│   │       ├── phase-2-semantic.prompt.md
│   │       ├── phase-3b-validation.prompt.md
│   │       └── phase-3d-review.prompt.md
│   └── resource-observability-topru/
│       ├── README.md
│       ├── templates/
│       │   ├── TOPRU_SEMANTIC_SPEC.md
│       │   ├── TOPRU_COMPAT_SPEC.md
│       │   └── TOPRU_DROP_SPEC.md
│       └── prompts/
│           ├── topru-phase-2.prompt.md
│           └── topru-phase-3d-review.prompt.md
└── examples/
    └── topru-case-study.md

## What this is

这是一个 maintainer-grade 的 AI 辅助改动协议，用来在生产代码库里安全引入 AI：

- AI 能写，但不能自证
- 所有结论必须标注 epistemic status（知识状态）
- “PASS / MERGE APPROVED” 默认禁止
- STOP（停止并请求澄清）是正确产物，不是失败

## Core Principles

- AI never upgrades epistemic status on its own
- 所有“结论”必须带标签 + 证据
- Maintainer authority by design
- Gate-based governance（硬闸门优先）：
  - Open assumptions ⇒ STOP（contracts/assumption-closure-gate.md）
  - Semantic decisions 无 maintainer GO ⇒ STOP（contracts/semantic-go-gate.md）
  - Verified-by-test 无 Evidence Index ⇒ STOP（contracts/evidence-policy.md）
  - Review findings 必须可关闭（closure requirement + evidence id）（contracts/review-evidence-slots.md）
- Evidence is immutable during review：证据必须绑定 commit/time/env/params；diff 变化 ⇒ 证据失效

## Trust Levels（TL0~TL4）

见：contracts/trust-levels.md

关键约束：Phase 3C/3D 默认上限 TL3，除非你能提供真实的测试/benchmark/日志等 artifacts（达到 TL4）。

## Tracks（Domain packs）

Tracks 是可复用的“领域扩展包”：不改 core governance，只增加领域模板、额外 gates、prompt helpers。

见：tracks/README.md

## Quick Start（你真的照着做就能落地）

### Quick Path（5分钟版本）

只写一个输入文件，剩下由 Codex 生成：

- templates/CHANGE_BRIEF.md（你只填目标/约束/验收/风险）
- prompts/quickstart.prompt.md（一键生成 Phase 0/1/2/3B）

入口说明：START_HERE.md

### Quick Path（3行极简版）

仅在你显式开启“半自动化功能实现”时使用：

- templates/CHANGE_BRIEF_MINI.md（仅 3 行：目标/约束/验收）
- prompts/quickstart-mini.prompt.md（补全为 CHANGE_BRIEF + Phase 0/1/2/3B）

### Standard Path（完整流程）

新建改动工作目录：

- templates/PHASE_0_INTAKE.md（填需求）
- templates/PHASE_1_RISK_REGISTER.md（列风险）
- templates/PHASE_2_DESIGN_RECORD.md（冻结设计）
- templates/PHASE_3B_VALIDATION_RECORD.md（验证设计/范围）
- templates/PHASE_3C_IMPLEMENTATION_LOG.md（实现日志）
- templates/PHASE_3D_REVIEW_REPORT.md（最终审查报告）
- templates/FAILURE_ASSUMPTIONS.md（失败假设清单）
- templates/EVIDENCE_INDEX.md（证据索引）

每一步调用对应 prompt（见 prompts/），把 AI 输出粘贴回模板文件。

PR 里必须贴：templates/PR_AI_DISCLOSURE.md
