# AI-assisted Change Protocol v1 — Defining Trust Boundaries for AI Agents in Engineering Workflows

## Status

Draft / Proposed

## Motivation

现有 AI integration（Claude skills / ChatGPT plugins / IDE agents）擅长：

- 多步执行
- 工具调用
- 生成 diff

但没解决最难的问题：

“到底哪些结论可以信？AI 说 PASS 到底意味着什么？”

本 RFC 强制：

- 明确 Trust Level
- 明确证据政策
- 明确 STOP 规则
- 明确人类权威

## Key Insight（从 Claude Skills 借鉴，但用流程实现）

成功点不在“更聪明”，而在：

- 能力显式（capability explicitly declared）
- 副作用可观察（artifacts）
- 允许失败（STOP）

由于我们不依赖特权 runtime，本协议用 “产物 + 契约 + 证据” 把这些能力落地。
