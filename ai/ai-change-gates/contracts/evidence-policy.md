# Evidence Policy

## Epistemic Labels（强制三选一）

所有结论必须归类为 且仅能归类为：

1) Design-level assessment（设计层评估）：基于文档/逻辑，不代表真实运行结果
2) Assumption-based（基于假设）：明确写出假设 X/Y/Z
3) Verified-by-test（测试验证）：必须附证据（artifact）

## Evidence（证据）可接受形式

- CI Job 链接 / 输出摘要
- go test / pytest / bazel test 等命令输出
- benchmark 报告（含 commit hash、环境、参数）
- 运行日志（含时间、版本、输入规模）
- 生产监控截图/导出（含时间窗口、指标名）

## 禁止项

- “我认为没问题”
- “应该通过”
- “PASS（不带证据）”
- “MERGE APPROVED”
