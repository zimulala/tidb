TopRU Final Gate Checklist (≤10)
1. 启用/禁用行为清晰且一致
  - RC 未启用时：TopRU 的行为是“跳过上报”还是“RU=0 上报”，与代码/注释/日志一致
  - 证据：diff（条件分支）、E_integ run.log / tidb.log（是否出现 TopRU report）
2. RURecords 结构化上报存在且字段合法
  - 至少 1 条 RURecords；exec_count >= 1；total_ru > 0（启用场景）
  - 证据：E_testgen_topru summary.md / run.log（结构化单测断言）或 E_integ 结构化 subscriber 输出
3. SQLMeta / PlanMeta 与 RURecords 能关联起来
  - SQL digest / plan digest 在 metas 中可找到（marker 或 digest 对齐）
  - 证据：结构化单测（G3/G4）run.log；或 subscriber/manifest 中记录
4. 聚合 key 正确：按 (user, sql_digest, plan_digest) 隔离
  - 不同 user 同 SQL 不混；同 user 不同 plan 不混（或符合设计）
  - 证据：结构化生成测试（G7 类）或你手写结构化测试断言；diff（key 构造）
5. 同一窗口累加正确（不丢不覆盖）
  - 同 timestamp 同 key 多次 finish：RU 增量被累加进入 TopN（或等价机制）
  - 证据：结构化测试（G8 类）/ diff（聚合累加逻辑）/ E_integ 日志信号
6. execCtx 生命周期正确且无泄漏
  - begin attach / finish clear；不会跨 statement 复用；并发下无数据竞态迹象
  - 证据：diff（生命周期点位）、结构化测试（G9 类）或单测注入、race 相关说明（如有）
7. 与 TopSQL（CPU）共存不回归
  - RU 链路不会影响 CPU TopSQL 的现有逻辑/开销路径（关键共享组件审过）
  - 证据：diff（共用 sink/ticker/锁）、E_perf sanity / 现有 reporter tests 通过
8. 失败处理不阻塞 SQL 路径
  - sink 失败/序列化失败/下游不可用时不会阻塞执行路径（drop/retry策略明确）
  - 证据：diff（错误处理策略）、日志（失败只 warn 不 fatal）、如有测试更好
9. 可诊断性：最小日志/指标能定位“是否在发/为何不发”
  - 至少能通过日志判断 enable 状态、发送次数/失败原因（不要求 verbose）
  - 证据：tidb.log（TopRU reporter 日志点）、subscriber log/summary.md
10. 证据齐全且可复现
- SSOT 中 E_func/E_integ/E_perf（+可选 E_testgen_topru）均 Captured，artifact 指向可读摘要（summary/run.log）
- 证据：PROJECT_STATE.md evidence 段 + artifacts 路径存在
