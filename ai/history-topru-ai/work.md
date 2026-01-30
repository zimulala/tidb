# work.md（工作台账 / 可复用模板）

> 目的：把“规划 + 说明用的架构”写清楚，把严格 review 与细粒度验证过程可追踪化，把重构的原因与影响说明白。
> 使用方式：每次做一个明确的任务就更新本文件（建议按 Iteration 记录），最终交付时确保验收点都能对应到验证记录。

---

## 0. 元信息
- 任务名：TopRU（TopSQL pipeline 复用：按 RU 近实时排序与聚合）
- Design Doc：docs/design/2026-01-19-topru-design.md
- Tracking Issue：pingcap/tidb#65471
- 分支：
- 负责人：
- 开始日期：2026-01-19
- 目标截止（可选）：

## 1. 问题陈述（Problem Statement）
- 现象/痛点：TiDB Cloud 以 RU 计费；当集群 RU 异常或触发 MAX RCU 告警时，需要在近实时（分钟级）定位 RU 消耗最高的 SQL。
- 影响范围：运维排障、限流/终止高消耗 SQL、回滚/优化决策。
- 为什么现在要做：现有 Slow Log 只覆盖已完成的慢查询；Statement Summary 默认 30min 级持久化窗口且缺少内存实时视角，无法满足分钟级实时定位需求。

## 2. 目标与边界（Goals / Non-goals）
### Goals
- G1：按 RU 累积消耗排序/查询，能识别“短但贵”的 SQL（非慢查询也可能高 RU）。
- G2：按 (user, sql_digest, plan_digest) 聚合，支持用户维度的 RU 分布诊断。
- G3：近实时：本地 1s 采样，按 report_interval（默认 60s；可 15s/30s/60s）批量上报；端到端延迟约 60~120s。
- G4：与现有 TopSQL（CPU time）共存：RU 数据独立存储/上报，不改变 TopSQL 语义。

### Non-goals（明确不做什么）
- NG1：TopRU RU ≠ Billing RU：本阶段不做与计费 RU 的对齐/核对。
- NG2：对“正在执行的 SQL”不保证具备完整的 slow query / statement 信息（未完成时信息可能不齐）。
- NG3：不做 RU 异常检测与自动告警。

## 3. 规划（Planning：阶段 / 里程碑 / 验收点）
### 验收标准（Acceptance Criteria）
- AC1：enable_top_ru=true 且 Resource Control 开启时，按 (user, sql_digest, plan_digest) 产出 RU 记录；RU 语义正确（RRU+WRU 的累计值采样为 delta，避免重复计数）。
- AC2：report_interval 15s/30s/60s 生效；上报节奏与端到端延迟符合预期（~1–2 个 interval）。
- AC3：内存硬边界：1s/15s tier 200×200 + others；最终上报 100×100 + others；高基数场景不无界增长。
- AC4：动态开关：关闭后停止采样/上报并在下一个周期清空 RU 数据；TopSQL CPU pipeline 不受影响。
- AC5：Resource Control 关闭时不采集/不上报（避免产出无意义的 0 数据）。
- AC6：协议向前兼容（仅新增字段/消息，不改既有语义）；老客户端可忽略 RU 数据。
- AC7：测试覆盖：RU delta、execCtx 生命周期、TopN/others、toggle；并验证 TopSQL 不被干扰。

### 里程碑
- M1：协议与数据模型打通
  - 交付物：TopRU protobuf（TopRURecord/Item + 订阅侧配置 enable_top_ru/report_interval）；TiDB 侧 ReportData 扩展携带 RURecords。
  - 验收点：编译通过；PubSub/DataSink 能发送 RURecords（向前兼容）。
- M2：执行期采样与 session-local 聚合
  - 交付物：StatementStats 增加 ExecutionContext（Ctx/Key/LastRUSample）+ Finish path 缓冲；1s tick 统一 MergeRUInto。
  - 验收点：RU delta 计算正确；finish/tick 合并不重复计数；空/负 delta 丢弃。
- M3：Reporter 侧分层缓冲与 TopN
  - 交付物：ruIncrementBuffer（按秒，200×200+others）；15s 合并到 ruPointBucket；report_interval 合并并做 100×100 最终筛选。
  - 验收点：边界保护生效；高基数下仍可控；_others_ 聚合正确。
- M4：开关/门控 + 测试与回归
  - 交付物：enable_top_ru 动态开关 + Resource Control 门控；单测/回归测试。
  - 验收点：AC1~AC7 全部满足。

### 风险与假设（先写出来，后面逐条验证）
- 假设 A1：执行上下文 ctx 在 SQL 执行链路全程携带 util.RUDetailsCtxKey（RUDetails 可被采样读取）。
- 假设 A2：TopRU 的订阅配置可通过现有 TopSQL PubSub 协议扩展（TopSQLSubRequest/Response 支持新增字段/oneof）。
- 风险 R1：当前 tipb 协议（topsql_agent.proto 生成代码）TopSQLSubRequest 为空且没有 TopRURecord 类型 —— 需要升级 tipb 依赖或同步修改协议，涉及下游组件兼容。
- 风险 R2：把 TopRU 开关与 TopSQL 开关耦合会违反“独立 feature”意图，需要保证两者 enable/disable 互不影响。

## 4. 说明用的架构（Explainable Architecture）
> 目标：解释清楚“采集了什么/聚合了什么/丢弃了什么/为什么要丢弃”，以及哪些诊断能力因此变得可行或不可行。

### 4.1 模块边界（模块/包/目录职责 + 不负责什么）
- SQL Execution Layer（pkg/executor/adapter.go 等）
  - 职责：在 SQL 开始/结束的生命周期点调用 TopRU/TopSQL 的 hook（注册 execCtx，finish 写入 session-local buffer）。
  - 不负责：不做 TopN/窗口聚合；不做跨 session 聚合；不做网络上报。
- stmtstats（pkg/util/topsql/stmtstats）
  - 职责：每 session 维护 StatementStats；新增 execCtx + RU delta 采样状态；提供 MergeRUInto 给 1s tick 统一拉取。
  - 不负责：不做跨节点聚合；不关心下游 sink；不做最终 TopN。
- aggregator（pkg/util/topsql/stmtstats/aggregator.go）
  - 职责：全局 1s tick：拉取所有 session 的 RU increments + stmtstats，并交给注册的 Collector/RUCollector。
  - 不负责：不做 15s/60s tier 合并；不做 protobuf 转换。
- reporter（pkg/util/topsql/reporter）
  - 职责：接收 1s RU increments，进行 200×200 两级 TopN 预过滤、15s 合并、report_interval（默认 60s）最终 100×100 筛选并上报；RU 数据与 CPU/stmtstats 数据分开存储，最终在 ReportData 汇合。
  - 不负责：不参与 SQL 执行；不读取 RUDetails；不强依赖 TopSQL enable。
- proto / sink（tipb + datasink/pubsub）
  - 职责：协议承载（新增 TopRURecord/Item，扩展订阅请求配置），并把 ReportData 发送给订阅者。
  - 不负责：不做本地聚合；不强制客户端理解所有新增字段（向前兼容）。

### 4.2 数据流 / 控制流（关键路径）
```
SQL Start
  -> StartExecution(register execCtx: ctx + (user,sql,plan) + lastRU)

1s tick
  -> aggregator.ruAggregate
     -> for each session StatementStats: MergeRUInto()
        - drain finishedRU buffer
        - sample execCtx RUDetails (cumulative) and compute delta
     -> RUCollector.CollectRUIncrements

Reporter
  -> ruIncrementBuffer.Add(ts=sec, increments)
     - 200 users × 200 sql/user TopN + others
  -> every 15s: processRUIncrementBuffer() merge -> ruPointBucket[startTs]
  -> every report_interval: reportRUData()
     - merge buckets, 100×100 final filtering + others
     - build RURecords + send

SQL Finish
  -> FinishExecution() compute final delta + write session-local finishedRU buffer
```

### 4.3 关键抽象与不变量（Key abstractions & invariants）
- 抽象 X：util.RUDetails（runtime 累积值）
  - 不变量：采样必须基于 delta（current - last）；delta<=0 或 RUDetails=nil 时丢弃。
- 抽象 Y：ExecutionContext（Ctx, Key, LastRUSample）
  - 不变量：每个 session 同时最多 1 个“当前执行语句”的 execCtx；Finish 会清理 execCtx。
- 抽象 Z：两级 TopN 缓冲（timestampBuffer: userBuffer）
  - 不变量：任何时刻 users<=200 且 per-user sql<=200；被驱逐项 RU 进入 _others_。

### 4.4 依赖方向与规则（Dependency direction）
- 允许依赖：executor/session -> stmtstats -> reporter；reporter -> datasink/proto
- 禁止依赖：stmtstats/aggregator 反向依赖 executor；reporter 反向依赖 stmtstats 的内部细节（仅依赖接口/数据结构）。
- 约束原因：避免在 SQL 热路径引入复杂聚合/IO；保持采样与上报解耦，便于演进与诊断。

### 4.5 扩展点（Extension points）
- EP1：新增维度（例如 resource group / keyspace / store type）时，优先在 Key 结构扩展并在 buffer 层做 TopN/others 策略调整。
- EP2：新增指标（如 RU share percent、RRU/WRU 分拆）可在 tsItem/record 中扩展，并保持协议字段仅新增。

### 4.6 演进路径（如何演进/替换，避免锁死）
- 方案演进：优先保持 RU pipeline 与 TopSQL pipeline“并行但共享基础设施”，避免把 TopRU 绑定到 TopSQL enable 语义。
- 兼容策略：protobuf 仅新增 message/field/oneof；旧订阅者可忽略 RU；新订阅者可请求/开关 TopRU。

## 5. 决策记录（Decision Log）
> 每条记录建议包含：Decision / Alternatives / Rationale / Trade-offs。

- D1：TopRU 复用 TopSQL PubSub 通道并扩展协议（而不是新增一套 gRPC service）
  - Decision：在 topsql_agent.proto 里新增 TopRURecord/Item，并扩展 TopSQLSubResponse oneof；TopSQLSubRequest 增加订阅配置（enable_top_ru/report_interval）。
  - Alternatives：新增 TopRU 独立 PubSub service；或复用但用 SQLMeta/PlanMeta side-channel。
  - Rationale：复用现有数据通道与 sink 生命周期管理，降低系统复杂度；协议 additive 可兼容旧客户端。
  - Trade-offs：需要升级 tipb 依赖，并协调下游组件对新增消息的处理。

- D2：RU 数据独立存储于 collecting.ruRecords/ruPointBucket（不复用 collecting.records）
  - Decision：RU 与 CPU/stmtstats 完全分开存储/聚合，最终在 ReportData 汇合。
  - Alternatives：扩展 records key 或把 RUByUser 嵌入 tsItem。
  - Rationale：避免改动现有 TopSQL 语义与关键路径；降低侵入性。
  - Trade-offs：reporter 结构更复杂，但边界更清晰。

## 6. 严格模式 Review 记录（可多，不怕多）
> 看到不符合架构/目标/维护性的点，明确点名并记录原因（便于追踪与改进）。

- Issue 1：协议与设计不一致（阻塞项）
  - 发现点：当前 tipb TopSQLSubRequest 为空，TopSQLSubResponse 仅支持 Record/SqlMeta/PlanMeta；不存在 TopRURecord。
  - 为什么不合适：设计依赖“订阅配置推送 enable_top_ru/report_interval”与“新增 RURecords 上报”；没有协议支持无法落地。
  - 修复策略：升级 tipb 依赖到包含 TopRU/订阅配置的版本，并同步调整 reporter/pubsub。
  - 状态：待修

## 7. 细粒度验证记录（Very fine-grained verification）
> 目标：逐条确认假设，避免“看起来对”。每次迭代至少 3 条假设验证。

### 验证条目模板
- 日期：
- 假设：
- 验证方式（命令/测试/静态检查/日志）：
- 结果：通过 / 失败 / 不确定
- Missing（还缺什么验证）：

### TiDB（如适用）测试注意事项
- 跑 unit tests 前：`make failpoint-enable`
- 跑完（即便失败）：`make failpoint-disable`

## 8. 重构记录（持续重构：原因 + 改动 + 依据）
> 把重构当作迭代的一部分；每次重构都写清楚“为什么改、改了什么、依据是什么、影响是什么”。

- Refactor 1：
  - 原因：维护性问题 / 冗余 / 违反架构 / 偏离目标 / 严格模式不通过 / 其他
  - 改动：
  - 依据（为什么更可维护/更少冗余/更对齐目标）：
  - 影响（行为/兼容性/测试/性能）：

## 9. 迭代日志（Loop：小步 → 严格 review → 细粒度验证 → 必要时重构）

### Iteration 1（日期：）
- 这一步做了什么（最小变更）：
- 严格 review 发现：
- 细粒度验证（至少 3 条）：
- Missing（下一步要补的）：
- 触发的重构（如有）：

### Iteration 2（日期：）
- 这一步做了什么（最小变更）：
- 严格 review 发现：
- 细粒度验证（至少 3 条）：
- Missing（下一步要补的）：
- 触发的重构（如有）：

## 10. 最终验收对照（交付前必须填）
- 验收点 A：对应验证记录：
- 验收点 B：对应验证记录：

## 11. 交付摘要（Deliverable Summary）
- 做了什么：
- 没做什么：
- 如何验证：
- 已知风险/后续工作：
