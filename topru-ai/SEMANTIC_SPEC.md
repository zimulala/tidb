# SEMANTIC_SPEC.md (TopRU)

> 所有 S# 都属于 Semantic GO Gate：没有 maintainer 显式 GO ⇒ STOP（不得进入实现）。
> 每条必须写：options + chosen（owner preference）+ fallback + user-visible signals（如适用）。

## S1：TopRU 模式（finish-only vs in-flight 1s delta sampling）
- Maintainer GO:
  - [ ] Approved (name/date)
- Options:
  - Option A：finish-only
    - 含义：仅在 SQL finish 时产生 RU 增量。
    - 限制：长查询在执行中贡献为 0；近实时诊断能力较弱。
  - Option B：in-flight 1s delta sampling
    - 含义：执行中按 1s tick 采样 RUDetails（累积值）并计算 deltaRU；finish-path 再补最后一段。
    - 关键点：避免 double count；处理 RUDetails=nil/reset；异常退出的收敛规则。
- Chosen (owner preference)：Option B
- User-visible signals：
  - （建议）可观测采样次数/丢弃次数（如果不做 metrics，至少要有 debug log 说明）

## S2：ExecCount 语义（begin-based vs finish-based；RU=0 是否计数）
- Maintainer GO:
  - [ ] Approved (name/date)
- Options:
  - Option A：begin-based（与 TopSQL 语义一致）
    - 含义：exec_count 统计“开始次数”。
    - 注意：可能出现 TotalRU/ExecDuration > 0 但 exec_count == 0（跨窗口/跨 tick）。必须文档化。
  - Option B：finish-based（与 RU/Duration 绑定）
    - 含义：exec_count 在 RU/Duration 增量落地时同步递增（通常在 finish 或 finalize）。
    - 优点：用户更易理解；缺点：与 TopSQL 不一致。
- RU=0 是否计数：
  - Option A：deltaRU==0 时不计数
  - Option B：仍计数（需解释价值与噪音控制）
- Chosen (owner preference)：Option A（begin-based）

## S3：TopRU enabled/disabled 行为（disabled 时 drain+drop 是否允许；是否需要 drop signal）
- Maintainer GO:
  - [ ] Approved (name/date)
- Options:
  - Option A：disabled ⇒ 不采集、不上报、清理/绕过状态（无 drain+drop、无 drop 统计）
  - Option B：disabled 允许 drain+drop ⇒ 必须有 user-visible signals（metrics/logs）
- Chosen (owner preference)：Option A

## S4：Backpressure / drop policy（随机丢是否可接受；若不接受需 deterministic）
- Maintainer GO:
  - [ ] Approved (name/date)
- Options:
  - Option A：deterministic（推荐）
    - 示例：TopN + others；keep-largest RU；明确 caps；可预测合并。
  - Option B：允许随机丢
    - 必须显式接受，并必须可观测（signals）。
- Chosen (owner preference)：Option A

## S5：Report interval coupling（TopRU interval 是否允许影响 TopSQL cadence）
- Maintainer GO:
  - [ ] Approved (name/date)
- Options:
  - Option A：允许耦合（effective interval = min(TopSQL, TopRU)）
    - 用户可见：启用 TopRU 可能增加 TopSQL 上报频率。
  - Option B：完全分离（两套 ticker/pipeline；在 send/bundle 层合并）
- Chosen (owner preference)：Option A

## S6：Metadata availability（TopSQL 关闭 + TopRU 开启时是否必须注册 SQL/Plan meta）
- Maintainer GO:
  - [ ] Approved (name/date)
- Options:
  - Option A：TopProfilingEnabled（TopSQL OR TopRU）为 true 时，应尽量注册 SQL/Plan meta
    - Fallback：至少保证 SQLMeta；PlanMeta 可 best-effort/限流/抽样。
    - plan_digest 缺失：允许缺失但必须定义行为（drop/degrade/mark unknown）+ signals。
  - Option B：仅 digest，meta 可缺省
- Chosen (owner preference)：Option A

## Correlation / time_window（必须写死）
- correlation keys：user + sql_digest + plan_digest（若缺失按 S6 fallback）
- time_window 定义：
  - 时间来源：report window 对齐 wall-clock / sampling timestamp
  - 对齐规则：
  - drift/jitter 处理：

## Privacy / redaction
- SQL text / plan / params：
- redaction policy：

## Notes
- 本文件应与 tracks/resource-observability-topru/templates/TOPRU_SEMANTIC_SPEC.md 对齐；出现分歧时以 maintainer GO 的版本为准。
