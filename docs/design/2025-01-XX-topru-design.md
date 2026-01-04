# TiDB TopRU 设计

- Author(s): [Your Name](http://github.com/your-github-id)
- Discussion PR: https://github.com/pingcap/tidb/pull/XXX
- Tracking Issue: https://github.com/pingcap/tidb/issues/XXX

## Table of Contents

* [1. Introduction](#1-introduction)
  * [1.1 Background](#11-background)
  * [1.2 Goals](#12-goals)
  * [1.3 Non-Goals](#13-non-goals)
* [2. Project Management](#2-project-management)
* [3. Detailed Design](#3-detailed-design)
  * [3.1 功能与语义定义](#31-功能与语义定义)
  * [3.2 聚合与排序模型](#32-聚合与排序模型)
  * [3.3 数据模型与存储](#33-数据模型与存储)
  * [3.4 系统架构](#34-系统架构)
  * [3.5 性能与风险分析](#35-性能与风险分析)
  * [3.6 可扩展性与后续演进](#36-可扩展性与后续演进)
* [4. Limitation](#4-limitation)
* [5. Compatibility Issues](#5-compatibility-issues)
  * [5.1 Functional](#51-functional)
  * [5.2 Upgrade](#52-upgrade)
* [6. Test Design](#6-test-design)
  * [6.1 Functional Test](#61-functional-test)
  * [6.2 Compatibility Test](#62-compatibility-test)
  * [6.3 Performance Test](#63-performance-test)
* [7. Impact & Risk](#7-impact--risk)

## 1. Introduction

### 1.1 Background

Next-gen TiDB Cloud 按 RU（Request Unit） 计费。当集群 RU 消耗异常或达到限制时，用户需要快速定位高 RU 消耗的 SQL，但目前缺乏有效手段识别 RU 消耗的主要来源。

TopRU 的核心价值：当监控系统检测到 RU 限流事件时，TopRU 提供对高消耗 SQL 的近实时可见性，使用户能够：
•  按 RU 消耗排序，快速定位高资源消耗的 SQL
•  按用户维度聚合，识别资源消耗的来源账户
•  为后续的查询优化或资源治理提供数据依据

核心场景：当集群达到 MAX RCU 限制并触发告警时，用户需要快速定位高 RU 消耗的 SQL，以便执行 Terminate 或优化操作，解除资源压力并恢复服务。

当前 TiDB 已具备慢日志、Statement Summary 等能力，但在"按 RU 实时定位高消耗 SQL（尤其是执行中 SQL）"方面仍存在不足，因此本设计引入 **TopRU** 功能来补齐该能力。

**现有方案的局限**：

1. **慢日志 (Slow Log)**: 记录执行时间超过阈值的 SQL,包含完整的 RU 信息,但仅针对已完成的慢查询,无法实时反映正在执行中的 SQL RU 消耗情况。

2. **Statement Summary**: 提供 SQL 级别的聚合统计，但存在以下局限：
   - **持久化周期长**：默认 30 分钟持久化一次到系统表，O11y 系统和前端通过查询持久化数据展示
   - **无法直接访问实时数据**：无法访问 TiDB 内存中的实时数据，无法满足"分钟级实时定位高 RU SQL"的需求
   - **无法反映执行中 SQL**：仅统计已完成的 SQL，无法实时反映正在执行中的 SQL RU 消耗

**TopRU** 通过复用 TopSQL 基础设施，提供按 RU 消耗排序的近实时可观测能力，弥补上述不足。

### 1.2 Goals

1. **按 RU 消耗排序**: 支持按累计 RU 消耗进行排序和查询,识别高 RU SQL（包括执行时间短但 RU 消耗大的 SQL）

2. **支持用户维度聚合**: 按 `(user, sql_digest, plan_digest)` 三元组维度聚合,支持按用户进行资源治理和配额管理。

3. **实时 RU 统计与历史查询**: 
   - 本地 1 秒采样，支持执行中 SQL 的 RU 统计
   - 每 60 秒批量上报至外部组件（如 VM）
   - 支持查询最近时间段的 RU 消耗历史数据

4. **兼容现有能力**: 与 TopSQL 现有的 CPU 时间统计功能并存，互不影响

### 1.3 Non-Goals

1. **TopRU 中 RU 与 Billing RU 等价**:
   - TopRU 展示的 RU 来自 `util.RUDetails` 的运行时观测值,用于排障/定位高消耗 SQL
   - Billing 侧 RU 可能包含计费口径的聚合/取整/折扣/多维度归因等逻辑
   - 因此 **TopRU 的 RU 与 Billing RU 不保证一致**,本期不做“对齐计费口径”的工作

2. **执行中 TopRU 一定可展示完整 slow query / SQL statement 相关信息**:
   - 对于 TopRU 列表中的执行中 SQL,即使已消耗大量 RU,也可能因为对应 SQL 尚未完成时，无法获取到对应 slow query 或者 statement summary 相关详细信息。

3. **RU Baseline 智能对比**: 基于长期历史数据自动计算 RU 消耗 baseline（如 P95、平均值等），并自动标识当前 RU 消耗相对 baseline 的偏差程度、提供异常评分等高级分析功能。本期提供基础的历史数据查询能力（见 Goal 3），智能 baseline 对比作为后续扩展方向。

4. **异常检测与自动告警**: 自动检测 RU 消耗异常并生成告警的功能,作为后续扩展方向。

## 2. Project Management

### 2.1 里程碑规划

- **Phase 1**: 数据模型扩展(2 周)
  - 扩展 `StatementStatsItem` 添加 RU 字段
  - 扩展 `tsItem` 和 `record` 添加 RU 字段
  - 扩展聚合键添加 user 字段
  
- **Phase 2**: 数据采集实现(2 周)
  - 在 `OnExecutionFinished` 中获取 RU 数据
  - 实现 RU 数据的聚合逻辑
  - 更新 protobuf 消息定义
  
- **Phase 3**: 查询接口实现(2 周)
  - 实现 TopRU 查询接口，支持按 RU 排序
  - 支持按 user 维度查询
  - 实现 RU Share Percent 计算
  
- **Phase 4**: 测试与文档(2 周)
  - 功能测试、兼容性测试、性能测试
  - 文档完善

## 3. Detailed Design

### 3.1 功能与语义定义

#### 3.1.1 TopRU 的精确定义

**TopRU** 是 TiDB 提供的按 RU 消耗排序和查询 SQL 的可观测性功能，支持按用户维度聚合，帮助用户快速定位高 RU 消耗的 SQL。

**RU 定义**: RU (Request Unit) 表示 SQL 执行过程中累计的资源消耗。RU 包括读请求和写请求消耗的总和,通过 `util.RUDetails` 的 `RRU()` 和 `WRU()` 方法获取,最终计算为 `TotalRU = RRU + WRU`。

**RU 采集时机**（详细见 `3.2.1`）:
- 三层定期采样: **1s 本地采集 → 60s 上报/持久化 → 用户刷新(默认 60s，可配 15/30/60)**，均写入时间桶（基于 `PrecisionSeconds` 配置）
- 执行完成时补采一次,保证最终值准确

**聚合维度**:
- TopRU 的聚合键为 `(user, sql_digest, plan_digest)` 三元组
- `user`: 执行 SQL 的用户名,从 `SessionVars.User.Username` 获取
- `sql_digest`: SQL Digest,用于标识 SQL 语句模式
- `plan_digest`: Plan Digest,用于标识执行计划

#### 3.1.2 TopRU 的关键特性

| 特性 | TopRU |
|------|-------|
| **排序依据** | RU 消耗（累计 RRU + WRU） |
| **统计对象** | 已完成的 SQL + 执行中的 SQL（通过定期采样） |
| **更新频率** | 本地 1 秒采集，用户侧默认 60 秒刷新（可配置 15/30/60s） |
| **聚合维度** | User + SQL Digest + Plan Digest（三元组） |
| **时间窗口** | 基于时间桶机制（PrecisionSeconds），支持历史数据查询 |
| **数据保留** | 内存中短期保留，支持查询最近时间段的数据 |

**TopRU 的核心价值**:
1. **RU 视角**: 提供按 RU 消耗排序的能力，直接对应计费维度，更符合资源治理需求
2. **用户维度**: 支持按用户维度聚合，便于按用户进行资源配额管理和审计
3. **实时性**: 1 秒本地采样 + 可配置刷新间隔，满足实时排障需求
4. **执行中 SQL**: 支持观测正在执行的 SQL 的 RU 消耗，弥补慢日志和 Statement Summary 的不足

### 3.2 聚合与排序模型

#### 3.2.1 RU 数据的采集时机

**RU 采集机制**:

为了支持执行中 SQL 的实时 RU 统计,需要采用定期采样机制,而不是仅在执行完成时采集。采用三层采样机制,平衡实时性、性能和资源消耗。

**采集位置**:

1. **本地定期采样** (执行中 SQL):
   - 在 SQL 执行过程中,每 1 秒定期采集一次 RU 数据
   - 通过后台 goroutine 定期检查正在执行的 SQL,读取其 `util.RUDetails` 中的当前 RU 值
   - 采集点: 下沉到 `stmtstats.aggregator` 的 1s ticker，在 `m.aggregate()` 后追加 `m.ruAggregate()`（同一 tick 内完成 stmtstats 聚合与 RU 采样）
   - 采集到的 RU 增量先累加到 ruIncrementBuffer；每 10s 过滤 TopK/user 后再落桶到 `collecting.ruRecords`

2. **上报/持久化采样**:
   - 每 60 秒将本地时间桶的 RU 数据上报给其他组件用于持久化和管理
   - 类似 Prometheus 的上报机制,定期批量上报数据
   - 触发点: 复用 TopSQL 现有上报链路(由 `collectWorker` 的 reportTicker 触发 `takeDataAndSendToReportChan`,再由 `reportWorker` 发送)
   - 上报的数据可以用于持久化存储、告警、监控等用途

3. **用户查询刷新** (默认 60 秒，可配置 15s/30s/60s):
   - **说明**: 用户侧刷新在 VM 组件中处理
   - TopRU 提供查询接口,返回本地时间桶的 RU 数据
   - VM 组件按刷新间隔拉取并刷新用户可见的数据（默认 60 秒，可配置 15s/30s/60s）
   - 本文档仅在此处提及,详细实现见 VM 组件文档

4. **执行完成时采集** (补充机制):
   - SQL 执行完成时,采集最终的 RU 数据,确保最终数据准确
   - 采集点: `pkg/executor/adapter.go` 的 `observeStmtFinishedForTopSQL()` 方法
   - 用于补充定期采样可能遗漏的最终数据

**实现方案**:

#### 3.2.1.1 活跃 SQL 的执行上下文管理

**三层缓冲架构**：

为了解决内存控制问题，TopRU 采用三层缓冲设计：

```
层级          频率    数据结构                     对象类型       内存控制
─────────────────────────────────────────────────────────────────
1. executionContext  实时   StatementStats中           上下文      每 session 1 个
                            └─ ExecutionContext
                            
2. ruIncrementBuffer 1s    map[UserSQLPlan]           轻量级      上限保护
                            └─ RUIncrement (24字节)
                            
3. ruRecords        10s    map[UserSQLPlan]           重对象      100 users × 100 条
                            └─ record (100+字节)
                               └─ tsItem
```

**为什么不能直接写 ruRecords？**

1. **内存膨胀风险**：
   - 60s 上报周期内可能有数千个 `(user, sql, plan)` 组合
   - 直接创建 record/tsItem：60s × 1000 = **6 MB+**
   - 先 buffer 再过滤：100 users × 100 条 = **1 MB**

2. **对象复杂度差异**：
   - `RUIncrement`：24 字节（轻量级）
   - `record + tsItem`：100+ 字节（重对象，包含 map、slice）

3. **TopN 过滤时机**：
   - 每 1s 采集：可能采集到低 RU 的 SQL
   - 每 10s 过滤：对每个 timestamp 独立过滤，取 Top 100 users 的每个 Top 100 SQL

**关键概念区分**：

在 RU 采集中，需要区分以下概念：

1. **`aggregator.statsSet`**（活跃 Session 集合）：
   - 包含所有**活跃 session** 的 `StatementStats`（`Finished() == false`）
   - 每个 session 创建时注册到 `globalAggregator.statsSet`
   - Session 活跃 ≠ 正在执行 SQL
   - Session 可能在两个 SQL 之间的空闲状态、等待用户输入、或在事务中但未执行 SQL

2. **`StatementStats.executionContext`**（当前执行上下文）：
   - 标记该 session **当前是否正在执行 SQL**
   - `executionContext != nil` → 正在执行 SQL
   - `executionContext == nil` → 空闲（session 活跃但没有执行 SQL）
   - 存储执行上下文（`context.Context`）和 RU 采样状态

3. **`ruIncrementBuffer`**（轻量级中间缓冲）：
   - 类型：`map[uint64]RUIncrementsMap`（key 为 timestamp，参考 `stmtStatsBuffer`）
   - 1s 采样的 RU 增量先累加到这里（不创建 record/tsItem）
   - 每 10s 清空一次，对**每个 timestamp 独立**进行 TopK 过滤后写入 `ruRecords`
   - 作用：避免直接创建大量重对象导致内存膨胀

4. **`collecting.ruRecords`**（最终时间桶存储）：
   - 存储经过 TopK 过滤的 RU 数据（创建 record/tsItem 重对象）
   - 每 60s 上报一次
   - 内存可控：100 users × 100 条 = 10,000 条上限

**为什么需要 executionContext（而不是直接扩展 data 字段）？**

有三个主要原因：

1. **数据生命周期不同**:
   - `data` 字段会被 `Take()` 每秒取走并清空
   - 执行上下文需要在 SQL 执行期间持续存在
   - RU 采样的中间状态（`LastRUSample`）不能被清空

2. **data 不能包含执行上下文**:
   - `data` 存储的是聚合后的统计数据（`StatementStatsItem`）
   - `StatementStatsItem` 需要序列化并通过 protobuf 传输
   - `context.Context` 无法序列化
   - RU 采样需要访问 `util.RUDetails` 上下文

3. **Key 维度不同**:
   - `data` 的 key 是 `(sql_digest, plan_digest)`
   - RU 统计需要 `(user, sql_digest, plan_digest)` 三元组
   - RU 数据需要独立的存储结构

**ExecutionContext 结构设计**：

```go
// 扩展 StatementStats 结构
type StatementStats struct {
    data     StatementStatsMap  // 存储聚合后的统计数据(会被 Take() 取走)
    finished *atomic.Bool
    mu       sync.Mutex
    
    // 当前正在执行的 SQL 上下文
    // nil = session 空闲（没有正在执行的 SQL）
    // 说明: MySQL 协议下同一 session 同时只会有一条语句在执行
    executionContext *ExecutionContext
}

// ExecutionContext 存储当前执行的 SQL 上下文信息
type ExecutionContext struct {
    Ctx           context.Context   // 核心：用于读取 util.RUDetails
    LastRUSample  *atomic.Float64   // 核心：上次采样值，用于计算增量（配合 CAS 实现去重）
    
    // SQL 标识信息（用于关联到 ruRecords）
    SQLDigest  []byte
    PlanDigest []byte
    User       string
}
```

**并发去重设计（基于 CAS + finish 时清空 context）**：

TopRU 存在两条可能并发/交叠的 RU 采集路径：

1. **执行中采样**：`collectActiveRUInto()`（由 `aggregator.ruAggregate()` 每 1s 调度）
2. **执行完成补采**：`observeStmtFinishedForTopSQL()`（SQL 完成时补充最终 RU）

在边界时刻（例如 SQL 恰好在采样 tick 附近完成），这两条路径可能在非常短的时间窗口内先后触发，进而带来**重复采集/重复叠加**的风险。

**方案选择分析**：

| 方案 | 说明 | 评价 |
|------|------|------|
| LastSampleSeq | 用序列号标记“本轮已处理” | ❌ 无法解决真正的并发竞态（Load/Store 不是原子 read-modify-write） |
| **CAS + finish 清空 context** | CAS 确保增量计算原子性，finish 后清空 context | ✅ 推荐，逻辑简单且能真正解决并发问题 |

**推荐方案：CAS + finish 时立即清空 context**

设计要点：
- `collectRUDelta()` 使用 CAS 确保 `LastRUSample` 的原子更新，CAS 失败则放弃本次采样
- `OnExecutionFinished()` 在完成最终增量计算后立即清空 `executionContext`
- `collectActiveRUInto()` 检查 `executionContext == nil` 则跳过

**设计说明**：
- **为什么 CAS 失败直接放弃而不自旋？** CAS 失败说明 finish 路径正在处理，最终值会被正确记录，丢失一次中间采样的增量是可接受的（误差 < 1s 的 RU 增量）
- **为什么 finish 要先清空 context？** 让采样路径发现 `executionContext == nil` 后直接跳过，避免后续的重复累加
- **性能影响？** 即使有 1000 个 `StatementStats`，CAS 竞争概率也很低（竞争窗口 ~1ms，实际竞争 < 0.1%）

**执行上下文的生命周期管理**：

```go
// 方法 1: 注册执行上下文（SQL 开始执行时）
func (s *StatementStats) StartExecution(sqlDigest, planDigest []byte, user string, ctx context.Context) {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    s.executionContext = &ExecutionContext{
        Ctx:           ctx,
        LastRUSample:  atomic.NewFloat64(0.0),
        SQLDigest:     sqlDigest,
        PlanDigest:    planDigest,
        User:          user,
    }
}

// 方法 2: 注销执行上下文（SQL 执行完成时）
func (s *StatementStats) FinishExecution() {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.executionContext = nil
}

// 方法 3: 获取执行上下文（RU 采样时）
func (s *StatementStats) GetExecutionContext() *ExecutionContext {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.executionContext
}
```

**在 adapter.go 中的使用**：

```go
// 在 observeStmtBeginForTopSQL 中注册
func (a *ExecStmt) observeStmtBeginForTopSQL(ctx context.Context) context.Context {
    // ... 现有代码 ...
    
    if stats := a.Ctx.GetStmtStats(); stats != nil && topsqlstate.TopSQLEnabled() {
        sqlDigest, planDigest := a.getSQLPlanDigest()
        user := ""
        if a.Ctx.GetSessionVars().User != nil {
            user = a.Ctx.GetSessionVars().User.Username
        }
        
        // 注册执行上下文
        stats.StartExecution(sqlDigest, planDigest, user, ctx)
    }
    
    return ctx
}

// 在 observeStmtFinishedForTopSQL 中注销（简化版）
// 说明：RU 增量计算逻辑已收敛到 OnExecutionFinished 内部
func (a *ExecStmt) observeStmtFinishedForTopSQL() {
    vars := a.Ctx.GetSessionVars()
    if vars == nil {
        return
    }
    if stats := a.Ctx.GetStmtStats(); stats != nil && topsqlstate.TopSQLEnabled() {
        sqlDigest, planDigest := a.getSQLPlanDigest()
        execDuration := vars.GetTotalCostDuration()
        
        // 获取最终的 RU 数据
        var finalRU float64
        if ruDetailsVal := a.GoCtx.Value(util.RUDetailsCtxKey); ruDetailsVal != nil {
            ruDetails := ruDetailsVal.(*util.RUDetails)
            finalRU = ruDetails.RRU() + ruDetails.WRU()
        }
        
        // 获取用户名
        user := ""
        if vars.User != nil {
            user = vars.User.Username
        }

        // 统一调用：OnExecutionFinished 内部完成 RU delta 计算 + buffer 写入 + executionContext 清理
        stats.OnExecutionFinished(sqlDigest, planDigest, execDuration, vars.OutPacketBytes.Load(), user, finalRU)
    }
}
```

**RU 采集的统一实现**：

为了提高可读性和维护性，将"执行中采样"和"执行完成采集"的 RU 增量计算逻辑统一收敛到 `StatementStats` 内部：

```go
// collectRUDelta：统一的 RU 增量计算逻辑
// 参数 currentRU: 当前 RU 值（执行中采样时从 RUDetails 读取，执行完成时为 finalRU）
// 返回 (key, delta)：用于后续聚合
func (s *StatementStats) collectActiveRUDelta(currentRU float64) (key UserSQLPlanDigest, delta float64, ok bool) {
    execCtx := s.GetExecutionContext()
    if execCtx == nil {
        return UserSQLPlanDigest{}, 0, false
    }
    
    // 计算增量
    lastRU := execCtx.LastRUSample.Load()
    delta = currentRU - lastRU
    if delta <= 0 {
        return UserSQLPlanDigest{}, 0, false
    }

    // CAS 失败说明有并发更新（finish 路径赢了），直接放弃
    if !execCtx.LastRUSample.CompareAndSwap(lastRU, currentRU) {
		return UserSQLPlanDigest{}, 0, false
    }
    
    // 构建 key
    key = UserSQLPlanDigest{
        User:       execCtx.User,
        SQLDigest:  BinaryDigest(execCtx.SQLDigest),
        PlanDigest: BinaryDigest(execCtx.PlanDigest),
    }
    return key, delta, true
}

func (s *StatementStats) collectFinalRUDelta(currentRU float64) (key UserSQLPlanDigest, delta float64, ok bool) {
    execCtx := s.GetExecutionContext()
    if execCtx == nil {
        return UserSQLPlanDigest{}, 0, false
    }

    for {
        lastRU := execCtx.LastRUSample.Load()
        delta = finalRU - lastRU
        if delta <= 0 {
            return UserSQLPlanDigest{}, 0, false  // 无增量或已被处理完
        }
        
        // CAS 成功则返回；失败则重试（采样路径可能刚更新了 lastRU）
        if execCtx.LastRUSample.CompareAndSwap(lastRU, finalRU) {
            key = UserSQLPlanDigest{
                User:       execCtx.User,
                SQLDigest:  BinaryDigest(execCtx.SQLDigest),
                PlanDigest: BinaryDigest(execCtx.PlanDigest),
            }
            return key, delta, tru
        }
        // CAS 失败，重新计算剩余增量
    }

    return UserSQLPlanDigest{}, 0, false
}

// collectActiveRUInto：执行中采样时调用（由 ruAggregate 调度）
// 从 executionContext.Ctx 读取当前 RU，计算增量并聚合到 total
func (s *StatementStats) collectActiveRUInto(total RUIncrementsMap) {
    execCtx := s.GetExecutionContext()
    if execCtx == nil {
        return
    }
    
    // 从 context 中读取当前 RU
    ruDetailsVal := execCtx.Ctx.Value(util.RUDetailsCtxKey)
    if ruDetailsVal == nil {
        return
    }
    ruDetails := ruDetailsVal.(*util.RUDetails)
    currentRU := ruDetails.RRU() + ruDetails.WRU()
    
    // 复用统一的增量计算逻辑
    if key, delta, ok := s.collectActiveRUDelta(currentRU); ok {
        key := UserSQLPlanDigest{
            User:       execCtx.User,
            SQLDigest:  BinaryDigest(execCtx.SQLDigest),
            PlanDigest: BinaryDigest(execCtx.PlanDigest),
        }
        total.Add(key, delta)
    }
}

// 10s 过滤阶段：按 timestamp 独立处理，每个 timestamp 内按 user TopK 后写入 collecting.ruRecords
// 说明：ruIncrementBuffer 的 key 是 timestamp，每个 timestamp 的数据独立处理，无跨时间聚合
func (tsr *RemoteTopSQLReporter) processRUIncrementBuffer() {
    tsr.ruBufferMu.Lock()
    buffer := tsr.ruIncrementBuffer
    tsr.ruIncrementBuffer = make(map[uint64]RUIncrementsMap)  // 重置 buffer
    tsr.ruBufferMu.Unlock()
    
    // 按 timestamp 逐个处理（每秒的数据独立过滤）
    for timestamp, ruIncMap := range buffer {
        tsr.processRUIncrementsForTimestamp(timestamp, ruIncMap)
    }
}

// processRUIncrementsForTimestamp：处理单个 timestamp 内的 RU 增量数据
// 每个 timestamp 独立做 TopN 过滤：Top 100 users × Top 100 SQL/user
func (tsr *RemoteTopSQLReporter) processRUIncrementsForTimestamp(timestamp uint64, ruIncMap RUIncrementsMap) {
    // 提取全局 others
    othersKey := stmtstats.UserSQLPlanDigest{
        User:       "",
        SQLDigest:  nil, // 和 topSQL 的 sqlDigest 保持一致
        PlanDigest: nil, // 和 topSQL 的 planDigest 保持一致
    }
    var globalOthersRU float64
    if othersInc, ok := ruIncMap[othersKey]; ok {
        globalOthersRU = othersInc.TotalRU
        delete(ruIncMap, othersKey)
    }

    // 按 user 分组
    userDataMap := make(map[string]map[UserSQLPlanDigest]float64)
    for key, inc := range ruIncMap {
        if _, ok := userDataMap[key.User]; !ok {
            userDataMap[key.User] = make(map[UserSQLPlanDigest]float64)
        }
        userDataMap[key.User][key] = inc.TotalRU
    }
    
    // 每个 user 取 Top 100，并限制 user 总数 100
    // 说明：10s 过滤和 60s 上报都是针对每个 timestamp 独立处理，取相同的 TopN 阈值
    topUsers := selectTopUsers(userDataMap, 100)
    var totalEvictedRU float64

    for user, data := range topUsers {
        topN, evicted := getTopNRU(data, 100)  // Top 100 SQL per user
        
        // 现在才创建 record/tsItem（重对象）
        for key, ru := range topN {
            record := tsr.collecting.getOrCreateRURecord(
                user,
                []byte(key.SQLDigest),
                []byte(key.PlanDigest),
            )
            tsItem := record.getOrCreateTsItem(timestamp)
            tsItem.stmtStats.TotalRU += ru
        }

        // evicted 部分累加到总量
        for _, ru := range evicted {
            totalEvictedRU += ru
        }
    }

    // 将所有 evicted RU + 全局 others 一次性写入
    totalEvictedRU += globalOthersRU
    if totalEvictedRU > 0 {
        tsr.collecting.appendOthersRU(timestamp, totalEvictedRU)
    }
}
```

**设计要点总结**：

1. **简化判断**：通过 `GetExecutionContext() != nil` 判断是否正在执行，不需要额外的比较逻辑
2. **直接访问**：ExecutionContext 中保存了所有必要信息（Ctx、SQLDigest、PlanDigest、User），无需从其他地方获取
3. **线程安全**：使用 mutex 保护 executionContext 的读写
4. **生命周期清晰**：
   - SQL 开始：`StartExecution()` 创建 executionContext
   - RU 采样：`GetExecutionContext()` 读取并更新 LastRUSample
   - SQL 完成：`FinishExecution()` 清空 executionContext

**优势**：
1. ✅ 复用 aggregator 机制，无需新建 ActiveSQLContextManager
2. ✅ 自动管理：aggregator 会自动清理已完成的 StatementStats（`Finished() == true`）
3. ✅ 减少复杂度：执行上下文的生命周期与 StatementStats 绑定
4. ✅ 线程安全：复用 StatementStats 现有的 mutex 保护

**本地采集实现(复用 aggregator)**:

RU 采集采用与 CPU 时间相同的 1 秒周期读取 `util.RUDetails`，并将“执行中采样”下沉到 `stmtstats.aggregator` 的 1s tick 中（`m.aggregate()` 后追加 `m.ruAggregate()`）；写入时间桶仍采用“10 秒过滤后落桶”的方式以控制内存。

```go
type RUIncrement struct {
    TotalRU float64
}

// RUIncrementsMap：每 1s tick 聚合出来的 RU 增量（key 带 user 维度）
type RUIncrementsMap map[UserSQLPlanDigest]*RUIncrement

// ruIncrementBuffer 定义（参考 stmtStatsBuffer）
// key: timestamp，value: 该时间戳内的 RU 增量数据
type ruIncrementBuffer map[uint64]RUIncrementsMap

// RemoteTopSQLReporter 增加轻量级 buffer(仅示意)
type RemoteTopSQLReporter struct {
    // ... existing fields ...
    ruIncrementBuffer ruIncrementBuffer  // key: timestamp, value: RUIncrementsMap
    ruBufferMu        sync.Mutex

    // 可配置项（默认值示意）
    ruBufferFlushInterval time.Duration // 10s
    ruTopKPerUser         int           // 100（10s 和 60s 均使用相同阈值）
    ruMaxUsers            int           // 100
    ruMaxBufferEntries    int           // 保护阈值：防止极端场景 buffer 无界增长
}

// RemoteTopSQLReporter 实现 stmtstats.RUCollector：接收 aggregator 每 1s 投递的 RU 增量
// 说明：这里保持"采集快、处理慢"的风格：采集只做累加到轻量级 map，复杂 TopK/user 放在 10s 的 processRUIncrementBuffer 里做。
func (tsr *RemoteTopSQLReporter) CollectRUIncrements(incr stmtstats.RUIncrementsMap) {
    tsr.ruBufferMu.Lock()
    defer tsr.ruBufferMu.Unlock()
    for key, delta := range incr {
        if delta <= 0 {
            continue
        }
        if tsr.ruMaxBufferEntries > 0 && len(tsr.ruIncrementBuffer) >= tsr.ruMaxBufferEntries {
            // 可按策略淘汰/汇总到 others
            othersKey := stmtstats.UserSQLPlanDigest{
                User:       "",  // 空字符串表示全局 others
                SQLDigest:  nil, // 和 topSQL 的 sqlDigest 保持一致
                PlanDigest: nil, // 和 topSQL 的 planDigest 保持一致
            }
            
            inc := tsr.ruIncrementBuffer[othersKey]
            if inc == nil {
                inc = &RUIncrement{}
                tsr.ruIncrementBuffer[othersKey] = inc
            }
            inc.totalRU += delta
            
            reporter_metrics.IgnoreRUBufferFullCounter.Inc()
            continue
        }

        inc := tsr.ruIncrementBuffer[key]
        if inc == nil {
            inc = &RUIncrement{}
            tsr.ruIncrementBuffer[key] = inc
        }
        inc.totalRU += delta
    }
}

// RU 采样下沉到 stmtstats.aggregator：
// - aggregator.run() 的 tick 里，在 m.aggregate() 后追加 m.ruAggregate()
// - m.ruAggregate() 遍历 statsSet，对每个 StatementStats 做一次 RU 采样，并把增量投递给 RUCollector（TopSQL reporter）
//
// 这样可以保证“执行中采样”与 stmtstats 聚合共享同一个 1s tick，避免在 reporter 侧重复扫描 statsSet。
func (m *aggregator) run() {
    tick := time.NewTicker(time.Second)
    defer tick.Stop()
    for {
        select {
        case <-m.ctx.Done():
            return
        case <-tick.C:
            m.aggregate()
            m.ruAggregate()
        }
    }
}

// RUCollector 是可选扩展接口：不改变现有 Collector（CollectStmtStatsMap）即可接入 RU 采样数据。
// 仅 TopSQL reporter 需要实现该接口。
type RUCollector interface {
    CollectRUIncrements(RUIncrementsMap)
}

// RUIncrementsMap：每 1s tick 聚合出来的 RU 增量（key 带 user 维度）
type RUIncrementsMap map[UserSQLPlanDigest]float64

// ruAggregate：遍历所有活跃 StatementStats，采样 executionContext 的 RUDetails，计算增量后聚合并发送给 RUCollector
func (m *aggregator) ruAggregate() {
    if !state.TopSQLEnabled() {
        return
    }

    total := RUIncrementsMap{}
    m.statsSet.Range(func(statsR, _ any) bool {
        stats := statsR.(*StatementStats)
        if stats.Finished() {
            // aggregate() 已处理 unregister，这里防御性跳过即可
            return true
        }
        // 在 StatementStats 内部完成 RUDetails 读取 + delta 计算 + executionContext 收尾
        stats.collectActiveRUInto(total)
        return true
    })
    if len(total) == 0 {
        return
    }
    m.collectors.Range(func(c, _ any) bool {
        if rc, ok := c.(RUCollector); ok {
            rc.CollectRUIncrements(total)
        }
        return true
    })
}
```

`processRUIncrementBuffer()` 函数实现见上文 `collectActiveRUInto` 代码块后的定义。

**与 aggregator 常规收集流程的区别**:

1. **采集频率不同**:
   - aggregator 常规收集: 1 秒一次,调用 `CollectStmtStatsMap` 收集 CPU 时间等数据
   - RU 采集: 1 秒一次,与 CPU 时间采集对齐

2. **数据处理方式不同**:
   - aggregator 常规收集: 调用 `stats.Take()`,会清空 `StatementStats.data`,数据被取走
   - RU 采集: 不调用 `Take()`，先写入轻量级 buffer，并在 10s 周期过滤后落桶

3. **数据流向不同**:
   - aggregator 常规收集: StatementStats → aggregator → Collector.CollectStmtStatsMap(...) → reporter.stmtStatsBuffer → collecting.records
   - RU 采集(下沉到 aggregator): StatementStats.executionContext → aggregator.ruAggregate() → RUCollector.CollectRUIncrements(...) → reporter.ruIncrementBuffer(10s) → processRUIncrementBuffer() → collecting.ruRecords 时间桶

**RU 差值处理逻辑**:

1. **首次采样**:
   - `LastRUSample = 0`
   - `currentRU = 100` (假设)
   - `ruDelta = 100 - 0 = 100`
   - 将 100 RU 累加到 ruIncrementBuffer

2. **后续采样(1秒后)**:
   - `LastRUSample = 100`
   - `currentRU = 250` (假设 RU 继续增长)
   - `ruDelta = 250 - 100 = 150`
   - 将 150 RU 累加到 ruIncrementBuffer
   - 每 10 秒触发一次过滤，将 TopK/user 落入 collecting 时间桶

3. **处理边界情况**:
   - 如果 `ruDelta <= 0`: 跳过本次采样(理论上不应该发生,但需要防护)
   - 如果 `util.RUDetails` 为空: 跳过本次采样
   - 如果 SQL 执行完成: 从活跃列表中移除,不再采样

**两个数据来源的处理**:

1. **定期采样(1秒)**:
   - **数据来源**: 
     - 通过 aggregator 获取所有活跃的 `StatementStats`
     - 从每个 `StatementStats.executionContext` 中读取活跃执行的上下文
     - 从执行上下文中读取 `util.RUDetails` 的当前值
   - **处理方式**: 
     - 计算 RU 差值(增量): `ruDelta = currentRU - LastRUSample`
     - 将差值累加到 ruIncrementBuffer（轻量级，仅增量）
     - 更新活跃执行的 `LastRUSample = currentRU`(使用原子操作)
     - 每 10s 调用 `processRUIncrementBuffer()`：按 user 分组取 TopK 并写入 `collecting.ruRecords`
   - **存储位置**: 最终落桶到 `collecting.ruRecords[userSQLPlanDigest].tsItems[timestamp].stmtStats.TotalRU`
   - **特点**: 
     - 每次采样只记录增量,避免重复计算
     - 复用 aggregator 机制,无需维护独立的活跃 SQL 列表
     - 与 aggregator 的常规收集对齐(1秒),可以更好地复用现有机制
     - RU 数据按 user 维度存储,支持每个 user 的 Top 100

2. **执行完成时采集**:
   - **数据来源**: SQL 执行完成时,从 `util.RUDetails` 读取最终值
   - **处理方式**: 
     - 将“finalRU 与 LastRUSample 的差值计算 + executionContext 清理”收敛到 `StatementStats.OnExecutionFinished` 内部
     - `observeStmtFinishedForTopSQL()` 只负责读取 `finalRU/user/sqlDigest/planDigest` 并调用扩展后的 `OnExecutionFinished(..., user, finalRU)`
     - `OnExecutionFinished` 内部：
       - 读取并校验 `executionContext` 是否匹配当前语句
       - 计算 `ruDelta = finalRU - LastRUSample`
       - 将 `ruDelta` 作为一次“增量事件”写入 **本地 buffer**（由 reporter 负责 10s 过滤落桶）
       - 清理 `executionContext`
   - **存储位置**: 与定期采样一致,最终落桶到 `collecting.ruRecords[userSQLPlanDigest].tsItems[timestamp].stmtStats.TotalRU`
   - **作用**: 确保最终数据准确,补充定期采样可能遗漏的数据(特别是执行时间 < 1秒的 SQL)

执行完成时的 RU 采集代码见 `3.2.1.1` 中的 `observeStmtFinishedForTopSQL()` 函数。

```go
// 统一命名风格：类似 GetOrCreateStatementStatsItem，这里提供 GetOrCreateRUIncrementItem
type RUIncrementItem struct {
    TotalRU float64
}

// GetOrCreateRUIncrementItem：获取或创建 RU 增量条目
// 说明：接收 UserSQLPlanDigest key，与 collectRUDelta 返回的 key 一致
func (s *StatementStats) GetOrCreateRUIncrementItem(key UserSQLPlanDigest) *RUIncrementItem {
    s.ruMu.Lock()
    defer s.ruMu.Unlock()
    if s.ruIncrements == nil {
        s.ruIncrements = make(map[UserSQLPlanDigest]*RUIncrementItem)
    }
    item := s.ruIncrements[key]
    if item == nil {
        item = &RUIncrementItem{}
        s.ruIncrements[key] = item
    }
    return item
}

// OnExecutionFinished（扩展）示意：在 StatementStats 内部完成 RU delta 计算与 executionContext 清理
func (s *StatementStats) OnExecutionFinished(sqlDigest, planDigest []byte, execDuration time.Duration, outNetworkBytes uint64,
    user string, finalRU float64) {
    // 1) 复用原有逻辑：写入 stmtstats data（network/latency 等）
    //    ...（省略，与现有实现一致）...

    // 2) TopRU 收尾：复用统一的增量计算逻辑
    //    说明：session 串行执行 SQL，executionContext 一定是当前 SQL 的，无需比较 digest
    if key, delta, ok := s.collectFinalRUDelta(finalRU);; ok {
        // 将增量写入本地 RU buffer（等待 aggregator.ruAggregate 汇总投递）
        item := s.GetOrCreateRUIncrementItem(key)
        item.TotalRU += delta
    }
    
    // 3) 清理 executionContext（避免执行完成后仍被执行中采样扫描）
    s.FinishExecution()
}
```

#### 3.2.1.3 上报/持久化流程(60秒)

**复用现有的 collectWorker 函数（10s 过滤落桶在这里做）**:

RU 的 1s 采样下沉到 `stmtstats.aggregator.ruAggregate()`，TopSQL reporter 侧保留：
- **10s**：把 `ruIncrementBuffer` 做 TopK/user 过滤并落桶到 `collecting.ruRecords`
- **60s**：复用 reportTicker 触发 `takeDataAndSendToReportChan()` 上报并清空 collecting

```go
// collectWorker（复用 + 小幅扩展：增加 10s RU 过滤落桶）
func (tsr *RemoteTopSQLReporter) collectWorker() {
    ruProcessTicker := time.NewTicker(10 * time.Second)
    defer ruProcessTicker.Stop()

    currentReportInterval := topsqlstate.GlobalState.ReportIntervalSeconds.Load() // 默认 60 秒
    reportTicker := time.NewTicker(time.Second * time.Duration(currentReportInterval))
    defer reportTicker.Stop()
    for {
        select {
        // ... 其他 case（CPU/stmtstats 的 channel）...
        case <-ruProcessTicker.C:
            tsr.processRUIncrementBuffer()
        case <-reportTicker.C:
            tsr.processStmtStatsData()      // 处理 CPU/StmtStats 数据
            tsr.takeDataAndSendToReportChan() // 取数据并发送上报(包括 RU 数据)
            if newInterval := topsqlstate.GlobalState.ReportIntervalSeconds.Load(); newInterval != currentReportInterval {
                currentReportInterval = newInterval
                reportTicker.Reset(time.Second * time.Duration(currentReportInterval))
            }
        }
    }
}

// takeDataAndSendToReportChan 函数扩展(需要修改)
func (tsr *RemoteTopSQLReporter) takeDataAndSendToReportChan() {
    // Send to report channel. When channel is full, data will be dropped.
    select {
    case tsr.reportCollectedDataChan <- collectedData{
        collected:         tsr.collecting.take(),  // 包括 RU 数据(ruRecords)
        normalizedSQLMap:  tsr.normalizedSQLMap.take(),
        normalizedPlanMap: tsr.normalizedPlanMap.take(),
        // RU 数据在 collected 中一起上报
    }:
    default:
        // ignore if chan blocked
        reporter_metrics.IgnoreReportChannelFullCounter.Inc()
    }
}
```

**RU 数据在上报流程中的位置**:

```go
// collecting.take() 会同时取走 CPU 数据和 RU 数据
func (c *collecting) take() *collecting {
    r := &collecting{
        records: c.records,      // CPU/StmtStats 数据
        ruRecords: c.ruRecords,  // RU 数据(新增)
        // ...
    }
    c.records = map[string]*record{}
    c.ruRecords = map[string]*record{}  // 清空 RU 数据
    return r
}

// 在 DataSink 中,RU 数据与 CPU 数据一起上报
func (ds *DataSink) TrySend(data *ReportData) {
    // ReportData.DataRecords 包含 CPU/StmtStats 数据
    // ReportData.RURecordsByUser 包含按 user 分组的 RU 数据(新增字段)
    ds.sendTopSQLRecords(ctx, data.DataRecords)
    ds.sendRURecordsByUser(ctx, data.RURecordsByUser)  // 新增 RU 数据上报
}


**聚合语义**:

- 相同 `(user, sql_digest, plan_digest)` 的所有执行会被聚合到一起
- 聚合后的指标包括 TopSQL 现有的指标(CPU 时间、执行次数等)加上新增的 RU 指标:
  - `TotalRU`: 所有执行的累计 RU 消耗
  - `RUSharePercent`: 该 SQL 的 RU 消耗占总 RU 的百分比(查询时计算)

**用户隔离**:

- 不同用户的相同 SQL 会被分别统计
- 支持按用户维度进行资源配额管理和审计
- 支持查询特定用户的 Top RU SQL

### 3.3 数据模型与存储

#### 3.3.1 User 维度的 RU 存储设计

**问题分析**:

当前 TopSQL 的数据流:
```
StatementStats.data (key: sql_digest + plan_digest)
    ↓ Take()
stmtStatsBuffer (key: sql_digest + plan_digest)
    ↓ processStmtStatsData()
collecting.records (key: sql_digest + plan_digest)
    ↓ getReportRecords()
records (按 totalCPUTimeMs 排序,取 Top N)
```

**RU 数据的关键差异**:

1. **Key 不同**: RU 需要 `(user, sql_digest, plan_digest)` 作为 key,而现有流程使用 `(sql_digest, plan_digest)`
2. **存储位置**: RU 数据需要存储到 `record.tsItems[timestamp].stmtStats.TotalRU`
3. **TopN 策略**: PM 需求是**每个 user 存 top100 RU 记录,user 上限 100**

**设计方案对比**（关注对现有 TopSQL 的影响）:

方案 A: 直接扩展 `collecting.records` 的 key 为 `(user, sql_digest, plan_digest)`（将 **TopSQL 原本的主干 records** 也变成 user 维度）
- **变化点**:
  - CPU/RU 共享同一 `records` map；`TopN/evicted/others/encodeKey` 等全部需要引入 user 维度
  - 等价于把 TopSQL 从“全局按 `(sql, plan)` 聚合”改成“按 `(user, sql, plan)` 聚合”
- **优点**:
  - 数据结构看起来最“统一”：所有指标（CPU/RU/stmtstats）都天然带 user，查询模型简单
  - 后续如果要做“按 user 的 TopSQL by CPU”，改动更少
- **缺点/对现有 TopSQL 的影响（关键）**:
  - **语义变化/兼容性风险**: 现有 TopSQL 主要面向“全局 Top SQL（不分 user）”。方案 A 会让 CPU TopN 也变成“按 user 切分后的 TopN”，导致原有 TopSQL 结果含义变化；若要保持原语义，需要额外引入“全局汇总视图（user=all/empty）”或双索引，复杂度反而上升
  - **侵入性高**: 需要改动 reporter/datamodel 主干路径（`processCPUTimeData`、`collecting.records`、`evicted/others`、`encodeKey`、`toProto` 等），并牵动下游消费方（DataSink/Proto/展示）对 key 的理解
  - **性能与内存风险**: key 基数从 `(sql, plan)` 变为 `(user, sql, plan)`，records 数量可能显著膨胀（用户数 × SQL 数），TopN/eviction 的计算与内存占用均上升；即使 RU 侧有 “user≤100 & top100/user” 的限制，CPU 侧也会被动承受 user 维度膨胀
  - **长期维护成本高**: 后续 TopSQL 任意优化/修 bug 都要同时考虑 user 维度与非 user 维度语义，耦合更强

方案 B: 保持 `collecting.records` 不变，在 `tsItem.stmtStats` 内嵌 `RUByUser`，查询时再分组
- **优点**:
  - `collecting.records` 与现有 CPU TopSQL 主干完全不动，侵入性最低
  - 理论上只需要在 `StatementStatsItem/tsItem` 上扩展一个 `map[user]ru` 的表达
- **缺点**:
  - **内存/GC 风险**: 每个时间桶的 `tsItem` 可能携带 `map[user]ru`，高基数场景会造成大量小 map、GC 压力与热点锁竞争
  - **实现复杂/易出错**: 需要定义 `RUByUser` 的 merge/序列化/裁剪语义（尤其是跨 bucket/跨分钟窗口合并）
  - **难以满足“每 user Top100 & user≤100”**: 这种限制更适合在采集/存储层做裁剪；方案 B 往往只能在查询时分组再 TopN，导致查询端开销大且不可控

方案 C: 分层存储（当前实现使用）
- 做法: `collecting.records` 仍按 `(sql_digest, plan_digest)` 存 CPU；新增 `collecting.ruRecords` 按 `(user, sql_digest, plan_digest)` 专存 RU
- 优点:
  - 与现有 TopSQL CPU 流程完全兼容，侵入性低
  - TopN/eviction 保持独立，按 user 维度做 Top100；逻辑清晰
  - 复用现有 `collectWorker/reportWorker` 上报链路，仅扩展 payload
- 缺点:
  - 多一份 RU map（ruRecords），内存略增；需要在 DataSink/Proto 中带上 RU payload

**最终选择**: 方案 C（分层存储）。
- **选择理由**:
  - 直接满足“每 user Top100 & user≤100”的产品约束，并且可以在 RU 存储层就进行裁剪与隔离
  - 不改变现有 TopSQL（CPU TopN）的语义与主干实现，避免方案 A 的兼容性与回归风险
  - 与 `collectWorker/reportWorker` 的 60s 上报链路天然匹配，只需扩展 RU payload；长期维护边界清晰

#### 3.3.2 StatementStatsItem 扩展

**现有结构** (`pkg/util/topsql/stmtstats/stmtstats.go`):

```go
type StatementStatsItem struct {
    KvStatsItem KvStatementStatsItem
    ExecCount uint64
    SumDurationNs uint64
    DurationCount uint64
    NetworkInBytes uint64
    NetworkOutBytes uint64
}
```

**扩展后结构**:

```go
type StatementStatsItem struct {
    KvStatsItem KvStatementStatsItem
    ExecCount uint64
    SumDurationNs uint64
    DurationCount uint64
    NetworkInBytes uint64
    NetworkOutBytes uint64
    
    // 新增 RU 相关字段
    TotalRU float64  // 累计总 RU
}

// Merge 方法扩展
func (i *StatementStatsItem) Merge(other *StatementStatsItem) {
    // ... 现有合并逻辑 ...
    i.TotalRU += other.TotalRU
}
```

#### 3.3.3 tsItem 扩展

**现有结构** (`pkg/util/topsql/reporter/datamodel.go`):

```go
type tsItem struct {
    stmtStats stmtstats.StatementStatsItem
    timestamp uint64
    cpuTimeMs uint32
}
```

**扩展说明**:

由于 `tsItem` 已经包含 `stmtStats stmtstats.StatementStatsItem`,而 `StatementStatsItem` 已扩展包含 RU 字段,因此 `tsItem` 无需额外修改。RU 数据通过 `tsItem.stmtStats.TotalRU` 访问。

#### 3.3.4 record 扩展

**现有结构**:

```go
type record struct {
    tsIndex        map[uint64]int
    sqlDigest      []byte
    planDigest     []byte
    tsItems        tsItems
    totalCPUTimeMs uint64
}
```

**扩展后结构**:

```go
type record struct {
    tsIndex        map[uint64]int
    sqlDigest      []byte
    planDigest     []byte
    user           string  // 新增: 用户名
    tsItems        tsItems
    totalCPUTimeMs uint64
    totalRU        float64 // 新增: 累计总 RU
}
```

#### 3.3.5 collecting 扩展支持 user 维度

**扩展 collecting 结构**:

```go
// collecting 扩展,支持 user 维度的 RU 数据
type collecting struct {
    records map[string]*record             // sqlPlanDigest => record (现有,用于 CPU 时间)
    evicted map[uint64]map[string]struct{} // { sqlPlanDigest } (现有)
    keyBuf  *bytes.Buffer                  // 现有
    
    // 新增: user 维度的 RU 数据
    ruRecords map[string]*record              // userSQLPlanDigest => record (key: user + sqlDigest + planDigest)
    ruEvicted map[uint64]map[string]struct{}  // { userSQLPlanDigest } (按 user 分组的 evicted 记录)
    ruKeyBuf  *bytes.Buffer                   // 用于编码 userSQLPlanDigest key
}

// 扩展后的 key 编码
func encodeRUKey(buf *bytes.Buffer, user string, sqlDigest, planDigest []byte) string {
    buf.Reset()
    buf.WriteString(user)
    buf.WriteByte('|')  // 分隔符
    buf.Write(sqlDigest)
    buf.Write(planDigest)
    return buf.String()
}

// 扩展 getOrCreateRecord,支持 user 维度
func (c *collecting) getOrCreateRURecord(user string, sqlDigest, planDigest []byte) *record {
    key := encodeRUKey(c.ruKeyBuf, user, sqlDigest, planDigest)
    r, ok := c.ruRecords[key]
    if !ok {
        r = newRURecord(user, sqlDigest, planDigest)
        c.ruRecords[key] = r
    }
    return r
}

func newRURecord(user string, sqlDigest, planDigest []byte) *record {
    listCap := min(topsqlstate.GlobalState.ReportIntervalSeconds.Load()/topsqlstate.GlobalState.PrecisionSeconds.Load()+1, maxTsItemsCapacity)
    return &record{
        sqlDigest:  sqlDigest,
        planDigest: planDigest,
        user:       user,  // 新增 user 字段
        tsItems:    make(tsItems, 0, listCap),
        tsIndex:    make(map[uint64]int, listCap),
    }
}
```

**Top 100 选择逻辑(每个 user)**:

```go
// 扩展 getReportRecords,支持按 user 分组取 Top 100
const (
    maxRUUsers     = 100  // user 上限
    topNRUPerUser  = 100  // 每个 user 的 Top N
)

// 按 user 计算每个 record 的 totalRU
func (r *record) calculateTotalRU() float64 {
    var totalRU float64
    for _, tsItem := range r.tsItems {
        totalRU += tsItem.stmtStats.TotalRU
    }
    r.totalRU = totalRU
    return totalRU
}

func (c *collecting) getReportRURecords(maxUsers int, topNPerUser int) map[string]records {
    result := make(map[string]records, maxUsers)
    
    // 第一步: 按 user 分组,并计算每个 record 的 totalRU
    userRecordsMap := make(map[string]records)
    for key, record := range c.ruRecords {
        // key 格式: "user|sqlDigest|planDigest"
        user := extractUserFromKey(key)
        
        // 计算该 record 的累计 totalRU
        record.calculateTotalRU()
        
        if _, ok := userRecordsMap[user]; !ok {
            userRecordsMap[user] = make(records, 0, topNPerUser)
        }
        userRecordsMap[user] = append(userRecordsMap[user], *record)
    }
    
    // 第二步: 如果 user 数量超过上限,选择 RU 总量最大的 user
    if len(userRecordsMap) > maxUsers {
        // 计算每个 user 的总 RU
        type userRUSum struct {
            user string
            sum  float64
        }
        userSums := make([]userRUSum, 0, len(userRecordsMap))
        for user, records := range userRecordsMap {
            var sum float64
            for _, r := range records {
                sum += r.totalRU
            }
            userSums = append(userSums, userRUSum{user: user, sum: sum})
        }
        
        // 按 RU 总量排序,只保留 Top maxUsers
        sort.Slice(userSums, func(i, j int) bool {
            return userSums[i].sum > userSums[j].sum
        })
        
        // 只保留 Top maxUsers 的 user
        newUserRecordsMap := make(map[string]records, maxUsers)
        for i := 0; i < maxUsers && i < len(userSums); i++ {
            user := userSums[i].user
            newUserRecordsMap[user] = userRecordsMap[user]
        }
        userRecordsMap = newUserRecordsMap
    }
    
    // 第三步: 每个 user 分别取 Top N
    for user, records := range userRecordsMap {
        // 按 totalRU 排序
        sort.Sort(records)
        
        // 取 Top N
        top, evicted := records.topN(topNPerUser)
        result[user] = top
        
        // 处理 evicted 记录(可选,可以将 evicted 记录的 RU 合并到该 user 的 "others")
        if len(evicted) > 0 {
            // 可以将 evicted 记录的 RU 累加到该 user 的 "others" 记录
            // 或者直接丢弃(简化处理)
        }
    }
    
    return result
}

// 辅助函数: 从 key 中提取 user
func extractUserFromKey(key string) string {
    idx := strings.IndexByte(key, '|')
    if idx == -1 {
        return ""  // 不应该发生
    }
    return key[:idx]
}
```

**上报流程说明(与现有 TopSQL 对齐)**:

- 数据取走: `collectWorker` 周期性调用 `takeDataAndSendToReportChan()` 将 `collecting.take()` 的结果放入 `reportCollectedDataChan`
- 数据发送: `reportWorker` 从 `reportCollectedDataChan` 消费并发送,在发送时追加 RU 相关 payload(例如 `RURecordsByUser`)

**完整数据流设计**:

```
RU 定期采样(1秒) / 执行完成时采集
    ↓
计算 RU 差值(增量)
    ↓
collecting.getOrCreateRURecord(user, sqlDigest, planDigest)
    ↓
record.appendStmtStatsItem(timestamp, item)  // item 包含 TotalRU
    ↓
record.tsItems[timestamp].stmtStats.TotalRU += ruDelta
    ↓
定期维护(可选): 检查每个 user 的记录数,超过 Top 100 时清理
    ↓
collectWorker 触发(60秒): processStmtStatsData() + takeDataAndSendToReportChan()
    ↓
takeDataAndSendToReportChan(): collecting.take() 同时取走 CPU 和 RU 数据
    ↓
reportWorker: 将 CPU 和 RU 数据一起上报给 DataSink
    ↓
DataSink.TrySend(): 并行上报 CPU 数据和 RU 数据
```

**关于 collectWorker 的复用**:

本节不再重复展开,上报/持久化层(60秒)的复用细节见 `3.2.1.3 上报/持久化流程(60秒)`。

**与现有 CPU 时间统计的关系**:

- **数据存储分离**: 
  - CPU 时间: `collecting.records` (key: sql_digest + plan_digest)
  - RU 数据: `collecting.ruRecords` (key: user + sql_digest + plan_digest)
  
- **数据流独立**:
  - CPU 时间数据: StatementStats → aggregator → stmtStatsBuffer → collecting.records
  - RU 数据: executionContext → 定期采样/执行完成 → collecting.ruRecords
  
- **查询分离**:
  - CPU 时间查询: 按 `(sql_digest, plan_digest)` 维度,全局 Top N
  - RU 数据查询: 按 `user` 维度,每个 user 的 Top 100

**StatementStatsMap 键设计**:

为了兼容现有实现,StatementStats 的 `data` 字段仍然使用 `(sql_digest, plan_digest)` 作为 key:

```go
// StatementStats.data 保持不变(兼容现有)
type StatementStatsMap map[SQLPlanDigest]*StatementStatsItem

// collecting.ruRecords 使用新的 key (支持 user 维度)
// key 格式: "user|sqlDigest|planDigest"
type ruRecordsMap map[string]*record  // string key = encodeRUKey(user, sqlDigest, planDigest)
```

**这样设计的好处**:
1. **向后兼容**: CPU 时间统计仍然使用原有流程,不受影响
2. **清晰分离**: RU 数据有独立的数据流和存储结构
3. **支持需求**: 直接支持"每个 user 存 top100,user 上限 100"的需求
4. **查询简单**: 查询时直接按 user 查询对应的 Top 100 记录
5. **内存可控**: user 上限 100,每个 user 在 `collecting.ruRecords` 中最多保留 TopK 条记录（默认 TopK=200，用于减少抖动；对外查询仍可返回 Top100），总记录数上限约 20,000

#### 3.3.6 采集/上报融入方式评估（实现复杂度 / 侵入性 / 性能 / 维护成本）

这里单独评估“RU 数据如何融入 TopSQL 既有流水线”的两种实现路径（与“按 user 的 TopN 存储方案 A/B/C”是不同维度的问题，避免混淆）。

| 维度 | 路径 1：独立 RU 采集 + 复用上报链路（推荐） | 路径 2：RU 也走 stmtstatsBuffer / processStmtStatsData（备选） |
|------|-------------------------------------------|-------------------------------------------------------------|
| **实现复杂度** | 低-中：在 `stmtstats.aggregator` 增加 `ruAggregate()` + reporter 侧 `ruIncrementBuffer/collecting.ruRecords` | 高：需要重构 stmtstats 的 key、采集/过滤、buffer 结构 |
| **对现有系统侵入性** | 低-中：不改 stmtstats 的主干 `StatementStatsMap` key；RU 通过可选 `RUCollector` 接口投递 | 高：stmtstats/aggregator/collector/reporter 多点联动修改 |
| **性能影响** | 可控：1s 扫描活跃执行，60s 批量上报（复用现有） | 不确定：key 维度扩张为 `(user, sql, plan)`，buffer 体积与处理开销上升 |
| **长期维护成本** | 低：RU 逻辑与 CPU/StmtStats 逻辑清晰分层，边界明确 | 高：RU 与原有 stmtstats 逻辑耦合，后续 stmtstats 变更更易相互影响 |

**推荐路径 1（当前文档采用）**:
- **采集**: 下沉到 `stmtstats.aggregator.ruAggregate()`（复用 1s tick）通过 `executionContext` 读取 `util.RUDetails`，计算增量并通过 `RUCollector.CollectRUIncrements(...)` 投递到 reporter
- **过滤落桶**: reporter 在 `collectWorker` 内每 10s 处理 `ruIncrementBuffer`，按 TopK/user 过滤后写入 `collecting.ruRecords`
- **上报/持久化**: 复用 TopSQL 现有 `collectWorker/reportWorker`，按 `ReportIntervalSeconds=60s` 批量取走并发送（见 `3.2.1.3`）

**路径 2（备选，不作为默认）**:
- 目标是“最大复用 `stmtStatsBuffer → processStmtStatsData()`”，但需要把 stmtstats 的聚合 key 与过滤/TopN 逻辑升级到 user 维度，并重新评估 buffer 的体积与热点开销。
- 由于侵入性与长期维护成本较高，建议仅在未来确有必要时再演进。

#### 3.3.7 其他方案建议(备选)

**方案 B: 在 record 层面聚合,查询时按 user 分组** (备选方案)

如果希望最小化修改,可以考虑在 `tsItem.stmtStats` 中按 user 聚合:

```go
// 扩展 StatementStatsItem,按 user 聚合 RU
type StatementStatsItem struct {
    // ... 现有字段 ...
    TotalRU float64
    RUByUser map[string]float64  // user => totalRU
}

// 在 appendStmtStatsItem 时,同时更新 RUByUser
func (r *record) appendRUByUser(timestamp uint64, user string, ruDelta float64) {
    if index, ok := r.tsIndex[timestamp]; ok {
        if r.tsItems[index].stmtStats.RUByUser == nil {
            r.tsItems[index].stmtStats.RUByUser = make(map[string]float64)
        }
        r.tsItems[index].stmtStats.RUByUser[user] += ruDelta
    }
}

// 查询时按 user 分组,每个 user 取 Top 100
func queryTopRUByUser(records []record, maxUsers int, topNPerUser int) map[string][]record {
    // 1. 按 user 分组
    userRecordsMap := make(map[string][]record)
    for _, record := range records {
        // 从 record 的所有 tsItems 中提取每个 user 的 RU
        for _, tsItem := range record.tsItems {
            for user, ru := range tsItem.stmtStats.RUByUser {
                // 为每个 user 创建虚拟 record,累加 RU
                // ...
            }
        }
    }
    
    // 2. 每个 user 取 Top N
    // ...
}
```

**优势**: 最小化修改,不影响现有流程
**劣势**: 查询时逻辑复杂,性能可能较差,不利于实时查询

**方案 C: 混合方案,分层存储** (备选方案)

完全分离 RU 数据的存储和查询流程:

```go
// collecting 扩展
type collecting struct {
    records map[string]*record  // 现有,用于 CPU 时间
    // ...
    
    // RU 数据独立存储
    ruCollecting *ruCollecting  // 专门的 RU 数据收集器
}

type ruCollecting struct {
    userRecords map[string]map[string]*record  // user => {sqlPlanDigest => record}
    maxUsers    int  // 100
    topNPerUser int  // 100
}

// 每个 user 独立维护 Top 100
func (rc *ruCollecting) addRURecord(user string, sqlDigest, planDigest []byte, timestamp uint64, ruDelta float64) {
    userRecs := rc.userRecords[user]
    if userRecs == nil {
        if len(rc.userRecords) >= rc.maxUsers {
            // user 数量超限,选择 RU 总量最小的 user 替换(或其他策略)
            rc.evictLeastActiveUser()
        }
        userRecs = make(map[string]*record)
        rc.userRecords[user] = userRecs
    }
    
    key := encodeKey(sqlDigest, planDigest)
    record := userRecs[key]
    if record == nil {
        if len(userRecs) >= rc.topNPerUser {
            // 该 user 的记录数超限,需要 TopN 选择
            rc.evictLeastRUForUser(user)
        }
        record = newRURecord(user, sqlDigest, planDigest)
        userRecs[key] = record
    }
    
    // 更新 RU 数据
    record.appendRUDelta(timestamp, ruDelta)
}

// 定期清理和 TopN 选择
func (rc *ruCollecting) maintainTopN() {
    for user, userRecs := range rc.userRecords {
        if len(userRecs) > rc.topNPerUser {
            // 按 totalRU 排序,只保留 Top N
            records := make([]*record, 0, len(userRecs))
            for _, r := range userRecs {
                records = append(records, r)
            }
            sort.Slice(records, func(i, j int) bool {
                return records[i].totalRU > records[j].totalRU
            })
            
            // 保留 Top N,清理其他
            topN := records[:rc.topNPerUser]
            newUserRecs := make(map[string]*record)
            for _, r := range topN {
                key := encodeKey(r.sqlDigest, r.planDigest)
                newUserRecs[key] = r
            }
            rc.userRecords[user] = newUserRecs
        }
    }
}
```

**优势**: 完全独立,不影响现有实现,实时维护 TopN
**劣势**: 实现复杂度较高,需要额外的维护逻辑

**按 user TopN 的存储方案权衡（实现复杂度 / 侵入性 / 性能影响 / 长期维护成本）**:

> 说明：这里讨论的是“如何在本地时间桶里组织 `(user, sql, plan)` 的 TopN 数据”；与 `3.3.6` 的“RU 如何融入 TopSQL 既有流水线（路径 1/2）”是不同问题。

| 维度 | 方案 A：扩展 `collecting.ruRecords`（默认） | 方案 B：在 `tsItem/StatementStatsItem` 内再做 user 聚合 | 方案 C：独立 `ruCollecting` 实时维护 TopN |
|------|--------------------------------------------|-------------------------------------------------------|------------------------------------------|
| **实现复杂度** | 中：新增 `ruRecords`/key/TopN-by-user | 低-中：写入点少，但查询侧/聚合侧复杂 | 高：需要新 collector + 淘汰/维护策略 |
| **对现有系统侵入性** | 低：不动 TopSQL 现有 `records`/CPU 流程 | 中：需要扩展 `StatementStatsItem` 结构（map）并贯穿序列化/合并 | 中-高：新增较多组件与策略代码 |
| **性能影响** | 可控：写入路径 O(1)，上报/查询时做分组 TopN | 风险更高：每个时间桶维护 `map[user]ru`，热点与 GC 压力更大 | 可控但实现敏感：实时淘汰可控内存，但维护成本与锁竞争需评估 |
| **长期维护成本** | 低：RU 存储与 CPU 存储分离、边界清晰 | 中-高：逻辑耦合到通用 `StatementStatsItem`，后续演进更易互相牵扯 | 高：策略多、边界多，长期需要持续调优与回归 |

**默认选择：方案 A**（更符合“侵入性低 + 性能可控 + 易维护”的平衡点）。  
**何时考虑方案 C**：仅在后续实测发现 `ruRecords` 的内存峰值/淘汰策略不足，且确需“采集阶段实时 TopN/实时控内存”时再引入；避免过早复杂化。

#### 3.3.8 Protobuf 消息扩展

**TopSQLRecordItem 扩展** (`tipb/top_sql.proto`):

```protobuf
message TopSQLRecordItem {
    uint64 timestamp_sec = 1;
    uint32 cpu_time_ms = 2;
    uint64 stmt_exec_count = 3;
    // ... 现有字段 ...
    
    // 新增 RU 相关字段
    double total_ru = 10;  // 总 RU
    string user = 11;      // 用户名
}
```

#### 3.3.9 数据保留策略

**复用 TopSQL 现有机制**:

- TopSQL 使用时间桶机制,基于 `PrecisionSeconds` 配置
- 数据保留时间由 TopSQL 现有配置控制
- RU 数据与 CPU 时间数据使用相同的保留策略,无需额外处理

### 3.4 系统架构

#### 3.4.1 数据采集路径

**数据采集流程** (复用 TopSQL 现有路径):

```
SQL Execution
    ↓
Executor (adapter.go)
    ↓
KV Request (TiKV/TiFlash)
    ↓
RU Details Collection (util.RUDetails)
    ↓
observeStmtFinishedForTopSQL() [扩展 RU 和 user 采集]
    ↓
StatementStats.OnExecutionFinished() [扩展接口]
    ↓
TopSQL Reporter (pkg/util/topsql/reporter/)
    ↓
Time Bucket Aggregation (复用现有机制)
    ↓
Memory Storage (复用现有存储)
```

**采集点设计**:

1. **本地定期采样** (执行中 SQL,1秒):
   - 下沉到 `stmtstats.aggregator`：在 `aggregator.run()` 的 1s tick 内，`m.aggregate()` 后追加 `m.ruAggregate()`
   - 每 1 秒扫描一次所有活跃的 `StatementStats.executionContext`
   - 从每个上下文中读取 `util.RUDetails` 的当前 RU 值
   - 将 RU 增量聚合为 `RUIncrementsMap` 并投递给 `RUCollector`（TopSQL reporter）
   - reporter 将 RU 增量累加到 `ruIncrementBuffer`；每 10 秒过滤 TopK/user 后再落桶到本地时间桶
   - 活跃 SQL 执行上下文复用 `StatementStats.executionContext`（注册/注销由执行路径驱动）

2. **上报/持久化采样** (60秒):
   - 复用 TopSQL 现有的上报链路(`collectWorker` 周期触发 + `reportWorker` 发送)
   - 每 60 秒将本地时间桶中的 RU 数据打包上报
   - 批量上报给其他组件用于持久化和管理
   - 类似 Prometheus 的上报机制,减少网络开销

3. **用户查询刷新** (默认 60 秒，可配置 15s/30s/60s):
   - **说明**: 用户侧刷新功能在 VM 组件中处理
   - TopSQL 提供查询接口,返回本地时间桶的 RU 数据
   - VM 组件按刷新间隔拉取并刷新用户可见的数据（默认 60 秒，可配置 15s/30s/60s）
   - 本文档仅在此处提及,详细实现见 VM 组件文档

4. **执行完成时采集** (补充机制):
   - 在 `pkg/executor/adapter.go` 的 `observeStmtFinishedForTopSQL()` 方法中扩展
   - 从 `context.Context` 中获取 `util.RUDetailsCtxKey` 对应的 `RUDetails`
   - 从 `SessionVars.User.Username` 获取用户名
   - 调用扩展后的 `StatementStats.OnExecutionFinished()` 方法
   - 此时 `util.RUDetails` 包含该 SQL 的最终 RU 消耗信息
   - 用于补充定期采样可能遗漏的最终数据,确保数据准确性

5. **活跃执行上下文管理** (复用 StatementStats):
   - 在 SQL 执行开始时,通过 `StatementStats.StartExecution()` 注册到 `StatementStats.executionContext`
   - 在 SQL 执行完成时,通过 `StatementStats.FinishExecution()` 从 `StatementStats.executionContext` 移除
   - 本地定期采样时,通过 aggregator 遍历所有活跃的 StatementStats,读取其中的活跃执行
   - **优势**: 活跃执行的生命周期与 StatementStats 绑定,由 aggregator 自动管理 StatementStats 的生命周期

6. **RU 数据来源**:
   - RU 消耗主要在 TiKV/TiFlash 的 KV 请求中产生
   - 通过 `util.RUDetails` 结构从 KV 响应中实时累积 RU 信息
   - 该机制已在现有代码中实现(见 `pkg/store/copr/coprocessor.go`)
   - `util.RUDetails` 在 SQL 执行过程中持续更新,可以随时读取当前值

**采集代码实现**:

```go
// 扩展 StatementObserver 接口
type StatementObserver interface {
    OnExecutionBegin(sqlDigest, planDigest []byte, inNetworkBytes uint64)
    
    // 扩展 OnExecutionFinished,添加 user 和 TotalRU 参数
    OnExecutionFinished(sqlDigest, planDigest []byte, execDuration time.Duration, 
        outNetworkBytes uint64, user string, totalRU float64)
}

```

#### 3.4.2 实时性与性能权衡

**实时性保证**:

1. **三层采样/刷新频率**: 
   - **本地采集层**: 每 1 秒采集一次,确保本地数据实时性
   - **上报/持久化层**: 每 60 秒上报一次,平衡实时性和网络开销
   - **用户查询刷新层**: 由 VM 组件处理,刷新间隔默认 60 秒,可配置 15s/30s/60s
   - 可以通过配置参数调整各层采样/刷新频率

2. **时间桶粒度**:
   - RU 数据的时间桶粒度复用 TopSQL 的 `PrecisionSeconds` 配置(默认 1 秒)
   - RUDetails 的读取频率为 1 秒,但为控制基数与内存,采用“10 秒过滤后落桶”
   - 因此时间桶的粒度仍为 1 秒,但 RU 写入点通常是每 10 秒一个点（可配置）
   - 上报和查询刷新从本地时间桶读取数据

3. **异步处理**:
   - 本地定期 RU 采集在独立的 goroutine 中执行,不阻塞 SQL 执行
   - 上报/持久化复用 TopSQL 现有的 `collectWorker/reportWorker`,无需新增独立 goroutine
   - 使用 TopSQL 现有的 channel 缓冲和后台 worker 机制
   - 活跃 SQL 上下文的注册和注销操作需要加锁保护
   - 定期采样时对 `executionContext` 使用读锁(`RWMutex.RLock`),注册/注销时使用写锁(`RWMutex.Lock`),减少锁竞争

**性能优化**:

1. **内存优化**:
   - RU 字段仅增加少量内存开销(每个 record 增加约 8 字节,TotalRU 字段)
   - user 字段使用 string,内存开销可控
   - 活跃执行存储在 StatementStats 中,每个 session 同时最多 1 个 `executionContext`（MySQL 协议语义）
   - 本地时间桶数据在内存中,上报后可以清理或压缩
   - 复用 TopSQL 现有的内存管理和清理机制

2. **CPU 优化**:
   - 本地定期 RU 采集(1秒)在独立 goroutine 中执行,不阻塞 SQL 执行
   - 上报/持久化(60秒)在现有的 collectWorker 中执行,不阻塞本地采集
   - StatementStats 内部使用独立 `RWMutex` 保护 executionContext(读多写少),定期采样时通过 aggregator 的 sync.Map 并发访问多个 StatementStats
   - 1 秒本地采样间隔在实时性和 CPU 开销之间取得平衡
   - 执行完成时的 RU 采集与 CPU 时间采集在同一个调用路径中

3. **网络优化**:
   - 上报/持久化采用批量上报机制(60秒),减少网络请求次数
   - 类似 Prometheus 的上报机制,批量打包数据,提高效率
   - 上报失败不影响本地数据采集和用户查询

4. **查询优化**:
   - 复用 TopSQL 现有的 Top N 计算机制
   - 支持按 RU 排序时,使用相同的排序算法
   - 可以同时支持按 CPU 时间排序和按 RU 排序

**性能目标**:
- 本地采集开销: 每 1 秒扫描一次活跃 SQL,开销 ≈ 1-2ms
- 上报开销: 每 60 秒批量上报一次,开销 < 5ms (与现有 TopSQL 上报开销相同)
- 内存开销:
  - `collecting.ruRecords` 通过 **10s 过滤 TopK/user** 进行约束（例如 user≤100、TopK/user=200），将 record 数量控制在 ~20k 量级
  - 单个 record 在 60s 上报窗口内的 `tsItems` 写入频率为 10s（约 6 个点/分钟），明显少于 1s 直接落桶
  - `ruIncrementBuffer` 为轻量级 map（仅 `float64 + time`），并可通过 `ruMaxBufferEntries` 做硬上限保护
  - 活跃 SQL 上下文: 每个 session 一个 `executionContext`（读多写少，使用 `RWMutex`）
- 查询延迟: 与 TopSQL 现有查询延迟相当；用户侧可见延迟由刷新间隔决定（默认 60 秒，可配置 15s/30s/60s）

#### 3.4.3 TopN 计算的代价控制

**复用 TopSQL 现有机制**:

TopSQL 已经实现了高效的 Top N 计算机制,我们复用该机制,只需要:
1. 在排序时支持按 RU 字段排序(而不仅仅是 CPU 时间)
2. 在聚合时考虑 user 维度

**排序实现**:

```go
// 在 reporter 中添加按 RU 排序的选项
func (tsr *RemoteTopSQLReporter) GetTopRecords(sortBy string, topN int) []*TopRecord {
    // sortBy 可以是 "cpu" 或 "ru"
    // 使用相同的排序算法,只是比较字段不同
    if sortBy == "ru" {
        // 按 TotalRU 排序
        sort.Slice(records, func(i, j int) bool {
            return records[i].TotalRU > records[j].TotalRU
        })
    } else {
        // 按 CPU 时间排序(现有逻辑)
        sort.Slice(records, func(i, j int) bool {
            return records[i].TotalCPUTimeMs > records[j].TotalCPUTimeMs
        })
    }
    // 返回 Top N
    return records[:min(topN, len(records))]
}
```

**代价控制策略** (复用 TopSQL 现有策略):

1. **限制查询窗口**: 复用 TopSQL 的查询窗口限制
2. **限制 Top N**: 复用 TopSQL 的 Top N 限制(默认 100)
3. **采样策略**: 复用 TopSQL 现有的采样和限流机制

### 3.5 性能与风险分析

#### 3.5.1 高并发场景下的开销

**并发场景分析** (基于 TopSQL 现有性能):

1. **高 QPS 场景**:
   - TopSQL 已经在高 QPS 场景下验证过性能
   - RU 数据采集与 CPU 时间采集在同一个调用路径中
   - 额外开销主要是获取 RU 数据和 user 字符串,开销很小
   
   **优化措施**:
   - 复用 TopSQL 现有的异步采集机制,避免阻塞 SQL 执行
   - RU 数据获取开销很小(从 context 中读取已有对象)
   - user 字符串已经存在于 SessionVars 中,无需额外计算

2. **内存使用**:
   - 每个 `StatementStatsItem` 增加约 8 字节(TotalRU 字段)
   - user 字段使用 string,平均长度约 20 字节
   - 活跃 SQL 上下文: `StatementStats` 为每个 session 一个,每个活跃 session 仅 1 个 `executionContext`（结构体较小,且读多写少）
   - `collecting.ruRecords` 通过 **10s 过滤 TopK/user** 控制基数（例如 user≤100、TopK/user=200 ⇒ record 上限 ~20k）
   - 每个 record 在 60s 上报窗口内的 `tsItems` 写入频率为 10s（约 6 点/分钟）,进一步降低常驻内存
   - `ruIncrementBuffer` 为轻量级 map,并可通过 `ruMaxBufferEntries` 设置硬上限（超限丢弃/汇总到 others）,避免极端场景无界增长
   
   **优化措施**:
   - 复用 TopSQL 现有的内存管理和清理机制
   - user 字符串可以共享(如果多个 record 使用相同的 user)
   - 活跃 SQL 上下文在 SQL 执行完成时自动清理
   - TopSQL 已经有内存使用限制和清理机制

3. **CPU 开销**:
   - RU 数据定期采样每 1 秒执行一次,在独立 goroutine 中,不阻塞 SQL 执行
   - RU 数据上报每 60 秒执行一次,复用现有的 collectWorker,不阻塞其他操作
   - 执行完成时的 RU 采集与 TopSQL 现有采集时机一致,开销很小
   - 定期采样开销: 扫描活跃 SQL 列表(约 1ms) + 读取 RU 数据(约 0.1μs 每个 SQL)
   - 总额外开销: < 1% CPU (相对于 TopSQL 现有开销)

#### 3.5.2 潜在 OOM / CPU 风险

**OOM 风险** (复用 TopSQL 现有防护):

1. **风险场景**:
   - TopSQL 已经有完善的内存管理机制
   - RU 扩展仅增加少量内存开销,风险较低
   - 主要风险来自 user 字段的字符串存储

2. **防护措施** (复用可观测性基础设施机制):
   - 利用现有的最大记录数限制和清理机制
   - 复用过期数据清理逻辑
   - user 字符串可以共享以减少内存占用

3. **降级策略**:
   - TopRU 功能可通过配置开关控制
   - 复用现有的降级和限流机制

**CPU 风险** (风险较低):

1. **风险场景**:
   - RU 数据采集开销很小,主要风险来自聚合和排序
   - 利用现有的锁机制,锁竞争风险可控

2. **防护措施**:
   - 复用现有的查询限流和采样机制
   - 按 RU 排序与按 CPU 排序使用相同的算法,性能相当

#### 3.5.3 近似计算 / 限流 / 降级策略

**TopRU 的控制策略**:

1. **采样策略**: 利用现有的采样和限流机制
2. **精度权衡**: RU 值使用 `float64` 类型,精度足够
3. **限流策略**: 复用现有的查询限流机制
4. **降级策略**: TopRU 功能可通过配置开关控制,支持降级

### 3.6 可扩展性与后续演进

#### 3.6.1 RU Baseline (历史对比)

**设计思路**:

1. **基线计算**:
   - 基于 TopRU 历史数据,计算每个 `(user, sql_digest)` 的平均 RU 消耗
   - 支持多种基线算法: 平均值、P95、P99 等
   - 基线数据可以存储在系统表中

2. **对比展示**:
   - 在 TopRU 查询结果中,增加 `BaselineRU` 和 `DeviationPercent` 字段
   - `DeviationPercent = (CurrentRU - BaselineRU) / BaselineRU * 100%`
   - 支持按偏差百分比排序,快速发现异常 SQL

**实现要点**:
- 基线计算采用离线批处理方式,避免影响在线性能
- 基线更新频率: 每天一次
- 可以利用现有的数据存储机制

#### 3.6.2 异常检测 / 趋势分析

**异常检测**:

1. **检测算法**:
   - 基于统计方法: 3-sigma 规则,检测偏离均值超过 3 倍标准差的 SQL
   - 基于规则: 检测 RU 消耗突然增长、执行次数异常等模式
   - 利用 TopRU 的时间序列数据

2. **告警机制**:
   - 检测到异常后,生成告警事件
   - 支持告警规则配置: 阈值、告警频率等
   - 告警可以通过邮件、Webhook 等方式通知

**趋势分析**:

1. **趋势计算**:
   - 利用 TopRU 的时间桶数据,计算每个 SQL 的 RU 消耗趋势
   - 使用线性回归或移动平均等算法
   - 支持多时间粒度: 小时、天、周等

2. **可视化**:
   - 提供 RU 消耗趋势图表
   - 支持对比不同时间段的趋势
   - 可以与现有的可观测性工具集成

#### 3.6.3 与其他可观测性模块的联动

**数据关联**:

1. **关联键**:
   - 使用 `(sql_digest, plan_digest)` 作为关联键
   - 在查询结果中可以提供跳转到慢日志的链接

2. **统一视图**:
   - 支持同时查看 CPU 时间和 RU 数据
   - 支持按不同维度排序（CPU 时间、RU 消耗等）
   - 可以与慢日志数据关联,提供更全面的 SQL 性能分析

**联动场景**:

1. **多维度分析**:
   - 用户可以同时查看 SQL 的 CPU 时间和 RU 消耗
   - 发现 CPU 时间高但 RU 消耗低的 SQL(可能是计算密集型操作)
   - 发现 RU 消耗高但 CPU 时间低的 SQL(可能是 I/O 密集型操作)

2. **与慢日志的联动**:
   - 用户在 TopRU 中发现高 RU 消耗的 SQL
   - 可以通过 sql_digest 关联到慢日志中的详细执行记录
   - 获取更详细的执行计划和执行详情

## 4. Limitation

1. **采样/刷新频率限制**: 
   - 本地采集每 1 秒一次,但用户侧刷新默认 60 秒（可配置 15s/30s/60s），因此用户看到的数据存在“刷新粒度”带来的延迟
   - 对于执行时间 < 1 秒的 SQL,可能只有执行完成时的一次采样
   - 对于执行时间 < 用户侧刷新间隔(默认 60 秒) 的 SQL,用户侧可能看不到中间采样点，只能在下一次刷新时看到聚合后的结果

2. **时间窗口限制**: 复用 TopSQL 现有的查询窗口限制机制。

3. **数据保留限制**: 复用 TopSQL 现有的数据保留策略。

4. **精度限制**: 
   - RU 值的精度为 `float64`,精度足够
   - 用户侧刷新间隔默认 60 秒（可配置 15s/30s/60s），可见性受刷新间隔影响
   - 本地采集频率为 1 秒,但用户侧刷新通常更低频
   - 时间桶粒度复用 TopSQL 的 `PrecisionSeconds` 配置(默认 1 秒)

5. **Top N 限制**: 复用 TopSQL 现有的 Top N 限制(默认 100)。

6. **内存限制**: RU 扩展仅增加少量内存开销,主要受 TopSQL 现有内存管理机制限制。

7. **用户维度限制**: 当前支持按用户名聚合,不支持按 Resource Group 聚合(后续可扩展)。

8. **跨节点限制**: 复用 TopSQL 现有的跨节点限制,数据仅在当前 TiDB 节点收集。

9. **计费口径限制（TopRU RU ≠ Billing RU）**:
   - TopRU 的 RU 用于定位“谁在消耗 RU”,来源为运行时 `util.RUDetails`
   - Billing 侧 RU 以计费口径为准,可能存在统计窗口、归因、取整等差异
   - 因此两者数值不保证一致,TopRU 不作为计费对账工具

10. **执行中 SQL 文本可用性限制**:
   - TopRU 的核心聚合维度是 `(user, sql_digest, plan_digest)`；对执行中 SQL,系统不保证一定能拿到对应的 SQL 文本/statement
   - 若 SQL 元信息缺失,列表可能仅展示 digest 与 plan digest；用户可结合慢日志/执行计划等信息进一步排查

11. **10s 过滤窗口的数据精度限制**:
   
   TopRU 采用"1s 采样 + 10s 过滤 + 60s 上报"的分层设计以控制内存：
   - **1s 采样**：将 RU 增量累积到轻量级 `ruIncrementBuffer[timestamp]`（每个条目仅 24 字节）
   - **10s 过滤**：按每个 timestamp 独立处理，每个 timestamp 保留 Top 100 users × Top 100 SQL/user，写入 `collecting.ruRecords`
   - **60s 上报**：直接上报 `collecting.ruRecords` 中的数据（已经过 10s 过滤）
   
   **10s 和 60s 处理的关系**：
   
   - `ruIncrementBuffer` 的类型为 `map[uint64]RUIncrementsMap`，key 是 timestamp（秒级）
   - 10s 过滤和 60s 上报都是按每个 timestamp 独立处理，无跨时间聚合
   - 10s 过滤后，每个 timestamp 保留 Top 100 users × Top 100 SQL/user 写入 `collecting.ruRecords`
   - 60s 上报时，直接取走 `collecting.ruRecords`（每个 timestamp 最多 10,000 条）
   
   **数据精度影响**：
   
   - **边界 SQL 的历史数据可能丢失**：
     - 如果某条 SQL 在某个 timestamp 内未进入该 user 的 Top 100，其 RU 数据会被汇总到全局 `"_others_"`
     - 若该 SQL 在后续 timestamp 进入 Top 100（例如突然进入慢查询阶段），之前 timestamp 的数据无法追溯
     - **示例场景**：
       ```
       T=1s:  SQL_101 累积 45 RU，排名 101 → 被过滤到 "_others_"
       T=2s:  SQL_101 累积 4500 RU，排名 15 → 进入 Top 100
       结果：T=1s 的 45 RU 在 "_others_" 中，T=2s 的 4500 RU 在正常 record 中
       ```
     - **影响范围**：主要影响排名在第 80-120 名之间的"边界 SQL"
   
   - **间歇性高 RU SQL 的数据不连续**：
     - 执行模式为"高 RU → 低 RU → 高 RU"的 SQL，其低 RU 阶段的数据可能被过滤
     - **示例场景**：
       ```
       T=1s:  SQL_X 高 RU (1000)，排名 50 → 进入 Top 100
       T=2s:  SQL_X 低 RU (10)，排名 150 → 被过滤到 "_others_"
       T=3s:  SQL_X 再次高 RU (1000)，排名 50 → 再次进入 Top 100
       结果：RU 趋势图在 T=2s 出现"断点"
       ```
     - **影响**：RU 趋势图可能不连续，但累计 RU 总量仍准确（丢失部分汇总在 `"_others_"` 中）
   
   - **TopN 边界抖动**：
     - 第 99-102 名的 SQL 可能在每个 timestamp 反复进出 Top 100
     - 导致这些 SQL 的部分时间点数据在 `"_others_"`，部分在正常 record 中
   
   **缓解措施**：
   
   - **扩大过滤阈值**：内部维护 Top 150（buffer 过滤阈值），对外查询仍为 Top 100，留 50% 余量减少抖动
   - **"_others_" 汇总机制**：所有被过滤的 RU 汇总到 `"_others_"` record（按 timestamp），用户可通过 `"_others_"` 的 RU 变化判断是否有重要 SQL 被遗漏
   - **适用场景说明**：
     - TopRU 的核心目标是"快速定位头部高 RU SQL"（Top 10-50）
     - 对于稳定在 Top 50 的 SQL，数据精度不受影响（始终在 Top 100 阈值内）
     - 对于排名在 100 名之后的 SQL，建议结合慢日志、Statement Summary 等其他工具分析
   
   **后续优化方向**：
   
   - **扩大 TopN 阈值**：后续可将 Top 100 扩大为 Top 200，以覆盖更多边界 SQL（内存开销增加约 1 倍）
   - **10s 时间窗口聚合**：可将每秒数据聚合为每 10s 一条，减少数据传输和存储开销
     - **优势**：数据量减少 10 倍，网络传输更高效
     - **劣势**：精度下降，从每秒数据变为每 10s 数据，无法看到秒级波动
   
   **查询建议**：
   
   - 如果发现 `"_others_"` 的 RU 占比较高（例如 > 20%），说明可能有重要 SQL 未进入 Top 100
   - 此时建议缩短查询时间窗口（例如从 1 小时缩短到 10 分钟），或配合慢日志定位具体 SQL

## 5. Compatibility Issues

### 5.1 Functional

1. **与 Resource Control 的兼容性**:
   - TopRU 依赖 `util.RUDetails` 收集 RU 数据,需要确保 Resource Control 功能已启用
   - 如果 Resource Control 未启用,`util.RUDetails` 可能为空,导致 RU 字段为 0
   - **解决方案**: 
     - 如果 Resource Control 未启用,RU 字段显示为 0,但不影响其他可观测性功能
     - 在文档中说明需要启用 Resource Control 才能获得准确的 RU 数据

2. **与现有可观测性功能的兼容性**:
   - TopRU 与现有的 CPU 时间统计功能完全兼容
   - 可以同时支持按 CPU 时间排序和按 RU 排序
   - **解决方案**: 
     - 保持向后兼容,现有的查询不受影响
     - TopRU 提供独立的查询接口

3. **与 Statement Summary 的兼容性**:
   - 可能从 Statement Summary 获取 SQL 文本,该机制保持不变
   - 但对于执行中 SQL/瞬时 SQL,SQL 文本可能存在缺失或延迟注册的情况,因此不保证 TopRU 一定能展示完整 SQL statement（见 Non-Goals / Limitation）
   - **解决方案**: 无特殊处理,复用现有机制

4. **与慢日志的兼容性**:
   - TopRU 和慢日志可以同时启用,两者独立工作
   - TopRU 可以提供慢日志中 RU 字段的聚合视图
   - **解决方案**: 无特殊处理,两者完全兼容

### 5.2 Upgrade

1. **从旧版本升级**:
   - 新版本提供 TopRU 功能,旧版本无 RU 统计能力
   - 升级后,TopRU 功能自动可用（如果可观测性功能已启用）
   - **解决方案**: 
     - TopRU 作为新功能,无需额外配置
     - 在升级文档中说明 TopRU 功能的使用方法

2. **数据格式兼容性**:
   - 内存数据不持久化,升级后数据会丢失
   - Protobuf 消息格式扩展,需要考虑向前兼容
   - **解决方案**: 
     - RU 相关字段使用 optional 字段,旧版本客户端可以忽略
     - 使用 Protobuf 的向后兼容机制,确保旧版本客户端不会出错

3. **API 兼容性**:
   - 提供 TopRU 查询接口,支持按 RU 排序
   - 保持现有接口不变,新增独立的 TopRU 接口
   - **解决方案**: 
     - 新接口不影响现有调用
     - 在 API 文档中说明新接口的使用方法

## 6. Test Design

### 6.1 Functional Test

#### 6.1.1 基础功能测试

1. **数据采集测试**:
   - 测试本地定期采样(1 秒)是否正确采集执行中 SQL 的 RU 数据
   - 测试上报/持久化采样(60 秒)是否正确上报数据
   - 测试 TopSQL 查询接口是否正确返回 RU 数据(用户查询刷新由 VM 组件处理)
   - 测试 SQL 执行完成时 RU 数据是否正确采集(补充机制)
   - 测试 RU 数据是否正确写入 `StatementStatsItem`
   - 测试 user 字段是否正确采集
   - 测试长时间执行的 SQL 是否有多个本地采样点(1秒间隔)
   - 测试活跃 SQL 上下文的注册和注销是否正确
   - 测试三层采样机制是否独立工作,互不干扰

2. **聚合测试**:
   - 测试相同 `(user, sql_digest, plan_digest)` 的多次执行是否正确聚合
   - 测试不同用户的相同 SQL 是否正确分别统计
   - 测试 RU 数据在时间桶中是否正确聚合
   - 测试 RU 数据与 CPU 时间数据是否独立统计

3. **查询测试**:
   - 测试 TopSQL 查询接口支持按 RU 排序
   - 测试按 user 维度查询是否正常工作
   - 测试 Top N 排序是否正确
   - 测试 RU Share Percent 计算是否正确
   - 测试同时支持按 CPU 时间排序和按 RU 排序

4. **数据保留测试**:
   - 复用 TopSQL 现有的数据保留测试
   - 测试 RU 数据与 CPU 时间数据使用相同的保留策略

#### 6.1.2 边界 case 测试

1. **时间边界**:
   - 复用 TopSQL 现有的时间边界测试

2. **数据边界**:
   - 测试 RU 值为 0 的情况(Resource Control 未启用时)
   - 测试 RU 值为负数的情况(不应该出现,但需要防护)
   - 测试 user 字段为空的情况(内部 SQL)
   - 测试 `util.RUDetails` 为空的情况
   - 测试执行时间 < 1 秒的 SQL 是否只有执行完成时的一次采样
   - 测试执行时间 > 1 秒的 SQL 是否有多个本地采样点
   - 测试执行时间 < 用户侧刷新间隔(默认 60 秒) 的 SQL 在用户侧查询中是否能看到数据
   - 测试执行时间 > 用户侧刷新间隔(默认 60 秒) 的 SQL 在 VM 组件中是否有多次刷新(由该组件处理)

3. **并发边界**:
   - 复用 TopSQL 现有的并发测试
   - 测试高并发场景下 RU 数据采集是否正确
   - 测试并发查询 RU 数据是否正确

### 6.2 Compatibility Test

1. **与 Resource Control 的兼容性测试**:
   - 测试 Resource Control 启用时 RU 数据是否正确
   - 测试 Resource Control 禁用时 RU 字段是否为 0
   - 测试 TopSQL 其他功能不受 Resource Control 状态影响

2. **与 TopSQL 现有功能的兼容性测试**:
   - 测试 RU 扩展不影响 TopSQL 现有的 CPU 时间统计
   - 测试可以同时查询 CPU 时间和 RU 数据
   - 测试可以同时按 CPU 时间排序和按 RU 排序

3. **版本兼容性测试**:
   - 测试从旧版本 TopSQL 升级后的行为
   - 测试 Protobuf 消息格式的向前兼容性
   - 测试旧版本客户端能否正确处理新消息格式

### 6.3 Performance Test

#### 6.3.1 采集性能测试

1. **本地定期采集性能**:
   - 测试每 1 秒扫描活跃 SQL 列表的开销
   - 目标: 扫描 1000 个活跃 SQL 的开销 < 1ms

2. **上报/持久化性能**:
   - 测试每 60 秒批量上报 RU 数据的开销
   - 目标: 批量上报 1000 条记录的开销 < 5ms

3. **用户查询刷新性能**:
   - 用户查询刷新由 VM 组件处理,性能测试见该组件文档

4. **单 SQL 性能**:
   - 测试单个 SQL 执行时 RU 数据采集的额外开销
   - 目标: 执行完成时采集开销 < 0.1ms,定期采样不阻塞 SQL 执行

5. **高 QPS 性能**:
   - 测试高并发场景下活跃 SQL 列表的管理开销
   - 目标: 注册/注销操作开销 < 0.01ms

6. **内存开销**:
   - 测试 RU 扩展带来的额外内存开销
   - 目标: 每个 record 增加 < 50 字节内存,活跃 SQL 上下文 < 1KB 每个

#### 6.3.2 查询性能测试

1. **按 RU 排序性能**:
   - 测试按 RU 排序的查询延迟
   - 目标: 与 TopSQL 现有按 CPU 时间排序性能相当

2. **按 user 维度查询性能**:
   - 测试按 user 维度过滤和聚合的查询性能
   - 目标: 查询延迟增加 < 10%

3. **并发查询**:
   - 复用 TopSQL 现有的并发查询测试
   - 目标: 与 TopSQL 现有并发查询性能相当

#### 6.3.3 内存性能测试

1. **内存使用**:
   - 测试 RU 扩展带来的额外内存使用
   - 目标: 额外内存 < 1% (相对于 TopSQL 现有内存使用)

2. **内存泄漏**:
   - 长时间运行测试,检查是否存在内存泄漏
   - 目标: 内存使用稳定,符合可观测性模块的一致性要求

3. **OOM 防护**:
   - 验证现有的 OOM 防护机制对 TopRU 有效
   - 目标: TopRU 不影响现有的防护机制

## 7. Impact & Risk

### 7.1 Positive Impacts

1. **提升可观测性**: TopRU 提供了 RU 维度可观测能力,增加了资源消耗视角,直接对应计费维度。

2. **加速排障**: 能够快速定位高 RU 消耗的 SQL,加速问题排查,特别是在 MAXRU 限制场景下。

3. **支持资源治理**: 按用户维度聚合,支持资源配额管理和审计。

4. **实现高效**: 复用可观测性基础设施,降低实现复杂度,保持系统架构统一性。

### 7.2 Negative Impacts

1. **性能开销**: TopRU 会带来少量的 CPU 和内存开销,但开销很小(< 1%)。

2. **内存使用**: 每个 record 增加约 50 字节内存,总体内存开销可控。

3. **复杂度增加**: 增加 RU 相关字段和逻辑,但复杂度增加有限。

### 7.3 Risks

1. **性能风险**:
   - **风险**: RU 数据采集可能带来额外的性能开销(采集频率为 1 秒)
   - **缓解措施**:
     - RU 数据采集开销很小(从 context 读取已有对象)
     - 利用现有的异步采集机制
     - 采集频率 1 秒,不会增加额外的采集周期
     - 性能开销 < 1% CPU (在可接受范围内)

2. **内存风险**:
   - **风险**: user 字段的字符串存储可能增加内存使用
   - **缓解措施**:
     - user 字符串可以共享以减少内存占用
     - 利用现有的内存管理和清理机制
     - 额外内存开销 < 1%

3. **数据准确性风险**:
   - **风险**: 如果 Resource Control 未启用,RU 数据可能不准确
   - **缓解措施**:
     - 在文档中明确说明需要启用 Resource Control
     - 如果 Resource Control 未启用,RU 字段显示为 0

4. **兼容性风险**:
   - **风险**: Protobuf 消息格式扩展可能影响旧版本客户端
   - **缓解措施**:
     - 使用 optional 字段确保向前兼容
     - 充分测试 Protobuf 兼容性
     - 旧版本客户端可以忽略新字段

5. **稳定性风险**:
   - **风险**: TopRU 功能可能存在 bug,影响系统稳定性
   - **缓解措施**:
     - 充分的单元测试和集成测试
     - 代码审查
     - 灰度发布
     - 提供配置开关,可以随时禁用 TopRU 功能

## 8. Investigation & Alternatives

### 8.1 原方案：10s 聚合 TopK 过滤

**原方案描述**：
- 1s 采集时将所有 RU 增量写入 `ruIncrementBuffer`，不做过滤
- 10s 时触发聚合，对每个 user 的 SQL 按 totalRU 排序，取 Top 100
- 超出 Top 100 的 SQL 汇总到 `_others_`

**内存估算基准**（基于 TopRU 上报字段）：

| 字段 | 类型 | 大小 |
|------|------|------|
| Keyspace | []byte | ~16 字节 |
| User | string | ~16 字节（平均用户名） |
| SQLDigest | []byte | 32 字节（SHA256） |
| PlanDigest | []byte | 32 字节（SHA256） |
| TotalRU | float64 | 8 字节 |
| ExecCount | uint64 | 8 字节 |
| SumDurationNs | uint64 | 8 字节 |
| **合计** | | **~120 字节/条目** |

注：实际内存占用还需考虑 Go map 开销（约 50%），因此每条目实际占用约 **180 字节**。

**不足之处**：

1. **内存风险高**：
    - 极端场景（1000 users × 5000 SQL）下，`ruIncrementBuffer` 可能在 1s 内膨胀到 500 万条目
    - 每条目约 180 字节，1s 内存占用可达 ~900 MB，10s 累积可能达到 **9 GB**
    - 高并发场景下极易触发 OOM

2. **计算开销大**：
    - 10s 聚合时需要对所有 users 的所有 SQL 进行全量排序
    - 1000 users × 5000 SQL 的排序复杂度为 O(n log n)，耗时可能超过百毫秒
    - 影响 collectWorker 主循环，可能导致上报延迟

3. **缺乏前置保护**：
    - 1s 采集时不做限流，完全依赖 10s 过滤
    - 如果 10s 过滤失败或延迟，内存可能失控
    - 缺少"fail-safe"机制

### 8.2 推荐方案：三层过滤机制

**方案描述**：
- **Layer 1**：每个 timestamp 限制 200 users，第 201 个 user 时按 totalRU 比较淘汰
- **Layer 2**：每个 user 限制 200 SQLs，第 201 个 SQL 时按 totalRU 比较淘汰
- **Layer 3**：所有被淘汰的 RU 汇总到 `_others_`

**优势**：

1. **内存可控**：
    - 1s 采集时即做限流，`ruIncrementBuffer` 最多 200 users × 200 SQLs = 40,000 条目
    - 每条目约 180 字节，最坏情况下内存占用 **~7.2 MB**（40,000 × 180 字节），10s 累积 **~72 MB**
    - 内存上界可预测，不受极端场景影响

2. **计算开销低**：
    - 增量计算：每次只需与当前 minRU 比较，O(1) 复杂度
    - 懒计算：仅在 minRUDirty 时重新计算 min，避免频繁遍历
    - 10s 聚合时只需平移数据，无需排序

3. **前置保护**：
    - 1s 采集时即做限流，避免 buffer 膨胀
    - 即使 10s 过滤失败，内存也不会失控
    - 双重保障：1s 限流 + 10s 过滤

4. **数据精度高**：
    - 保留每个 timestamp 的 Top 200 users 和每个 user 的 Top 200 SQLs
    - 相比原方案的"10s 聚合后再取 Top 100"，三层过滤保留了更多中间状态
    - 边界 SQL（第 199-201 名）的数据更完整

**权衡**：
- 实现复杂度稍高（需要维护 minRU 和 dirty flag）
- 但换来的是内存安全性和计算效率的大幅提升

### 8.3 方案对比

| 维度 | 原方案（10s TopK） | 推荐方案（三层过滤） |
|------|-------------------|--------------------|
| 内存上界 | 不可控（极端场景 9 GB） | 可控（最坏 72 MB） |
| 计算复杂度 | O(n log n) 排序 | O(1) 增量比较 |
| 前置保护 | 无 | 1s 限流 |
| 实现复杂度 | 简单 | 中等 |
| 数据精度 | 10s 聚合后 Top 100 | 1s Top 200 + 10s 过滤 |
| 极端场景 | 可能 OOM | 内存可控 |

**结论**：推荐采用**三层过滤机制**，在保证数据精度的同时，实现内存和计算的双重可控。
