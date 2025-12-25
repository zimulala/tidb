# TiDB TopSQL RU 扩展设计

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

TiDB X（Next-gen TiDB Cloud）主要面向客户销售并按 **RU（Request Unit / Resource Unit）** 计费，Cost-Effective（成本效益）要求 RU 成本具备更好的可解释性与可操作性。

**核心场景**：当集群存在 **MAXRU 限制**且持续达到上限时（通常会触发告警通知用户），用户登录后需要能快速看到**是哪条/哪些 SQL 在该时间窗口内消耗 RU 最高**，从而人工判断是否需要对该 SQL 执行 **Terminate**，以尽快解除 RU 压力并恢复服务。

当前 TiDB 已具备慢日志、TopSQL、Statement Summary 等能力，但在“按 RU 实时定位高消耗 SQL（尤其是执行中 SQL）”方面仍存在不足，因此本设计将 TopRU 作为 TopSQL 的扩展来补齐该能力。

1. **慢日志 (Slow Log)**: 记录执行时间超过阈值的 SQL,包含完整的 RU 信息,但仅针对已完成的慢查询,无法实时反映正在执行中的 SQL RU 消耗情况。

2. **TopSQL**: 主要聚焦 CPU 时间消耗,按 SQL Digest + Plan Digest 维度聚合,提供了完善的采集、聚合和上报机制,但未提供按 RU 消耗排序的能力,且缺少用户维度的聚合。

3. **Statement Summary**: 提供 SQL 级别的聚合统计,但更新频率较低(默认 30 秒),且同样仅统计已完成的 SQL。

**设计决策:**

考虑到 TiDB 可观测性模块的统一性、实现的复杂性和后续的扩展性,我们决定**将 TopRU 功能作为 TopSQL 的扩展**来实现,而不是创建一个新的独立模块。这样做的好处是:

1. **统一性**: 复用 TopSQL 现有的采集、聚合和上报基础设施,保持可观测性模块的统一
2. **实现简单**: 只需要在 TopSQL 现有数据结构中添加 RU 和 user 字段,无需重新设计数据模型
3. **扩展性好**: 在 TopSQL 基础上扩展更自然,后续可以继续添加其他维度的指标

因此,我们在 TopSQL 中扩展以下功能:
- 添加 RU 相关字段(TotalRU)到 TopSQL 数据模型
- 添加 user 字段到聚合键,支持按 `(user, sql_digest, plan_digest)` 维度聚合
- 实现定期采样机制（**本地 1 秒采集 + 用户侧默认 60 秒刷新**，可配置为 15s/30s/60s）,支持执行中 SQL 的实时 RU 统计
- 扩展查询接口,支持按 RU 消耗排序

### 1.2 Goals

TopSQL RU 扩展的核心目标是:

1. **按累计 RU 消耗排序**: 在 TopSQL 中支持按累计 RU 消耗进行排序,而不仅仅是 CPU 时间排序,能够识别出即使执行时间短但 RU 消耗大的 SQL。

2. **支持用户维度聚合**: 在 TopSQL 现有 `(sql_digest, plan_digest)` 聚合基础上,扩展为 `(user, sql_digest, plan_digest)` 维度聚合,便于按用户维度进行资源治理和配额管理。

3. **实时 RU 统计**: 通过 **1 秒本地采样** 读取执行中 SQL 的 RU 增量并写入时间桶；用户侧由 vector-extensions **按刷新间隔**拉取 TopSQL 查询结果（默认 60 秒，可配置 15s/30s/60s），从而在用户视角实现“分钟级可见、可按需调优到更实时”的 TopRU。

4. **保持 TopSQL 现有能力**: 扩展不影响 TopSQL 现有的 CPU 时间统计、执行次数统计等功能,两者可以并存。

### 1.3 Non-Goals

以下内容不在本次设计范围内:

1. **TopRU 中 RU 与 Billing RU 等价**:
   - TopRU 展示的 RU 来自 `util.RUDetails` 的运行时观测值,用于排障/定位高消耗 SQL
   - Billing 侧 RU 可能包含计费口径的聚合/取整/折扣/多维度归因等逻辑
   - 因此 **TopRU 的 RU 与 Billing RU 不保证一致**,本期不做“对齐计费口径”的工作

2. **执行中 TopRU 一定可展示完整 SQL query / SQL statement 文本**:
   - 对于 TopRU 列表中的执行中 SQL,即使已消耗大量 RU,也可能因为 SQL 文本/元信息尚未注册或无法获取而只能展示 digest 维度信息
   - 本期目标聚焦于“定位高 RU 的 SQL（digest/plan/user）与趋势/增量”,不保证对每条执行中 SQL 都能展示可读的 SQL 文本

3. **RU Baseline 与历史对比**: 基于历史数据提供 RU 消耗 baseline 对比功能,作为后续扩展方向。

4. **异常检测与趋势分析**: 自动检测 RU 消耗异常并生成告警的功能,作为后续扩展方向。

5. **跨集群/多租户视图**: 本次仅设计单集群内的 RU 统计能力。

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
  
- **Phase 3**: 查询接口扩展(2 周)
  - 扩展 TopSQL 查询接口支持按 RU 排序
  - 支持按 user 维度查询
  - 实现 RU Share Percent 计算
  
- **Phase 4**: 测试与文档(2 周)
  - 功能测试、兼容性测试、性能测试
  - 文档完善

### 2.2 交付物

1. 实现代码: 在 `pkg/util/topsql/` 目录下扩展现有实现
2. 查询接口: 扩展 TopSQL 的查询接口支持 RU 排序
3. 配置参数: 相关系统变量与控制参数(复用 TopSQL 现有配置)
4. 测试用例: 单元测试与集成测试
5. 设计文档: 本文档

## 3. Detailed Design

### 3.1 功能与语义定义

#### 3.1.1 TopSQL RU 扩展的精确定义

**TopSQL RU 扩展**是指在 TopSQL 现有功能基础上,添加 RU (Request Unit) 字段和 user 字段,支持按 RU 消耗排序和按用户维度聚合。

**RU 定义**: RU (Request Unit) 表示 SQL 执行过程中累计的资源消耗。RU 包括读请求和写请求消耗的总和,通过 `util.RUDetails` 的 `RRU()` 和 `WRU()` 方法获取,最终计算为 `TotalRU = RRU + WRU`。

**RU 采集时机**（详细见 `3.2.1`）:
- 三层定期采样: **1s 本地采集 → 60s 上报/持久化 → 用户刷新(默认 60s，可配 15/30/60)**，均写入 TopSQL 时间桶(`PrecisionSeconds`)
- 执行完成时补采一次,保证最终值准确

**聚合维度**:
- 扩展后的聚合键为 `(user, sql_digest, plan_digest)` 三元组
- `user`: 执行 SQL 的用户名,从 `SessionVars.User.Username` 获取
- `sql_digest`: SQL Digest,用于标识 SQL 语句模式
- `plan_digest`: Plan Digest,用于标识执行计划

#### 3.1.2 与现有 TopSQL 功能的关系

| 维度 | TopSQL (现有) | TopSQL RU 扩展 |
|------|--------------|----------------|
| **排序依据** | CPU 时间 | CPU 时间 或 RU 消耗(可选) |
| **统计对象** | 已完成的 SQL | 已完成的 SQL(不变) |
| **更新频率** | 实时(基于 PrecisionSeconds) | 实时(复用现有机制) |
| **聚合维度** | SQL Digest + Plan Digest | User + SQL Digest + Plan Digest |
| **时间窗口** | 基于 ReportIntervalSeconds | 复用现有时间窗口机制 |
| **数据保留** | 内存中短期保留 | 复用现有保留机制 |

**扩展的核心价值**:
1. **资源视角**: 在现有 CPU 时间排序基础上,增加按 RU 排序的能力,更符合资源治理需求
2. **用户维度**: 增加用户维度聚合,支持按用户进行资源配额管理和审计
3. **统一实现**: 复用 TopSQL 现有的采集、聚合和上报机制,降低实现复杂度

### 3.2 聚合与排序模型

#### 3.2.1 RU 数据的采集时机

**RU 采集机制**:

为了支持执行中 SQL 的实时 RU 统计,需要采用定期采样机制,而不是仅在执行完成时采集。采用三层采样机制,平衡实时性、性能和资源消耗。

**采集位置**:

1. **本地定期采样** (执行中 SQL):
   - 在 SQL 执行过程中,每 1 秒定期采集一次 RU 数据
   - 通过后台 goroutine 定期检查正在执行的 SQL,读取其 `util.RUDetails` 中的当前 RU 值
   - 采集点: 在 TopSQL reporter 内新增独立的 RU 采集 worker(1s ticker),定期扫描所有活跃的 SQL 执行
   - 采集到的 RU 数据立即更新到本地时间桶中

2. **上报/持久化采样**:
   - 每 60 秒将本地时间桶的 RU 数据上报给其他组件用于持久化和管理
   - 类似 Prometheus 的上报机制,定期批量上报数据
   - 触发点: 复用 TopSQL 现有上报链路(由 `collectWorker` 的 reportTicker 触发 `takeDataAndSendToReportChan`,再由 `reportWorker` 发送)
   - 上报的数据可以用于持久化存储、告警、监控等用途

3. **用户查询刷新** (默认 60 秒，可配置 15s/30s/60s):
   - **说明**: 用户侧刷新在 vector-extensions 组件中处理
   - TopSQL 提供查询接口,返回本地时间桶的 RU 数据
   - vector-extensions 组件按刷新间隔拉取并刷新用户可见的数据（默认 60 秒，可配置 15s/30s/60s）
   - 本文档仅在此处提及,详细实现见 vector-extensions 组件文档

4. **执行完成时采集** (补充机制):
   - SQL 执行完成时,采集最终的 RU 数据,确保最终数据准确
   - 采集点: `pkg/executor/adapter.go` 的 `observeStmtFinishedForTopSQL()` 方法
   - 用于补充定期采样可能遗漏的最终数据

**实现方案**:

#### 3.2.1.1 活跃 SQL 获取机制

**复用 stmtstats aggregator**:

TopSQL 已经通过 `stmtstats.aggregator` 管理所有活跃的 `StatementStats` 实例。我们可以复用这个机制来获取活跃的 SQL 执行,无需新建独立的管理结构。

**aggregator 机制回顾**:

```go
// aggregator 已经管理了所有 StatementStats
type aggregator struct {
    statsSet   sync.Map // map[*StatementStats]struct{} - 所有注册的 StatementStats
    collectors sync.Map // map[Collector]struct{}
}

// StatementStats 在创建时自动注册到 globalAggregator
func CreateStatementStats() *StatementStats {
    stats := &StatementStats{...}
    globalAggregator.register(stats)  // 自动注册
    return stats
}

// aggregator 定期(1秒)遍历所有 StatementStats,收集数据
func (m *aggregator) aggregate() {
    m.statsSet.Range(func(statsR, _ any) bool {
        stats := statsR.(*StatementStats)
        if stats.Finished() {
            m.unregister(stats)  // 自动清理已完成的
        }
        total.Merge(stats.Take())  // 收集数据
        return true
    })
}
```

**扩展 StatementStats 支持 RU 采集**:

为了支持 RU 定期采集,需要在 `StatementStats` 中扩展,存储当前执行的上下文信息。

**为什么需要 activeExecution（而不是直接扩展 data 字段）？**

有三个主要原因：**数据生命周期和处理频率不同**、**数据结构限制**和**Key 维度不同**。

1. **数据生命周期和处理频率不同**:
   ```go
   // aggregator 每1秒调用一次 Take(),取走数据并清空
   func (s *StatementStats) Take() StatementStatsMap {
       s.mu.Lock()
       defer s.mu.Unlock()
       data := s.data
       s.data = StatementStatsMap{}  // 清空！
       return data
   }
   ```

   **数据流对比**:
   - **CPU 时间数据流**: `StatementStats.data` → `Take()` → `stmtStatsBuffer` → `processStmtStatsData()` → `collecting.records`
   - **RU 数据期望流**: 需要在执行过程中持续访问,但如果通过 `data` 字段,会进入上述 CPU 时间数据流

   **问题分析**:
   - 如果 RU 数据也通过 `data` 字段流转,会与 CPU 时间数据混在一起
   - `processStmtStatsData()` 按 `(sql_digest, plan_digest)` 处理,不支持 `(user, sql_digest, plan_digest)`
   - RU 采样周期调整为 1 秒,与 CPU 时间收集周期对齐
   - RU 数据会被 `Take()` 取走,导致采样过程中间状态丢失

2. **data 存储的是聚合后的统计数据,不能包含执行上下文**:
   - `data` 存储的是 `StatementStatsItem`(聚合后的统计数据)
   - `StatementStatsItem` 会被序列化并通过 protobuf 传输到 DataSink
   - 不能包含 `context.Context`(无法序列化)
   - 不能包含中间状态(`LastRUSample`)用于计算 RU 差值(原子操作状态)
   - RU 采样需要访问 `util.RUDetails` 上下文,计算增量值

3. **Key 维度不同**:
   - `data` 的 key 是 `SQLPlanDigest` = `(sql_digest, plan_digest)`,不包含 `user`
   - RU 统计需要按 `(user, sql_digest, plan_digest)` 维度聚合,以支持按用户进行资源治理
   - 即使扩展 `data` 的 key,也无法区分 CPU 时间和 RU 数据流
   - RU 数据需要独立的 `collecting.ruRecords` 来存储和管理(数据会被 Take() 取走、不能存储执行上下文)

**因此,需要新建 `activeExecution` 来存储**:
   - 执行上下文(`context.Context`)用于定期采样时读取 `util.RUDetails`
   - 中间状态(`LastRUSample`)用于计算 RU 差值
   - 这些数据需要在执行过程中持续存在,不能被 `Take()` 取走
   - 不会被序列化,只在内存中使用

**设计方案**:

```go
// 扩展 StatementStats 结构
type StatementStats struct {
    data     StatementStatsMap  // 存储聚合后的统计数据(会被 Take() 取走)
    finished *atomic.Bool
    mu       sync.Mutex
    
    // 新增: 当前活跃执行的上下文信息(用于 RU 采样,不会被 Take() 取走)
    //
    // 说明: `StatementStats` 是“每个 session 一个”(见 pkg/util/topsql/stmtstats/stmtstats.go 注释),
    // MySQL 协议下同一 session 同时只会有一条语句在执行,因此这里用 **单个** activeExecution 即可。
    // 若未来出现“同一 session 并发执行多条 SQL”(例如协议/执行模型变化),可平滑演进为 map[execID]*ActiveExecution。
    activeExecution *ActiveExecution
}

type ActiveExecution struct {
    SQLDigest   []byte
    PlanDigest []byte
    User        string
    Ctx         context.Context     // 用于获取 util.RUDetails
    StartTime   time.Time
    LastRUSample *atomic.Float64    // 上次采样的 RU 值,用于计算差值
    LastSampleTime *atomic.Int64    // 上次采样时间(Unix 时间戳)
}

// 新增方法: 注册活跃执行
func (s *StatementStats) RegisterActiveExecution(sqlDigest, planDigest []byte, user string, ctx context.Context) {
    s.mu.Lock()
    defer s.mu.Unlock()

    s.activeExecution = &ActiveExecution{
        SQLDigest:   sqlDigest,
        PlanDigest: planDigest,
        User:        user,
        Ctx:         ctx,
        StartTime:   time.Now(),
        LastRUSample: atomic.NewFloat64(0.0),
        LastSampleTime: atomic.NewInt64(time.Now().Unix()),
    }
}

// 新增方法: 注销活跃执行
func (s *StatementStats) UnregisterActiveExecution(sqlDigest, planDigest []byte, user string) {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 保护性校验: 仅当当前 activeExecution 与本次结束的语句匹配时才清理
    if s.activeExecution == nil {
        return
    }
    if string(s.activeExecution.SQLDigest) == string(sqlDigest) &&
        string(s.activeExecution.PlanDigest) == string(planDigest) &&
        s.activeExecution.User == user {
        s.activeExecution = nil
    }
}

// 新增方法: 获取当前活跃执行(用于 RU 采样)
func (s *StatementStats) GetActiveExecution() *ActiveExecution {
    s.mu.Lock()
    defer s.mu.Unlock()
    return s.activeExecution
}

// 在 adapter.go 的 observeStmtBeginForTopSQL 中注册
func (a *ExecStmt) observeStmtBeginForTopSQL(ctx context.Context) context.Context {
    // ... 现有代码 ...
    
    if stats := a.Ctx.GetStmtStats(); stats != nil && topsqlstate.TopSQLEnabled() {
        sqlDigest, planDigest := a.getSQLPlanDigest()
        user := ""
        if a.Ctx.GetSessionVars().User != nil {
            user = a.Ctx.GetSessionVars().User.Username
        }
        
        // 注册活跃执行到 StatementStats
        stats.RegisterActiveExecution(sqlDigest, planDigest, user, ctx)
    }
    
    return ctx
}

// 在 adapter.go 的 observeStmtFinishedForTopSQL 中注销
func (a *ExecStmt) observeStmtFinishedForTopSQL() {
    // ... 现有代码 ...
    
    if stats := a.Ctx.GetStmtStats(); stats != nil {
        sqlDigest, planDigest := a.getSQLPlanDigest()
        user := ""
        if a.Ctx.GetSessionVars().User != nil {
            user = a.Ctx.GetSessionVars().User.Username
        }
        
        // 从 StatementStats 中注销活跃执行
        stats.UnregisterActiveExecution(sqlDigest, planDigest, user)
    }
    
    // ... 执行完成时的 RU 采集 ...
}
```

**优势**:

1. **复用现有机制**: 无需新建 ActiveSQLContextManager,直接复用 aggregator 的 `statsSet`
2. **自动管理**: aggregator 会自动清理已完成的 StatementStats(`Finished() == true`)
3. **减少复杂度**: 活跃 SQL 的管理与 StatementStats 的生命周期绑定
4. **线程安全**: 复用 StatementStats 现有的 mutex 保护

#### 3.2.1.2 本地采集流程(1秒)

**数据存储位置**:

本地采集的 RU 数据存储在 TopSQL 现有的时间桶结构中,每个时间桶包含该时间段内的 RU 增量数据:

```go
// 在 TopSQL 的 record 结构中扩展
type record struct {
    tsIndex        map[uint64]int
    sqlDigest      []byte
    planDigest     []byte
    user           string
    tsItems        tsItems
    totalCPUTimeMs uint64
    totalRU        float64  // 累计总 RU
}

// tsItem 通过 stmtStats 访问 TotalRU
type tsItem struct {
    stmtStats stmtstats.StatementStatsItem  // 包含 TotalRU 字段
    timestamp uint64
    cpuTimeMs uint32
}
```

**本地采集实现(复用 aggregator)**:

RU 数据采集采用与 CPU 时间相同的 1 秒周期,可以更好地复用现有 aggregator 机制:

```go
// 在 TopSQL reporter 中添加定期 RU 采集(本地采集层,1秒)
func (tsr *RemoteTopSQLReporter) collectRUPeriodically() {
    ticker := time.NewTicker(1 * time.Second) // 1 秒本地采集间隔(与 aggregator 的 1 秒对齐)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            // 扫描所有活跃的 SQL 执行,更新本地时间桶
            tsr.collectActiveSQLRU()
        case <-tsr.ctx.Done():
            return
        }
    }
}

func (tsr *RemoteTopSQLReporter) collectActiveSQLRU() {
    now := time.Now()
    timestamp := uint64(now.Unix())
    
    // 第一步: 复用 aggregator 获取所有活跃的 StatementStats
    // 通过 aggregator.statsSet 遍历所有未 Finished 的 StatementStats
    stmtstats.GetGlobalAggregator().RangeActiveStats(func(stats *stmtstats.StatementStats) bool {
        // 获取该 StatementStats 中当前活跃执行(每个 session 同时最多一条语句执行)
        exec := stats.GetActiveExecution()
        if exec == nil {
            return true
        }

            // 1. 从上下文中读取当前 RU 值
            var currentRU float64
            if ruDetailsVal := exec.Ctx.Value(util.RUDetailsCtxKey); ruDetailsVal != nil {
                ruDetails := ruDetailsVal.(*util.RUDetails)
                currentRU = ruDetails.RRU() + ruDetails.WRU()
            } else {
                // 如果没有 RUDetails,跳过本次采样
                return true
            }
            
            // 2. 计算 RU 差值(增量)
            // 第一次采样时,LastRUSample = 0,差值 = currentRU
            // 后续采样时,差值 = currentRU - LastRUSample
            // 使用原子操作读取 LastRUSample,避免加锁
            lastRUSample := exec.LastRUSample.Load()
            ruDelta := currentRU - lastRUSample
            
            // 3. 如果差值 <= 0,说明 RU 没有增长,跳过本次采样
            // (理论上不应该发生,因为 RU 是累计值,但需要防护边界情况)
            if ruDelta <= 0 {
                return true
            }
            
        // 4. 更新到对应时间桶的 RU 数据
        // 获取或创建对应的 RU record (按 user + sqlDigest + planDigest)
        record := tsr.collecting.getOrCreateRURecord(exec.User, exec.SQLDigest, exec.PlanDigest)
            
            // 获取或创建对应时间戳的 tsItem
            tsItem := record.getOrCreateTsItem(timestamp)
            
            // 累加 RU 增量到该 tsItem 的 StatementStatsItem
            tsItem.stmtStats.TotalRU += ruDelta
            
            // 5. 更新活跃执行的上次采样值和时间
            // 使用原子操作更新,避免加锁
            exec.LastRUSample.Store(currentRU)
            exec.LastSampleTime.Store(now.Unix())
        
        return true  // 继续遍历下一个 StatementStats
    })
}

// 需要在 stmtstats 包中添加辅助方法
// 在 aggregator.go 中添加:
func (m *aggregator) RangeActiveStats(fn func(*StatementStats) bool) {
    m.statsSet.Range(func(statsR, _ any) bool {
        stats := statsR.(*StatementStats)
        if stats.Finished() {
            return true  // 跳过已完成的,继续下一个
        }
        return fn(stats)  // 调用回调函数处理未完成的 StatementStats
    })
}

// 在 aggregator.go 中暴露 globalAggregator (或通过包级函数)
func GetGlobalAggregator() *aggregator {
    return globalAggregator
}
```

**与 aggregator 常规收集流程的区别**:

1. **采集频率不同**:
   - aggregator 常规收集: 1 秒一次,调用 `CollectStmtStatsMap` 收集 CPU 时间等数据
   - RU 采集: 1 秒一次,与 CPU 时间采集对齐

2. **数据处理方式不同**:
   - aggregator 常规收集: 调用 `stats.Take()`,会清空 `StatementStats.data`,数据被取走
   - RU 采集: 不调用 `Take()`,直接更新时间桶,不清空 StatementStats 的数据

3. **数据流向不同**:
   - aggregator 常规收集: StatementStats → aggregator → Collector(如 RemoteTopSQLReporter)
   - RU 采集: StatementStats.activeExecution → 直接更新 TopSQL reporter 的时间桶

**RU 差值处理逻辑**:

1. **首次采样**:
   - `LastRUSample = 0`
   - `currentRU = 100` (假设)
   - `ruDelta = 100 - 0 = 100`
   - 将 100 RU 累加到对应时间桶

2. **后续采样(1秒后)**:
   - `LastRUSample = 100`
   - `currentRU = 250` (假设 RU 继续增长)
   - `ruDelta = 250 - 100 = 150`
   - 将 150 RU 累加到对应时间桶

3. **处理边界情况**:
   - 如果 `ruDelta <= 0`: 跳过本次采样(理论上不应该发生,但需要防护)
   - 如果 `util.RUDetails` 为空: 跳过本次采样
   - 如果 SQL 执行完成: 从活跃列表中移除,不再采样

**两个数据来源的处理**:

1. **定期采样(1秒)**:
   - **数据来源**: 
     - 通过 aggregator 获取所有活跃的 `StatementStats`
     - 从每个 `StatementStats.activeExecution` 中读取活跃执行的上下文
     - 从执行上下文中读取 `util.RUDetails` 的当前值
   - **处理方式**: 
     - 计算 RU 差值(增量): `ruDelta = currentRU - LastRUSample`
     - 将差值累加到对应时间桶的 `tsItem.stmtStats.TotalRU`
     - 更新活跃执行的 `LastRUSample = currentRU`(使用原子操作)
     - **关键**: 使用 `collecting.getOrCreateRURecord(user, sqlDigest, planDigest)` 获取或创建 RU record
     - RU record 的 key 为 `(user, sql_digest, plan_digest)`,与 CPU 时间统计的 record 分离
   - **存储位置**: TopSQL 的 `collecting.ruRecords[userSQLPlanDigest].tsItems[timestamp].stmtStats.TotalRU`
   - **特点**: 
     - 每次采样只记录增量,避免重复计算
     - 复用 aggregator 机制,无需维护独立的活跃 SQL 列表
     - 与 aggregator 的常规收集对齐(1秒),可以更好地复用现有机制
     - RU 数据按 user 维度存储,支持每个 user 的 Top 100

2. **执行完成时采集**:
   - **数据来源**: SQL 执行完成时,从 `util.RUDetails` 读取最终值
   - **处理方式**: 
     - 从当前 `StatementStats` 中查找对应的活跃执行
     - **如果找到活跃执行**: 
       - 计算最终 RU 与上次采样的差值: `ruDelta = finalRU - LastRUSample`
       - 将差值累加到时间桶(与定期采样相同)
       - 从 `StatementStats.activeExecution` 中移除
       - 使用 `collecting.getOrCreateRURecord(user, sqlDigest, planDigest)` 获取或创建 RU record
     - **如果未找到活跃执行**: 
       - 直接使用最终 RU 值(这种情况较少见,可能是 SQL 执行很快 < 1秒,或者活跃执行已被清理)
       - 将最终 RU 值累加到时间桶
       - 同样使用 `collecting.getOrCreateRURecord(user, sqlDigest, planDigest)` 获取或创建 RU record
   - **存储位置**: 与定期采样相同,累加到 `collecting.ruRecords[userSQLPlanDigest].tsItems[timestamp].stmtStats.TotalRU`
   - **作用**: 确保最终数据准确,补充定期采样可能遗漏的数据(特别是执行时间 < 1秒的 SQL)

```go
// 执行完成时的 RU 采集
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
        
        // 获取该 session 当前活跃执行(若存在)
        activeExec := stats.GetActiveExecution()
        
        if activeExec != nil &&
            string(activeExec.SQLDigest) == string(sqlDigest) &&
            string(activeExec.PlanDigest) == string(planDigest) &&
            activeExec.User == user {
            // 如果找到活跃执行,计算最终 RU 与上次采样的差值
            lastRUSample := activeExec.LastRUSample.Load()
            ruDelta := finalRU - lastRUSample
            if ruDelta > 0 {
                // 累加差值到时间桶(通过 OnRUSample 方法)
                timestamp := uint64(time.Now().Unix())
                stats.OnRUSample(sqlDigest, planDigest, user, ruDelta, timestamp)
            }
            // 从 StatementStats 中移除活跃执行
            stats.UnregisterActiveExecution(sqlDigest, planDigest, user)
        } else {
            // 如果未找到活跃执行,直接使用最终 RU 值
            // (这种情况较少见,可能是 SQL 执行很快 < 1秒,或者活跃执行已被清理)
            timestamp := uint64(time.Now().Unix())
            stats.OnRUSample(sqlDigest, planDigest, user, finalRU, timestamp)
        }
        
        // 调用扩展后的 OnExecutionFinished
        stats.OnExecutionFinished(sqlDigest, planDigest, execDuration, 
            vars.OutPacketBytes.Load(), user, finalRU)
    }
}

// OnRUSample 方法用于更新 RU 数据到时间桶
func (s *StatementStats) OnRUSample(sqlDigest, planDigest []byte, user string, ruDelta float64, timestamp uint64) {
    // 这个方法需要与 TopSQL reporter 交互,将 RU 数据更新到时间桶
    // 具体实现见 TopSQL reporter 部分
}
```

#### 3.2.1.3 上报/持久化流程(60秒)

**复用现有的 collectWorker 函数**:

RU 数据上报复用 TopSQL 现有的 `collectWorker` 函数,频率与 CPU/StmtStats 数据对齐(60秒):

```go
// 现有的 collectWorker 函数(复用,不需要修改)
func (tsr *RemoteTopSQLReporter) collectWorker() {
    currentReportInterval := topsqlstate.GlobalState.ReportIntervalSeconds.Load() // 默认 60 秒
    reportTicker := time.NewTicker(time.Second * time.Duration(currentReportInterval))
    defer reportTicker.Stop()
    for {
        select {
        // ... 其他 case ...
        case <-reportTicker.C:
            tsr.processStmtStatsData()  // 处理 CPU/StmtStats 数据
            tsr.takeDataAndSendToReportChan()  // 取数据并发送上报(包括 RU 数据)
            // Update `reportTicker` if report interval changed.
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
```

#### 3.2.1.4 用户查询刷新(默认 60 秒，可配置 15s/30s/60s)

**说明**: 用户侧刷新功能在 vector-extensions 组件中处理,本文档仅在此处提及。TopSQL 提供查询接口,返回本地时间桶的 RU 数据；vector-extensions 组件按刷新间隔拉取并刷新用户可见的数据（默认 60 秒，可配置 15s/30s/60s）。

// 在 adapter.go 中扩展 - 执行完成时的 RU 采集
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
        
        // 获取该 session 当前活跃执行(若存在)
        activeExec := stats.GetActiveExecution()
        
        if activeExec != nil &&
            string(activeExec.SQLDigest) == string(sqlDigest) &&
            string(activeExec.PlanDigest) == string(planDigest) &&
            activeExec.User == user {
            // 如果找到活跃执行,计算最终 RU 与上次采样的差值
            lastRUSample := activeExec.LastRUSample.Load()
            ruDelta := finalRU - lastRUSample
            if ruDelta > 0 {
                // 累加差值到时间桶(通过 OnRUSample 方法)
                timestamp := uint64(time.Now().Unix())
                stats.OnRUSample(sqlDigest, planDigest, user, ruDelta, timestamp)
            }
            // 从 StatementStats 中移除活跃执行
            stats.UnregisterActiveExecution(sqlDigest, planDigest, user)
        } else {
            // 如果未找到活跃执行,直接使用最终 RU 值
            // (这种情况较少见,可能是 SQL 执行很快 < 1秒,或者活跃执行已被清理)
            timestamp := uint64(time.Now().Unix())
            stats.OnRUSample(sqlDigest, planDigest, user, finalRU, timestamp)
        }
        
        // 调用扩展后的 OnExecutionFinished
        stats.OnExecutionFinished(sqlDigest, planDigest, execDuration, 
            vars.OutPacketBytes.Load(), user, finalRU)
    }
}
```

**RU 数据来源**:

- RU 数据通过 `util.RUDetails` 结构从执行上下文中获取
- `util.RUDetails` 在 SQL 执行过程中,通过 KV 请求响应实时累积 RU 消耗
- 本地定期采样(1秒)时,读取 `util.RUDetails` 的当前值(可能还在增长)
- 执行完成时,读取 `util.RUDetails` 的最终值,确保数据准确

**时间桶分配**:

- 每个采样点的 RU 数据会分配到对应的时间桶中
- 时间桶的粒度与 TopSQL 的 `PrecisionSeconds` 配置一致(默认 1 秒)
- 三层采样机制的时间桶分配:
  - **本地采集层(1秒)**: 每 1 秒采集一次,立即更新到本地时间桶
  - **上报/持久化层(60秒)**: 每 60 秒读取本地时间桶数据,批量上报
   - **用户查询刷新层(默认 60 秒，可配置 15s/30s/60s)**: 由 vector-extensions 组件处理,从 TopSQL 查询接口获取本地时间桶数据
   - **执行完成时采集**: 从 `StatementStats.activeExecution` 中获取活跃执行,计算最终 RU 差值
- 如果 SQL 执行时间超过 1 秒,会有多个本地采样点
- 如果 SQL 执行时间超过 60 秒,会有多个上报点
- 如果 SQL 执行时间超过用户侧刷新间隔(默认 60 秒),vector-extensions 组件会看到多次刷新的数据

#### 3.2.2 User + SQL 维度的实现语义

**聚合键扩展**:

TopSQL 现有的聚合键为 `(sql_digest, plan_digest)`,扩展后为 `(user, sql_digest, plan_digest)`:

```go
// 现有结构
type SQLPlanDigest struct {
    SQLDigest  BinaryDigest
    PlanDigest BinaryDigest
}

// 扩展后结构
type UserSQLPlanDigest struct {
    User       string
    SQLDigest  BinaryDigest
    PlanDigest BinaryDigest
}
```

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
  - RU 数据: activeExecution → 定期采样/执行完成 → collecting.ruRecords
  
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
5. **内存可控**: user 上限 100,每个 user 最多 100 条记录,总记录数上限 10,000

#### 3.3.6 采集/上报融入方式评估（实现复杂度 / 侵入性 / 性能 / 维护成本）

这里单独评估“RU 数据如何融入 TopSQL 既有流水线”的两种实现路径（与“按 user 的 TopN 存储方案 A/B/C”是不同维度的问题，避免混淆）。

| 维度 | 路径 1：独立 RU 采集 + 复用上报链路（推荐） | 路径 2：RU 也走 stmtstatsBuffer / processStmtStatsData（备选） |
|------|-------------------------------------------|-------------------------------------------------------------|
| **实现复杂度** | 低-中：新增 RU sampler worker + `collecting.ruRecords` | 高：需要重构 stmtstats 的 key、采集/过滤、buffer 结构 |
| **对现有系统侵入性** | 低：不改 stmtstats/collector 的主干数据模型与 key | 高：stmtstats/aggregator/collector/reporter 多点联动修改 |
| **性能影响** | 可控：1s 扫描活跃执行，60s 批量上报（复用现有） | 不确定：key 维度扩张为 `(user, sql, plan)`，buffer 体积与处理开销上升 |
| **长期维护成本** | 低：RU 逻辑与 CPU/StmtStats 逻辑清晰分层，边界明确 | 高：RU 与原有 stmtstats 逻辑耦合，后续 stmtstats 变更更易相互影响 |

**推荐路径 1（当前文档采用）**:
- **采集**: 独立 RU sampler worker(1s ticker) 通过 `activeExecution` 读取 `util.RUDetails`，计算增量并直接写入 `collecting.ruRecords`
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
   - 在 TopSQL reporter 内新增独立的 RU 采集 worker(1s ticker)
   - 每 1 秒扫描一次所有活跃的 SQL 执行上下文
   - 从每个上下文中读取 `util.RUDetails` 的当前 RU 值
   - 立即更新到本地时间桶的 RU 数据
   - 需要维护活跃 SQL 执行上下文的注册表

2. **上报/持久化采样** (60秒):
   - 复用 TopSQL 现有的上报链路(`collectWorker` 周期触发 + `reportWorker` 发送)
   - 每 60 秒将本地时间桶中的 RU 数据打包上报
   - 批量上报给其他组件用于持久化和管理
   - 类似 Prometheus 的上报机制,减少网络开销

3. **用户查询刷新** (默认 60 秒，可配置 15s/30s/60s):
   - **说明**: 用户侧刷新功能在 vector-extensions 组件中处理
   - TopSQL 提供查询接口,返回本地时间桶的 RU 数据
   - vector-extensions 组件按刷新间隔拉取并刷新用户可见的数据（默认 60 秒，可配置 15s/30s/60s）
   - 本文档仅在此处提及,详细实现见 vector-extensions 组件文档

4. **执行完成时采集** (补充机制):
   - 在 `pkg/executor/adapter.go` 的 `observeStmtFinishedForTopSQL()` 方法中扩展
   - 从 `context.Context` 中获取 `util.RUDetailsCtxKey` 对应的 `RUDetails`
   - 从 `SessionVars.User.Username` 获取用户名
   - 调用扩展后的 `StatementStats.OnExecutionFinished()` 方法
   - 此时 `util.RUDetails` 包含该 SQL 的最终 RU 消耗信息
   - 用于补充定期采样可能遗漏的最终数据,确保数据准确性

5. **活跃执行上下文管理** (复用 StatementStats):
   - 在 SQL 执行开始时,通过 `StatementStats.RegisterActiveExecution()` 注册到 `StatementStats.activeExecution`
   - 在 SQL 执行完成时,通过 `StatementStats.UnregisterActiveExecution()` 从 `StatementStats.activeExecution` 移除
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
    
    // 新增: 定期采样 RU 数据的方法
    OnRUSample(sqlDigest, planDigest []byte, user string, totalRU float64, timestamp uint64)
}

```

#### 3.4.2 实时性与性能权衡

**实时性保证**:

1. **三层采样/刷新频率**: 
   - **本地采集层**: 每 1 秒采集一次,确保本地数据实时性
   - **上报/持久化层**: 每 60 秒上报一次,平衡实时性和网络开销
   - **用户查询刷新层**: 由 vector-extensions 组件处理,刷新间隔默认 60 秒,可配置 15s/30s/60s
   - 可以通过配置参数调整各层采样/刷新频率

2. **时间桶粒度**:
   - RU 数据的时间桶粒度复用 TopSQL 的 `PrecisionSeconds` 配置(默认 1 秒)
   - 本地采集频率(1秒)与时间桶粒度(1秒)对齐,每个采集点直接落入对应时间桶
   - 每次本地采样时,将 RU 值累加到对应的时间桶中
   - 上报和查询刷新从本地时间桶读取数据

3. **异步处理**:
   - 本地定期 RU 采集在独立的 goroutine 中执行,不阻塞 SQL 执行
   - 上报/持久化复用 TopSQL 现有的 `collectWorker/reportWorker`,无需新增独立 goroutine
   - 使用 TopSQL 现有的 channel 缓冲和后台 worker 机制
   - 活跃 SQL 上下文的注册和注销操作需要加锁保护
   - 定期采样时使用读锁,注册/注销时使用写锁,减少锁竞争

**性能优化**:

1. **内存优化**:
   - RU 字段仅增加少量内存开销(每个 record 增加约 8 字节,TotalRU 字段)
   - user 字段使用 string,内存开销可控
   - 活跃执行存储在 StatementStats 中,每个 StatementStats 的活跃执行数量有限(通常每个 session < 10)
   - 本地时间桶数据在内存中,上报后可以清理或压缩
   - 复用 TopSQL 现有的内存管理和清理机制

2. **CPU 优化**:
   - 本地定期 RU 采集(1秒)在独立 goroutine 中执行,不阻塞 SQL 执行
   - 上报/持久化(60秒)在现有的 collectWorker 中执行,不阻塞本地采集
   - StatementStats 内部使用 mutex 保护 activeExecution,定期采样时通过 aggregator 的 sync.Map 并发访问多个 StatementStats
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
- 内存开销: 每个 record 增加约 8 字节 + user 字符串长度 + 活跃 SQL 上下文
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
   - 活跃 SQL 上下文列表: 每个上下文约 100 字节,假设最多 1000 个活跃 SQL
   - 假设有 10,000 个不同的 `(user, sql_digest, plan_digest)` 组合
   - 额外内存: 10,000 * (8 + 20) + 1000 * 100 ≈ 380KB (可接受)
   
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

2. **防护措施** (复用 TopSQL 现有机制):
   - TopSQL 已经有最大记录数限制和清理机制
   - 复用 TopSQL 的过期数据清理逻辑
   - user 字符串可以共享以减少内存占用

3. **降级策略**:
   - 如果 TopSQL 功能被禁用,RU 扩展自然也被禁用
   - 复用 TopSQL 现有的降级和限流机制

**CPU 风险** (风险较低):

1. **风险场景**:
   - RU 数据采集开销很小,主要风险来自聚合和排序
   - 复用 TopSQL 现有的锁机制,锁竞争风险与 TopSQL 相同

2. **防护措施**:
   - 复用 TopSQL 现有的查询限流和采样机制
   - 按 RU 排序与按 CPU 排序使用相同的算法,性能相当

#### 3.5.3 近似计算 / 限流 / 降级策略

**复用 TopSQL 现有策略**:

1. **采样策略**: 复用 TopSQL 现有的采样和限流机制
2. **精度权衡**: RU 值使用 `float64` 类型,精度与 TopSQL 的 CPU 时间精度相当
3. **限流策略**: 复用 TopSQL 现有的查询限流机制
4. **降级策略**: 与 TopSQL 功能绑定,TopSQL 降级时 RU 扩展也降级

### 3.6 可扩展性与后续演进

#### 3.6.1 RU Baseline (历史对比)

**设计思路**:

1. **基线计算**:
   - 基于 TopSQL 历史数据,计算每个 `(user, sql_digest)` 的平均 RU 消耗
   - 支持多种基线算法: 平均值、P95、P99 等
   - 基线数据可以存储在系统表中

2. **对比展示**:
   - 在 TopSQL 查询结果中,增加 `BaselineRU` 和 `DeviationPercent` 字段
   - `DeviationPercent = (CurrentRU - BaselineRU) / BaselineRU * 100%`
   - 支持按偏差百分比排序,快速发现异常 SQL

**实现要点**:
- 基线计算采用离线批处理方式,避免影响在线性能
- 基线更新频率: 每天一次
- 可以复用 TopSQL 现有的数据存储机制

#### 3.6.2 异常检测 / 趋势分析

**异常检测**:

1. **检测算法**:
   - 基于统计方法: 3-sigma 规则,检测偏离均值超过 3 倍标准差的 SQL
   - 基于规则: 检测 RU 消耗突然增长、执行次数异常等模式
   - 可以复用 TopSQL 现有的时间序列数据

2. **告警机制**:
   - 检测到异常后,生成告警事件
   - 支持告警规则配置: 阈值、告警频率等
   - 告警可以通过邮件、Webhook 等方式通知

**趋势分析**:

1. **趋势计算**:
   - 利用 TopSQL 现有的时间桶数据,计算每个 SQL 的 RU 消耗趋势
   - 使用线性回归或移动平均等算法
   - 支持多时间粒度: 小时、天、周等

2. **可视化**:
   - 提供 RU 消耗趋势图表
   - 支持对比不同时间段的趋势
   - 可以与 TopSQL 现有的可视化工具集成

#### 3.6.3 与其他可观测性模块的联动

**数据关联**:

1. **关联键**:
   - 使用 `(sql_digest, plan_digest)` 作为关联键
   - 在 TopSQL 查询结果中,可以提供跳转到慢日志的链接

2. **统一视图**:
   - 在 TopSQL 查询结果中同时展示 CPU 时间和 RU 数据
   - 支持按 CPU 时间排序和按 RU 排序的切换
   - 可以与慢日志数据关联,提供更全面的 SQL 性能分析

**联动场景**:

1. **TopSQL 内部联动**:
   - 用户可以同时查看 SQL 的 CPU 时间和 RU 消耗
   - 发现 CPU 时间高但 RU 消耗低的 SQL(可能是有计算密集型操作)
   - 发现 RU 消耗高但 CPU 时间低的 SQL(可能是 I/O 密集型操作)

2. **与慢日志的联动**:
   - 用户在 TopSQL 中发现高 RU 消耗的 SQL
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

## 5. Compatibility Issues

### 5.1 Functional

1. **与 Resource Control 的兼容性**:
   - TopSQL RU 扩展依赖 `util.RUDetails` 收集 RU 数据,需要确保 Resource Control 功能已启用
   - 如果 Resource Control 未启用,`util.RUDetails` 可能为空,导致 RU 字段为 0
   - **解决方案**: 
     - 如果 Resource Control 未启用,RU 字段显示为 0,但不影响 TopSQL 其他功能
     - 在文档中说明需要启用 Resource Control 才能获得准确的 RU 数据

2. **与 TopSQL 现有功能的兼容性**:
   - RU 扩展作为 TopSQL 的一部分,与现有 CPU 时间统计功能完全兼容
   - 可以同时支持按 CPU 时间排序和按 RU 排序
   - **解决方案**: 
     - 保持向后兼容,现有的 CPU 时间查询不受影响
     - 新增 RU 相关字段和排序选项

3. **与 Statement Summary 的兼容性**:
   - TopSQL 可能从 Statement Summary 获取 SQL 文本,该机制保持不变
   - 但对于执行中 SQL/瞬时 SQL,SQL 文本可能存在缺失或延迟注册的情况,因此不保证 TopRU 一定能展示完整 SQL statement（见 Non-Goals / Limitation）
   - **解决方案**: 无特殊处理,复用现有机制

4. **与慢日志的兼容性**:
   - TopSQL RU 扩展和慢日志可以同时启用,两者独立工作
   - TopSQL RU 扩展可以提供慢日志中 RU 字段的聚合视图
   - **解决方案**: 无特殊处理,两者完全兼容

### 5.2 Upgrade

1. **从旧版本升级**:
   - 新版本在 TopSQL 中扩展 RU 功能,旧版本 TopSQL 无 RU 字段
   - 升级后,如果 TopSQL 已启用,则 RU 功能自动可用
   - **解决方案**: 
     - RU 扩展作为 TopSQL 的一部分,无需额外配置
     - 如果 TopSQL 未启用,需要先启用 TopSQL 才能使用 RU 功能
     - 在升级文档中说明 RU 功能的使用方法

2. **数据格式兼容性**:
   - TopSQL 的内存数据不持久化,升级后数据会丢失
   - Protobuf 消息格式扩展,需要考虑向前兼容
   - **解决方案**: 
     - RU 相关字段使用 optional 字段,旧版本客户端可以忽略
     - 使用 Protobuf 的向后兼容机制,确保旧版本客户端不会出错

3. **API 兼容性**:
   - TopSQL 查询接口扩展,添加按 RU 排序的选项
   - 保持现有接口不变,新增可选参数
   - **解决方案**: 
     - 新增参数使用默认值,现有调用不受影响
     - 在 API 文档中说明新参数的使用方法

## 6. Test Design

### 6.1 Functional Test

#### 6.1.1 基础功能测试

1. **数据采集测试**:
   - 测试本地定期采样(1 秒)是否正确采集执行中 SQL 的 RU 数据
   - 测试上报/持久化采样(60 秒)是否正确上报数据
   - 测试 TopSQL 查询接口是否正确返回 RU 数据(用户查询刷新由 vector-extensions 组件处理)
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
   - 测试执行时间 > 用户侧刷新间隔(默认 60 秒) 的 SQL 在 vector-extensions 组件中是否有多次刷新(由该组件处理)

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
   - 用户查询刷新由 vector-extensions 组件处理,性能测试见该组件文档

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
   - 目标: 内存使用稳定,与 TopSQL 现有表现一致

3. **OOM 防护**:
   - 复用 TopSQL 现有的 OOM 防护测试
   - 目标: RU 扩展不影响 TopSQL 现有的防护机制

## 7. Impact & Risk

### 7.1 Positive Impacts

1. **提升可观测性**: TopSQL RU 扩展提供了 RU 维度可观测能力,在现有 CPU 时间统计基础上增加了资源消耗视角。

2. **加速排障**: 能够快速定位高 RU 消耗的 SQL,加速问题排查。

3. **支持资源治理**: 按用户维度聚合,支持资源配额管理和审计。

4. **统一实现**: 作为 TopSQL 的扩展,保持了可观测性模块的统一性,降低实现复杂度。

### 7.2 Negative Impacts

1. **性能开销**: RU 扩展会带来少量的 CPU 和内存开销,但相对于 TopSQL 现有开销很小(< 1%)。

2. **内存使用**: 每个 record 增加约 50 字节内存,总体内存开销可控。

3. **复杂度增加**: 在 TopSQL 中增加 RU 相关字段和逻辑,但复杂度增加有限。

### 7.3 Risks

1. **性能风险**:
   - **风险**: RU 数据采集可能带来额外的性能开销(采集频率为 1 秒)
   - **缓解措施**:
     - RU 数据采集开销很小(从 context 读取已有对象)
     - 复用 TopSQL 现有的异步采集机制
     - 采集频率与 CPU 时间对齐(1 秒),不会增加额外的采集周期
     - 性能开销 < 1% CPU (仍在可接受范围内)

2. **内存风险**:
   - **风险**: user 字段的字符串存储可能增加内存使用
   - **缓解措施**:
     - user 字符串可以共享以减少内存占用
     - 复用 TopSQL 现有的内存管理和清理机制
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
   - **风险**: RU 扩展可能存在 bug,影响 TopSQL 稳定性
   - **缓解措施**:
     - 充分的单元测试和集成测试
     - 代码审查
     - 灰度发布
     - 如果出现问题,可以禁用 TopSQL 功能(同时禁用 RU 扩展)
