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

随着 TiDB 在大型集群中部署规模的增长,资源治理和排障能力变得日益重要。当前 TiDB 已具备慢日志、TopSQL、Statement Summary 等可观测性能力,但在 RU (Request Unit) 维度的实时定位能力仍存在不足。

**现有能力分析:**

1. **慢日志 (Slow Log)**: 记录执行时间超过阈值的 SQL,包含完整的 RU 信息,但仅针对已完成的慢查询,无法实时反映正在执行中的 SQL RU 消耗情况。

2. **TopSQL**: 主要聚焦 CPU 时间消耗,按 SQL Digest + Plan Digest 维度聚合,提供了完善的采集、聚合和上报机制,但未提供按 RU 消耗排序的能力,且缺少用户维度的聚合。

3. **Statement Summary**: 提供 SQL 级别的聚合统计,但更新频率较低(默认 30 秒),且同样仅统计已完成的 SQL。

**问题场景:**

- 在排查资源消耗异常时,需要按 RU 消耗排序而非 CPU 时间排序
- 需要按用户维度进行资源治理和配额管理
- 希望拉长查询窗口(如 24 小时)以发现异常 SQL 模式,而不仅仅是"正常但消耗大的 SQL"

**设计决策:**

考虑到 TiDB 可观测性模块的统一性、实现的复杂性和后续的扩展性,我们决定**将 TopRU 功能作为 TopSQL 的扩展**来实现,而不是创建一个新的独立模块。这样做的好处是:

1. **统一性**: 复用 TopSQL 现有的采集、聚合和上报基础设施,保持可观测性模块的统一
2. **实现简单**: 只需要在 TopSQL 现有数据结构中添加 RU 和 user 字段,无需重新设计数据模型
3. **扩展性好**: 在 TopSQL 基础上扩展更自然,后续可以继续添加其他维度的指标

因此,我们在 TopSQL 中扩展以下功能:
- 添加 RU 相关字段(TotalRU)到 TopSQL 数据模型
- 添加 user 字段到聚合键,支持按 `(user, sql_digest, plan_digest)` 维度聚合
- 实现定期采样机制(15 秒),支持执行中 SQL 的实时 RU 统计
- 扩展查询接口,支持按 RU 消耗排序

### 1.2 Goals

TopSQL RU 扩展的核心目标是:

1. **按累计 RU 消耗排序**: 在 TopSQL 中支持按累计 RU 消耗进行排序,而不仅仅是 CPU 时间排序,能够识别出即使执行时间短但 RU 消耗大的 SQL。

2. **支持用户维度聚合**: 在 TopSQL 现有 `(sql_digest, plan_digest)` 聚合基础上,扩展为 `(user, sql_digest, plan_digest)` 维度聚合,便于按用户维度进行资源治理和配额管理。

3. **实时 RU 统计**: 通过定期采样机制(15 秒),支持执行中 SQL 的实时 RU 统计,通过 TopSQL 现有的时间桶机制进行聚合,能够反映指定时间窗口内消耗 RU 最多的 SQL。

4. **保持 TopSQL 现有能力**: 扩展不影响 TopSQL 现有的 CPU 时间统计、执行次数统计等功能,两者可以并存。

### 1.3 Non-Goals

以下内容不在本次设计范围内:

1. **RU Baseline 与历史对比**: 基于历史数据提供 RU 消耗基线对比功能,作为后续扩展方向。

2. **异常检测与趋势分析**: 自动检测 RU 消耗异常并生成告警的功能,作为后续扩展方向。

3. **跨集群/多租户视图**: 本次仅设计单集群内的 RU 统计能力。

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

**RU 采集时机**:
- RU 数据需要实时采集,支持执行中 SQL 的 RU 统计
- 采用三层定期采样机制,不同层级有不同的采样频率:
  1. **本地采集层**: 每 2 秒从 `util.RUDetails` 中读取当前累计的 RU 消耗,更新到本地时间桶
  2. **上报/持久化层**: 每 5 秒将本地时间桶数据上报给其他组件用于持久化和管理(类似 Prometheus 的上报机制)
  3. **用户查询刷新层**: 每 15 秒刷新一次用户查询可见的 RU 数据,确保用户看到的数据相对实时
- 在 SQL 执行过程中,定期从 `util.RUDetails` 中读取当前累计的 RU 消耗
- 每个采样点的 RU 数据会累加到对应的时间桶中
- 执行完成时,也会采集一次最终的 RU 数据,确保最终数据准确
- TopSQL 现有的时间桶机制(基于 `PrecisionSeconds`)同样适用于 RU 数据

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
   - 在 SQL 执行过程中,每 2 秒定期采集一次 RU 数据
   - 通过后台 goroutine 定期检查正在执行的 SQL,读取其 `util.RUDetails` 中的当前 RU 值
   - 采集点: 在 TopSQL 的 `collectWorker` 中扩展,定期扫描所有活跃的 SQL 执行
   - 采集到的 RU 数据立即更新到本地时间桶中

2. **上报/持久化采样**:
   - 每 5 秒将本地时间桶的 RU 数据上报给其他组件用于持久化和管理
   - 类似 Prometheus 的上报机制,定期批量上报数据
   - 采集点: 在 TopSQL 的 `reportWorker` 中扩展
   - 上报的数据可以用于持久化存储、告警、监控等用途

3. **用户查询刷新** (15秒):
   - **说明**: 15 秒用户查询刷新功能在 vector-extensions 组件中处理
   - TopSQL 提供查询接口,返回本地时间桶的 RU 数据
   - vector-extensions 组件负责按 15 秒频率刷新用户可见的数据
   - 本文档仅在此处提及,详细实现见 vector-extensions 组件文档

4. **执行完成时采集** (补充机制):
   - SQL 执行完成时,采集最终的 RU 数据,确保最终数据准确
   - 采集点: `pkg/executor/adapter.go` 的 `observeStmtFinishedForTopSQL()` 方法
   - 用于补充定期采样可能遗漏的最终数据

**实现方案**:

#### 3.2.1.1 活跃 SQL 执行上下文管理

**活跃 SQL 注册机制**:

在 SQL 执行开始时,将执行上下文注册到活跃列表中:

```go
// 活跃 SQL 执行上下文管理结构
type ActiveSQLContextManager struct {
    mu sync.RWMutex
    // key: sessionID + sqlDigest + planDigest (唯一标识一个 SQL 执行)
    // value: SQL 执行上下文信息
    activeSQLs map[string]*ActiveSQLContext
}

type ActiveSQLContext struct {
    Key         string              // 唯一标识: sessionID + sqlDigest + planDigest
    SQLDigest   []byte
    PlanDigest []byte
    User        string
    Ctx         context.Context     // 用于获取 util.RUDetails
    StartTime   time.Time
    LastRUSample *atomic.Float64    // 上次采样的 RU 值,用于计算差值(使用原子操作)
    LastSampleTime *atomic.Int64    // 上次采样时间(Unix 时间戳,使用原子操作)
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
        
        // 生成唯一标识
        sessionID := a.Ctx.GetSessionVars().ConnectionID
        key := fmt.Sprintf("%d_%s_%s", sessionID, sqlDigest, planDigest)
        
        // 注册到活跃 SQL 列表
        activeSQLCtx := &ActiveSQLContext{
            Key:         key,
            SQLDigest:   sqlDigest,
            PlanDigest: planDigest,
            User:        user,
            Ctx:         ctx,
            StartTime:   time.Now(),
            LastRUSample: atomic.NewFloat64(0.0),
            LastSampleTime: atomic.NewInt64(time.Now().Unix()),
        }
        topsql.RegisterActiveSQL(activeSQLCtx)
    }
    
    return ctx
}

// 在 adapter.go 的 observeStmtFinishedForTopSQL 中注销
func (a *ExecStmt) observeStmtFinishedForTopSQL() {
    // ... 现有代码 ...
    
    // 从活跃列表中移除
    sqlDigest, planDigest := a.getSQLPlanDigest()
    sessionID := a.Ctx.GetSessionVars().ConnectionID
    key := fmt.Sprintf("%d_%s_%s", sessionID, sqlDigest, planDigest)
    topsql.UnregisterActiveSQL(key)
    
    // ... 执行完成时的 RU 采集 ...
}
```

#### 3.2.1.2 本地采集流程(2秒)

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

**本地采集实现**:

```go
// 在 TopSQL reporter 中添加定期 RU 采集(本地采集层,2秒)
func (tsr *RemoteTopSQLReporter) collectRUPeriodically() {
    ticker := time.NewTicker(2 * time.Second) // 2 秒本地采集间隔
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
    
    // 第一步: 使用读锁读取所有活跃 SQL 列表
    tsr.activeSQLManager.mu.RLock()
    activeSQLsCopy := make([]*ActiveSQLContext, 0, len(tsr.activeSQLManager.activeSQLs))
    for _, sqlCtx := range tsr.activeSQLManager.activeSQLs {
        activeSQLsCopy = append(activeSQLsCopy, sqlCtx)
    }
    tsr.activeSQLManager.mu.RUnlock()
    
    // 第二步: 遍历活跃 SQL,采集 RU 数据
    for _, sqlCtx := range activeSQLsCopy {
        // 1. 从上下文中读取当前 RU 值
        var currentRU float64
        if ruDetailsVal := sqlCtx.Ctx.Value(util.RUDetailsCtxKey); ruDetailsVal != nil {
            ruDetails := ruDetailsVal.(*util.RUDetails)
            currentRU = ruDetails.RRU() + ruDetails.WRU()
        } else {
            // 如果没有 RUDetails,跳过本次采样
            continue
        }
        
        // 2. 计算 RU 差值(增量)
        // 第一次采样时,LastRUSample = 0,差值 = currentRU
        // 后续采样时,差值 = currentRU - LastRUSample
        // 使用原子操作读取 LastRUSample,避免加锁
        lastRUSample := sqlCtx.LastRUSample.Load()
        ruDelta := currentRU - lastRUSample
        
        // 3. 如果差值 <= 0,说明 RU 没有增长,跳过本次采样
        // (理论上不应该发生,因为 RU 是累计值,但需要防护边界情况)
        if ruDelta <= 0 {
            continue
        }
        
        // 4. 更新到对应时间桶的 RU 数据
        // 获取或创建对应的 record (按 user + sqlDigest + planDigest)
        record := tsr.getOrCreateRecord(sqlCtx.SQLDigest, sqlCtx.PlanDigest, sqlCtx.User)
        
        // 获取或创建对应时间戳的 tsItem
        tsItem := record.getOrCreateTsItem(timestamp)
        
        // 累加 RU 增量到该 tsItem 的 StatementStatsItem
        tsItem.stmtStats.TotalRU += ruDelta
        
        // 5. 更新活跃 SQL 上下文的上次采样值和时间
        // 使用原子操作更新,避免加锁
        sqlCtx.LastRUSample.Store(currentRU)
        sqlCtx.LastSampleTime.Store(now.Unix())
    }
}
```

**RU 差值处理逻辑**:

1. **首次采样**:
   - `LastRUSample = 0`
   - `currentRU = 100` (假设)
   - `ruDelta = 100 - 0 = 100`
   - 将 100 RU 累加到对应时间桶

2. **后续采样(2秒后)**:
   - `LastRUSample = 100`
   - `currentRU = 250` (假设 RU 继续增长)
   - `ruDelta = 250 - 100 = 150`
   - 将 150 RU 累加到对应时间桶

3. **处理边界情况**:
   - 如果 `ruDelta <= 0`: 跳过本次采样(理论上不应该发生,但需要防护)
   - 如果 `util.RUDetails` 为空: 跳过本次采样
   - 如果 SQL 执行完成: 从活跃列表中移除,不再采样

**两个数据来源的处理**:

1. **定期采样(2秒)**:
   - **数据来源**: 从活跃 SQL 上下文中读取 `util.RUDetails` 的当前值
   - **处理方式**: 
     - 计算 RU 差值(增量): `ruDelta = currentRU - LastRUSample`
     - 将差值累加到对应时间桶的 `tsItem.stmtStats.TotalRU`
     - 更新活跃 SQL 上下文的 `LastRUSample = currentRU`
   - **存储位置**: TopSQL 的 `record.tsItems[timestamp].stmtStats.TotalRU`
   - **特点**: 每次采样只记录增量,避免重复计算

2. **执行完成时采集**:
   - **数据来源**: SQL 执行完成时,从 `util.RUDetails` 读取最终值
   - **处理方式**: 
     - 检查该 SQL 是否还在活跃列表中
     - **如果还在活跃列表中**: 
       - 计算最终 RU 与上次采样的差值: `ruDelta = finalRU - LastRUSample`
       - 将差值累加到时间桶(与定期采样相同)
       - 从活跃列表中移除
     - **如果不在活跃列表中**: 
       - 直接使用最终 RU 值(这种情况较少见,可能是 SQL 执行很快 < 2秒,或者活跃列表被清理)
       - 将最终 RU 值累加到时间桶
   - **存储位置**: 与定期采样相同,累加到对应时间桶的 `tsItem.stmtStats.TotalRU`
   - **作用**: 确保最终数据准确,补充定期采样可能遗漏的数据(特别是执行时间 < 2秒的 SQL)

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
        
        // 检查该 SQL 是否还在活跃列表中
        sessionID := vars.ConnectionID
        key := fmt.Sprintf("%d_%s_%s", sessionID, sqlDigest, planDigest)
        activeCtx := topsql.GetActiveSQL(key)
        
        if activeCtx != nil {
            // 如果还在活跃列表中,计算最终 RU 与上次采样的差值
            lastRUSample := activeCtx.LastRUSample.Load()
            ruDelta := finalRU - lastRUSample
            if ruDelta > 0 {
                // 累加差值到时间桶
                timestamp := uint64(time.Now().Unix())
                stats.OnRUSample(sqlDigest, planDigest, user, ruDelta, timestamp)
            }
            // 从活跃列表中移除
            topsql.UnregisterActiveSQL(key)
        } else {
            // 如果不在活跃列表中,直接使用最终 RU 值
            // (这种情况较少见,可能是 SQL 执行很快,或者活跃列表被清理)
            timestamp := uint64(time.Now().Unix())
            stats.OnRUSample(sqlDigest, planDigest, user, finalRU, timestamp)
        }
        
        // 调用扩展后的 OnExecutionFinished
        stats.OnExecutionFinished(sqlDigest, planDigest, execDuration, 
            vars.OutPacketBytes.Load(), user, finalRU)
    }
}
```

#### 3.2.1.3 上报/持久化流程(5秒)

```go
// 上报/持久化层(5秒)
func (tsr *RemoteTopSQLReporter) reportRUPeriodically() {
    ticker := time.NewTicker(5 * time.Second) // 5 秒上报间隔
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            // 将本地时间桶的 RU 数据上报给其他组件
            tsr.reportRUToExternal()
        case <-tsr.ctx.Done():
            return
        }
    }
}

func (tsr *RemoteTopSQLReporter) reportRUToExternal() {
    // 读取本地时间桶的 RU 数据
    // 批量上报给持久化组件、监控组件等
    // 类似 Prometheus 的上报机制
    // 上报的数据包括: (user, sql_digest, plan_digest, timestamp, totalRU)
    // ...
}
```

#### 3.2.1.4 用户查询刷新(15秒)

**说明**: 15 秒用户查询刷新功能在 vector-extensions 组件中处理,本文档仅在此处提及。TopSQL 提供查询接口,返回本地时间桶的 RU 数据,vector-extensions 组件负责按 15 秒频率刷新用户可见的数据。

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
        
        // 检查该 SQL 是否还在活跃列表中
        sessionID := vars.ConnectionID
        key := fmt.Sprintf("%d_%s_%s", sessionID, sqlDigest, planDigest)
        activeCtx := topsql.GetActiveSQL(key)
        
        if activeCtx != nil {
            // 如果还在活跃列表中,计算最终 RU 与上次采样的差值
            lastRUSample := activeCtx.LastRUSample.Load()
            ruDelta := finalRU - lastRUSample
            if ruDelta > 0 {
                // 累加差值到时间桶
                timestamp := uint64(time.Now().Unix())
                stats.OnRUSample(sqlDigest, planDigest, user, ruDelta, timestamp)
            }
            // 从活跃列表中移除
            topsql.UnregisterActiveSQL(key)
        } else {
            // 如果不在活跃列表中,直接使用最终 RU 值
            // (这种情况较少见,可能是 SQL 执行很快,或者活跃列表被清理)
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
- 本地定期采样(2秒)时,读取 `util.RUDetails` 的当前值(可能还在增长)
- 执行完成时,读取 `util.RUDetails` 的最终值,确保数据准确

**时间桶分配**:

- 每个采样点的 RU 数据会分配到对应的时间桶中
- 时间桶的粒度与 TopSQL 的 `PrecisionSeconds` 配置一致(默认 1 秒)
- 三层采样机制的时间桶分配:
  - **本地采集层(2秒)**: 每 2 秒采集一次,立即更新到本地时间桶
  - **上报/持久化层(5秒)**: 每 5 秒读取本地时间桶数据,批量上报
  - **用户查询刷新层(15秒)**: 由 vector-extensions 组件处理,从 TopSQL 查询接口获取本地时间桶数据
- 如果 SQL 执行时间超过 2 秒,会有多个本地采样点
- 如果 SQL 执行时间超过 5 秒,会有多个上报点
- 如果 SQL 执行时间超过 15 秒,vector-extensions 组件会看到多次刷新的数据

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

#### 3.3.1 StatementStatsItem 扩展

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

#### 3.3.2 tsItem 扩展

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

#### 3.3.3 record 扩展

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

#### 3.3.4 聚合键扩展

**StatementStatsMap 键扩展**:

```go
// 现有键
type SQLPlanDigest struct {
    SQLDigest  BinaryDigest
    PlanDigest BinaryDigest
}

// 扩展后键(用于支持按 user 维度聚合)
type UserSQLPlanDigest struct {
    User       string
    SQLDigest  BinaryDigest
    PlanDigest BinaryDigest
}

// StatementStatsMap 扩展
type StatementStatsMap map[UserSQLPlanDigest]*StatementStatsItem
```

**注意**: 为了保持向后兼容,可以考虑:
1. 保留现有的 `SQLPlanDigest` 作为内部键
2. 在查询时按 user 维度进行二次聚合
3. 或者提供两种查询模式: 按 `(sql_digest, plan_digest)` 聚合(兼容现有)和按 `(user, sql_digest, plan_digest)` 聚合(新功能)

#### 3.3.5 Protobuf 消息扩展

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

#### 3.3.6 数据保留策略

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

1. **本地定期采样** (执行中 SQL,2秒):
   - 在 TopSQL reporter 的 `collectWorker` 中扩展定期 RU 采集逻辑
   - 每 2 秒扫描一次所有活跃的 SQL 执行上下文
   - 从每个上下文中读取 `util.RUDetails` 的当前 RU 值
   - 立即更新到本地时间桶的 RU 数据
   - 需要维护活跃 SQL 执行上下文的注册表

2. **上报/持久化采样** (5秒):
   - 在 TopSQL reporter 的 `reportWorker` 中扩展上报逻辑
   - 每 5 秒读取一次本地时间桶的 RU 数据
   - 批量上报给其他组件用于持久化和管理
   - 类似 Prometheus 的上报机制,减少网络开销

3. **用户查询刷新** (15秒):
   - **说明**: 15 秒用户查询刷新功能在 vector-extensions 组件中处理
   - TopSQL 提供查询接口,返回本地时间桶的 RU 数据
   - vector-extensions 组件负责按 15 秒频率刷新用户可见的数据
   - 本文档仅在此处提及,详细实现见 vector-extensions 组件文档

4. **执行完成时采集** (补充机制):
   - 在 `pkg/executor/adapter.go` 的 `observeStmtFinishedForTopSQL()` 方法中扩展
   - 从 `context.Context` 中获取 `util.RUDetailsCtxKey` 对应的 `RUDetails`
   - 从 `SessionVars.User.Username` 获取用户名
   - 调用扩展后的 `StatementStats.OnExecutionFinished()` 方法
   - 此时 `util.RUDetails` 包含该 SQL 的最终 RU 消耗信息
   - 用于补充定期采样可能遗漏的最终数据,确保数据准确性

5. **活跃 SQL 上下文管理**:
   - 在 SQL 执行开始时,将执行上下文注册到活跃列表中
   - 在 SQL 执行完成时,从活跃列表中移除
   - 本地定期采样时,遍历活跃列表,读取每个 SQL 的当前 RU 值

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

// 活跃 SQL 执行上下文管理
type ActiveSQLContext struct {
    mu sync.RWMutex
    contexts map[string]*SQLExecContext  // key: sessionID + sqlDigest
}

type SQLExecContext struct {
    SQLDigest  []byte
    PlanDigest []byte
    User        string
    Ctx         context.Context  // 用于获取 util.RUDetails
    StartTime   time.Time
}

// 在 TopSQL reporter 中添加定期 RU 采集
func (tsr *RemoteTopSQLReporter) collectRUPeriodically() {
    ticker := time.NewTicker(15 * time.Second) // 15 秒采样间隔
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            tsr.collectActiveSQLRU()
        case <-tsr.ctx.Done():
            return
        }
    }
}

func (tsr *RemoteTopSQLReporter) collectActiveSQLRU() {
    tsr.activeSQLs.mu.RLock()
    defer tsr.activeSQLs.mu.RUnlock()
    
    now := time.Now()
    timestamp := uint64(now.Unix())
    
    for _, sqlCtx := range tsr.activeSQLs.contexts {
        // 从上下文中读取当前 RU 值
        var totalRU float64
        if ruDetailsVal := sqlCtx.Ctx.Value(util.RUDetailsCtxKey); ruDetailsVal != nil {
            ruDetails := ruDetailsVal.(*util.RUDetails)
            totalRU = ruDetails.RRU() + ruDetails.WRU()
        }
        
        // 更新对应时间桶的 RU 数据
        tsr.updateRUSample(sqlCtx.SQLDigest, sqlCtx.PlanDigest, sqlCtx.User, totalRU, timestamp)
    }
}

// 在 adapter.go 中扩展
func (a *ExecStmt) observeStmtBeginForTopSQL(ctx context.Context) context.Context {
    // ... 现有代码 ...
    
    // 注册到活跃 SQL 列表
    if stats := a.Ctx.GetStmtStats(); stats != nil && topsqlstate.TopSQLEnabled() {
        sqlDigest, planDigest := a.getSQLPlanDigest()
        user := ""
        if a.Ctx.GetSessionVars().User != nil {
            user = a.Ctx.GetSessionVars().User.Username
        }
        
        // 注册活跃 SQL
        topsql.RegisterActiveSQL(sqlDigest, planDigest, user, ctx)
    }
    
    return ctx
}

func (a *ExecStmt) observeStmtFinishedForTopSQL() {
    vars := a.Ctx.GetSessionVars()
    if vars == nil {
        return
    }
    if stats := a.Ctx.GetStmtStats(); stats != nil && topsqlstate.TopSQLEnabled() {
        sqlDigest, planDigest := a.getSQLPlanDigest()
        execDuration := vars.GetTotalCostDuration()
        
        // 获取最终的 RU 数据
        var totalRU float64
        if ruDetailsVal := a.GoCtx.Value(util.RUDetailsCtxKey); ruDetailsVal != nil {
            ruDetails := ruDetailsVal.(*util.RUDetails)
            totalRU = ruDetails.RRU() + ruDetails.WRU()
        }
        
        // 获取用户名
        user := ""
        if vars.User != nil {
            user = vars.User.Username
        }
        
        // 从活跃列表中移除
        topsql.UnregisterActiveSQL(sqlDigest, planDigest, user)
        
        // 调用扩展后的方法
        stats.OnExecutionFinished(sqlDigest, planDigest, execDuration, 
            vars.OutPacketBytes.Load(), user, totalRU)
    }
}
```

#### 3.4.2 实时性与性能权衡

**实时性保证**:

1. **三层采样频率**: 
   - **本地采集层**: 每 2 秒采集一次,确保本地数据实时性
   - **上报/持久化层**: 每 5 秒上报一次,平衡实时性和网络开销
   - **用户查询刷新层**: 由 vector-extensions 组件处理,用户查询结果延迟最多 15 秒
   - 可以通过配置参数调整各层采样频率

2. **时间桶粒度**:
   - RU 数据的时间桶粒度复用 TopSQL 的 `PrecisionSeconds` 配置(默认 1 秒)
   - 本地采集频率(2秒)高于时间桶粒度(1秒),确保数据及时更新
   - 每次本地采样时,将 RU 值累加到对应的时间桶中
   - 上报和查询刷新从本地时间桶读取数据

3. **异步处理**:
   - 本地定期 RU 采集在独立的 goroutine 中执行,不阻塞 SQL 执行
   - 上报/持久化在独立的 goroutine 中执行,不阻塞本地采集
   - 用户查询刷新由 vector-extensions 组件处理,本文档不详细描述
   - 使用 TopSQL 现有的 channel 缓冲和后台 worker 机制
   - 活跃 SQL 上下文的注册和注销操作需要加锁保护
   - 定期采样时使用读锁,注册/注销时使用写锁,减少锁竞争

**性能优化**:

1. **内存优化**:
   - RU 字段仅增加少量内存开销(每个 record 增加约 8 字节,TotalRU 字段)
   - user 字段使用 string,内存开销可控
   - 活跃 SQL 上下文列表需要额外内存,但数量有限(通常 < 1000)
   - 本地时间桶数据在内存中,上报后可以清理或压缩
   - 复用 TopSQL 现有的内存管理和清理机制

2. **CPU 优化**:
   - 本地定期 RU 采集(2秒)在独立 goroutine 中执行,不阻塞 SQL 执行
   - 上报/持久化(5秒)在独立 goroutine 中执行,不阻塞本地采集
   - 用户查询刷新由 vector-extensions 组件处理,不阻塞 SQL 执行
   - 活跃 SQL 列表使用读写锁,读操作(定期采样)并行,写操作(注册/注销)串行
   - 2 秒本地采样间隔平衡了实时性和 CPU 开销
   - 执行完成时的 RU 采集与 CPU 时间采集在同一个调用路径中

3. **网络优化**:
   - 上报/持久化采用批量上报机制(5秒),减少网络请求次数
   - 类似 Prometheus 的上报机制,批量打包数据,提高效率
   - 上报失败不影响本地数据采集和用户查询

4. **查询优化**:
   - 复用 TopSQL 现有的 Top N 计算机制
   - 支持按 RU 排序时,使用相同的排序算法
   - 可以同时支持按 CPU 时间排序和按 RU 排序
   - 用户查询刷新由 vector-extensions 组件处理,确保数据相对实时

**性能目标**:
- 本地采集开销: 每 2 秒扫描一次活跃 SQL,开销 < 1ms
- 上报开销: 每 5 秒批量上报一次,开销 < 5ms
- 查询刷新开销: 由 vector-extensions 组件处理,本文档不评估其性能
- 内存开销: 每个 record 增加约 8 字节 + user 字符串长度 + 活跃 SQL 上下文
- 查询延迟: 与 TopSQL 现有查询延迟相当,数据延迟最多 15 秒

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
   - RU 数据定期采样每 15 秒执行一次,在独立 goroutine 中,不阻塞 SQL 执行
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

1. **采样频率限制**: 
   - 用户查询可见的 RU 数据每 15 秒刷新一次,可能存在最多 15 秒的延迟
   - 本地采集每 2 秒一次,但用户查询刷新为 15 秒,因此用户看到的数据可能有延迟
   - 对于执行时间 < 2 秒的 SQL,可能只有执行完成时的一次采样
   - 对于执行时间 < 15 秒的 SQL,用户查询可能看不到中间采样点

2. **时间窗口限制**: 复用 TopSQL 现有的查询窗口限制机制。

3. **数据保留限制**: 复用 TopSQL 现有的数据保留策略。

4. **精度限制**: 
   - RU 值的精度为 `float64`,精度足够
   - 用户查询刷新频率为 15 秒,可能存在最多 15 秒的延迟
   - 本地采集频率为 2 秒,但用户查询刷新为 15 秒
   - 时间桶粒度复用 TopSQL 的 `PrecisionSeconds` 配置(默认 1 秒)

5. **Top N 限制**: 复用 TopSQL 现有的 Top N 限制(默认 100)。

6. **内存限制**: RU 扩展仅增加少量内存开销,主要受 TopSQL 现有内存管理机制限制。

7. **用户维度限制**: 当前支持按用户名聚合,不支持按 Resource Group 聚合(后续可扩展)。

8. **跨节点限制**: 复用 TopSQL 现有的跨节点限制,数据仅在当前 TiDB 节点收集。

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
   - 测试本地定期采样(2 秒)是否正确采集执行中 SQL 的 RU 数据
   - 测试上报/持久化采样(5 秒)是否正确上报数据
   - 测试 TopSQL 查询接口是否正确返回 RU 数据(用户查询刷新由 vector-extensions 组件处理)
   - 测试 SQL 执行完成时 RU 数据是否正确采集(补充机制)
   - 测试 RU 数据是否正确写入 `StatementStatsItem`
   - 测试 user 字段是否正确采集
   - 测试长时间执行的 SQL 是否有多个本地采样点(2秒间隔)
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
   - 测试执行时间 < 2 秒的 SQL 是否只有执行完成时的一次采样
   - 测试执行时间 > 2 秒的 SQL 是否有多个本地采样点
   - 测试执行时间 < 15 秒的 SQL 在用户查询中是否能看到数据
   - 测试执行时间 > 15 秒的 SQL 在 vector-extensions 组件中是否有多次刷新(由该组件处理)

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
   - 测试每 2 秒扫描活跃 SQL 列表的开销
   - 目标: 扫描 1000 个活跃 SQL 的开销 < 1ms

2. **上报/持久化性能**:
   - 测试每 5 秒批量上报 RU 数据的开销
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
   - **风险**: RU 数据采集可能带来额外的性能开销
   - **缓解措施**: 
     - RU 数据采集开销很小(从 context 读取已有对象)
     - 复用 TopSQL 现有的异步采集机制
     - 性能开销 < 0.5% CPU

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
