# TiDB TopSQL 源码阅读：低开销的 SQL 资源监控实现

## 1. 背景与问题定义

### 1.1 为什么需要 TopSQL

在持续运行的 TiDB 生产环境中，数据库管理员和开发人员经常面临这样的问题：集群 CPU 使用率飙升，但不知道是哪些 SQL 导致的；或者发现某个业务出现性能瓶颈，却无法快速定位到具体的 SQL 语句。传统的慢日志虽然能记录执行完成的慢查询，但存在明显的局限性：

- **滞后性**：慢日志只能记录已完成的查询，无法反映正在执行中的 SQL 资源消耗
- **阈值依赖**：需要预设慢查询阈值（如 `long_query_time`），阈值设置不当可能遗漏重要信息
- **采样盲区**：执行时间短但频繁执行的 SQL，其累计资源消耗可能很大，但单次执行不会触发慢日志

TopSQL 正是为了解决这些问题而设计的。它以低侵入、低开销的方式，实时产出「最消耗资源的 SQL/Plan」及其关键指标，帮助运维和开发人员快速定位资源热点。

### 1.2 核心设计目标

TopSQL 的核心设计目标可以概括为三个关键词：**低开销、高价值、可聚合**。

- **低开销**：不能对正常 SQL 执行路径造成明显的性能影响。这意味着所有重计算（profile 解析、TopN 选择、编码/上报）都必须在后台异步完成；热路径上只允许**常数级**操作，并且要避免与 SQL 数量/plan 数量线性相关的额外开销（同时允许必要时“有损丢弃”，以保证主流程不被阻塞）。
- **高价值**：采集的数据必须能够真实反映 SQL 的资源消耗，帮助用户快速定位问题。这要求准确归因 CPU 时间到具体的 SQL 和 Plan。
- **可聚合**：数据必须能够跨实例、跨时间窗口聚合，支持全局视角的资源分析。这要求数据模型支持多维度聚合和压缩。

**这一部分对排障/运维的实际意义是什么**：理解 TopSQL 的设计目标，有助于我们在使用和排障时建立正确的预期。例如，TopSQL 基于采样而非精确计时，因此对于执行时间极短的 SQL 可能无法采集到数据，这是设计权衡而非缺陷。运维人员在排查问题时，应该将 TopSQL 与慢日志、Statement Summary 结合使用，而不是完全依赖单一数据源。

## 2. 整体架构概览

### 2.1 TopSQL 在 TiDB 生态系统中的角色

TopSQL 的“数据采集”主要发生在 TiDB（以及部分指标在 TiKV 侧产生），而“展示/汇总/查询”通常在接收端（TopSQL Agent / Dashboard）完成。本文主要聚焦 **TiDB 侧的采集、聚合与上报实现**。从数据流向来看，TopSQL 的架构可以分为三层：

1. **执行路径埋点层**：在 SQL 解析、优化、执行的关键路径上设置埋点，通过 goroutine labels 标记 SQL 和 Plan 信息
2. **采集与聚合层**：后台异步采集 CPU profile 和语句统计，进行 TopN 筛选和聚合
3. **上报与展示层**：将处理后的数据通过 gRPC 上报给 TopSQL Agent 或推送给 Dashboard 订阅者

### 2.2 数据流向概览

TopSQL 的数据采集采用两条并行链路：

**CPU 采样链路**：
```
runtime/pprof CPU Profile 
  -> cpuprofile.parallelCPUProfiler（每秒滚动采样）
  -> SQLCPUCollector（解析 goroutine labels，按 SQL/Plan 聚合）
  -> RemoteTopSQLReporter（TopN 筛选，时间序列聚合）
```

**语句统计链路**：
```
SQL 执行（OnExecutionBegin/Finished）
  -> StatementStats（session 内计数）
  -> Aggregator（每秒聚合所有 session）
  -> RemoteTopSQLReporter（与 CPU 数据汇合）
```

两条链路在 `RemoteTopSQLReporter` 汇合后，按照 `(sqlDigest, planDigest)` 维度聚合成时间序列数据，最终通过 DataSink 上报或推送。

### 2.3 与其他可观测性工具的区别

为了更好地理解 TopSQL 的定位，我们将其与几个相关工具对比：

| 工具 | 采集方式 | 数据时效性 | 资源维度 | 适用场景 |
|------|---------|-----------|---------|---------|
| **TopSQL** | 采样（CPU profile）+ 计数 | 近实时（秒级） | CPU、执行次数、耗时、网络 I/O | 定位资源热点 SQL |
| **Slow Query** | 精确计时（完成时） | 滞后（查询完成后） | 执行时间 | 分析慢查询详情 |
| **Statement Summary** | 精确计数（内存表） | 近实时，但持久化周期长（30分钟） | 统计信息 | 长期趋势分析 |
| **CPU Profiling** | 采样（pprof） | 实时 | CPU 调用栈 | 深度性能分析 |

TopSQL 的核心优势在于**结合了采样和计数的优点**：通过 CPU profile 采样获得资源消耗的近似值，通过计数获得精确的执行统计，两者结合能够快速定位资源消耗最高的 SQL。

**这一部分对排障/运维的实际意义是什么**：理解 TopSQL 在整个可观测性体系中的位置，有助于我们在实际排障时选择合适的工具。例如，当需要快速定位当前资源热点时，应该查看 TopSQL；当需要分析某个慢查询的详细执行计划时，应该查看 Slow Query；当需要了解 SQL 的长期执行趋势时，应该查看 Statement Summary。不同的工具适用于不同的场景，合理组合使用才能提高排障效率。

## 3. 核心数据模型与关键概念

### 3.1 SQL Digest 与 Plan Digest：资源归因的基石

TopSQL 的核心思想是将 CPU 时间和其他资源消耗归因到具体的 SQL 语句和执行计划上。为此，它使用了两层 digest 机制：

- **SQL Digest**：SQL 语句的「指纹」，通过 `parser.NormalizeDigest` 生成。相同的 SQL 模式（忽略字面量差异）会得到相同的 digest。例如，`SELECT * FROM t WHERE id = 1` 和 `SELECT * FROM t WHERE id = 2` 会得到相同的 SQL digest。
- **Plan Digest**：执行计划的「指纹」，由执行计划的结构决定。相同的执行计划结构会得到相同的 plan digest，即使 SQL 不同。

通过 `(sqlDigest, planDigest)` 这个二元组，TopSQL 能够精确区分不同的 SQL 执行模式，实现准确的资源归因。

在代码中，这两个 digest 通过 goroutine labels 传递：

```12:14:pkg/util/topsql/collector/cpu.go
const (
	labelSQLDigest  = "sql_digest"
	labelPlanDigest = "plan_digest"
	labelSQLUID     = "sql_global_uid"
)
```

当 SQL 解析完成后，通过 `AttachAndRegisterSQLInfo` 设置 `sql_digest` label；当执行计划生成后，通过 `AttachSQLAndPlanInfo` 同时设置 `sql_digest` 和 `plan_digest` labels。这样，CPU profile 采样时就能通过 labels 识别出每条 sample 属于哪个 SQL 和 Plan。

### 3.2 TopSQL 中「Top」的判定逻辑

TopSQL 的核心算法可以理解为“**以 CPU 为主导信号做裁剪**，再把其他指标有损地汇合进来”。在当前实现中：

- **CPU 维度**：对每秒解析得到的 `(sqlDigest, planDigest)` CPUTimeMs 记录做 TopN（默认 N=100，由 `MaxStatementCount` 控制）。
- **最终上报集合**：以 CPU TopN 为“主集合”，同时把 stmtstats（执行次数、耗时、网络等）以近似/有前提的方式汇合进去；因此从工程语义上讲，它**不保证**最终每秒严格只出现 N 条 digest（再加 others），但会通过过滤与 `others` 控制集合膨胀。

TopN 的判定基于 CPU 时间，具体流程如下：

1. 以约 1s 为周期滚动采集 CPU profile，解析出所有 `(sqlDigest, planDigest)` 的 CPU 时间
2. 对当前秒的所有 CPU 记录，使用 quickselect 算法快速找出 TopN（避免全量排序）
3. TopN 的记录进入 `collecting.records`，用于构建时间序列
4. 被淘汰的记录标记为 `evicted`，其 CPU 时间累加到 `others` 记录中，避免完全丢失长尾信号

关键代码在 `processCPUTimeData`：

```216:239:pkg/util/topsql/reporter/reporter.go
// data that is not in top N. All the evicted cpuRecords will be summary into the others.
func (tsr *RemoteTopSQLReporter) processCPUTimeData(timestamp uint64, data cpuRecords) {
	defer util.Recover("top-sql", "processCPUTimeData", nil, false)

	// Get top N cpuRecords of each round cpuRecords. Collect the top N to tsr.collecting
	// for each round. SQL meta will not be evicted, since the evicted SQL can be appeared
	// on other components (TiKV) TopN DataRecords.
	top, evicted := data.topN(int(topsqlstate.GlobalState.MaxStatementCount.Load()))
	for _, r := range top {
		tsr.collecting.getOrCreateRecord(r.SQLDigest, r.PlanDigest).appendCPUTime(timestamp, r.CPUTimeMs)
	}
	if len(evicted) == 0 {
		return
	}
	totalEvictedCPUTime := uint32(0)
	for _, e := range evicted {
		totalEvictedCPUTime += e.CPUTimeMs
		// Mark which digests are evicted under each timestamp.
		// We will determine whether the corresponding CPUTime has been evicted
		// when collecting stmtstats. If so, then we can ignore it directly.
		tsr.collecting.markAsEvicted(timestamp, e.SQLDigest, e.PlanDigest)
	}
	tsr.collecting.appendOthersCPUTime(timestamp, totalEvictedCPUTime)
}
```

`topN` 方法使用 quickselect 算法实现，时间复杂度为 O(n)，避免了全量排序的 O(n log n) 开销：

```598:604:pkg/util/topsql/reporter/datamodel.go
// topN returns the largest n cpuRecords (by CPUTimeMs), other cpuRecords are returned as evicted.
func (rs cpuRecords) topN(n int) (top, evicted cpuRecords) {
	if len(rs) <= n {
		return rs, nil
	}
	if err := quickselect.QuickSelect(rs, n); err != nil {
		return rs, nil
	}
	return rs[:n], rs[n:]
}
```

### 3.3 TagInfos 与 Labels：CPU 统计的归因机制

TopSQL 的 CPU 统计依赖于 Go 的 goroutine labels 机制。在 SQL 执行过程中，关键位置会通过 `pprof.SetGoroutineLabels` 设置 labels，CPU profile 采样时会自动携带这些 labels。

除了 `sql_digest` 和 `plan_digest`，TopSQL 还使用 `sql_global_uid` label 来标记进程维度的 SQL 执行。这个 label 的格式是 `connID_sqlID`，用于在同一个连接内区分不同的 SQL 请求（因为连接可能被多个 SQL 复用）。

`parseCPUProfileBySQLLabels` 方法负责解析 profile 中的 labels，按 SQL 和 Plan 聚合 CPU 时间：

```175:207:pkg/util/topsql/collector/cpu.go
// parseCPUProfileBySQLLabels uses to aggregate the cpu-profile sample data by sql_digest and plan_digest labels,
// output the TopSQLCPUTimeRecord slice. Want to know more information about profile labels, see https://rakyll.org/profiler-labels/
// The sql_digest label is been set by `SetSQLLabels` function after parse the SQL.
// The plan_digest label is been set by `SetSQLAndPlanLabels` function after build the SQL plan.
// Since `SQLCPUCollector` only care about the cpu time that consume by (sql_digest,plan_digest), the other sample data
// without those label will be ignore.
func (sp *SQLCPUCollector) parseCPUProfileBySQLLabels(p *profile.Profile) []SQLCPUTimeRecord {
	sqlMap := make(map[string]*sqlStats)
	idx := len(p.SampleType) - 1
	for _, s := range p.Sample {
		digests, ok := s.Label[labelSQLDigest]
		if !ok || len(digests) == 0 {
			continue
		}
		for _, digest := range digests {
			stmt, ok := sqlMap[digest]
			if !ok {
				stmt = &sqlStats{
					plans: make(map[string]int64),
					total: 0,
				}
				sqlMap[digest] = stmt
			}
			stmt.total += s.Value[idx]

			plans := s.Label[labelPlanDigest]
			for _, plan := range plans {
				stmt.plans[plan] += s.Value[idx]
			}
		}
	}
	return sp.createSQLStats(sqlMap)
}
```

这里有一个重要的细节：由于 plan digest 只有在执行计划生成后才会设置，而 CPU profile 是持续采样的，所以可能存在「只有 `sql_digest` 没有 `plan_digest`」的 sample。这部分 CPU 时间通常对应优化器生成执行计划的时间。`tune()` 方法会将这些时间归入 `planDigest == ""` 的记录中：

```242:283:pkg/util/topsql/collector/cpu.go
// tune use to adjust sql stats. Consider following situation:
// The `sqlStats` maybe:
//
//	plans: {
//	    "table_scan": 200ms, // The cpu time of the sql that plan with `table_scan` is 200ms.
//	    "index_scan": 300ms, // The cpu time of the sql that plan with `index_scan` is 300ms.
//	  },
//	total:      600ms,       // The total cpu time of the sql is 600ms.
//
// total_time - table_scan_time - index_scan_time = 100ms, and this 100ms means those sample data only contain the
// sql_digest label, doesn't contain the plan_digest label. This is cause by the `pprof profile` is base on sample,
// and the plan digest can only be set after optimizer generated execution plan. So the remain 100ms means the plan
// optimizer takes time to generated plan.
// After this tune function, the `sqlStats` become to:
//
//	plans: {
//	    ""          : 100ms,  // 600 - 200 - 300 = 100ms, indicate the optimizer generated plan time cost.
//	    "table_scan": 200ms,
//	    "index_scan": 300ms,
//	  },
//	total:      600ms,
func (s *sqlStats) tune() {
	if len(s.plans) == 0 {
		s.plans[""] = s.total
		return
	}
	if len(s.plans) == 1 {
		for k := range s.plans {
			s.plans[k] = s.total
			return
		}
	}
	planTotal := int64(0)
	for _, v := range s.plans {
		planTotal += v
	}
	optimize := s.total - planTotal
	if optimize <= 0 {
		return
	}
	s.plans[""] += optimize
}
```

这是一个典型的设计取舍：通过算法补偿采样带来的信息缺失，而不是要求所有 sample 都必须有完整的 labels。

**这一部分对排障/运维的实际意义是什么**：理解 TopSQL 的数据模型，有助于我们正确解读数据。例如，当看到 `planDigest` 为空的记录时，应该知道这代表优化器阶段的 CPU 消耗，而不是数据异常。当看到 `others` 记录的 CPU 时间较高时，说明存在大量未被 TopN 捕获的长尾 SQL，可能需要调大 `MaxStatementCount` 或深入分析这些 SQL。

## 4. 关键源码流程解析

### 4.1 SQL 执行过程中的 TopSQL 埋点

TopSQL 在 SQL 执行的关键路径上设置了三个埋点：

**埋点 1：连接 dispatch 时（进程维度标记）**

在 `server/conn.go` 的请求 dispatch 处，TopSQL 会分配一个自增的 `sqlID`，并通过 `AttachAndRegisterProcessInfo` 设置 `sql_global_uid` label：

```151:156:pkg/util/topsql/topsql.go
// AttachAndRegisterProcessInfo attach the ProcessInfo into Goroutine labels.
func AttachAndRegisterProcessInfo(ctx context.Context, connID uint64, sqlID uint64) context.Context {
	ctx = collector.CtxWithProcessInfo(ctx, connID, sqlID)
	pprof.SetGoroutineLabels(ctx)
	return ctx
}
```

这个 label 用于将 CPU profile 归因到具体的连接和 SQL 请求，支持进程维度的 CPU 统计（如 `SHOW PROCESSLIST` 中显示的 CPU 时间）。

**埋点 2：SQL 解析完成后（SQL digest 标记）**

在 `session.executeStmtImpl` 或 `session.ParseWithParams` 中，解析完成后会调用 `AttachAndRegisterSQLInfo`：

```100:125:pkg/util/topsql/topsql.go
// AttachAndRegisterSQLInfo attach the sql information into Top SQL and register the SQL meta information.
func AttachAndRegisterSQLInfo(ctx context.Context, normalizedSQL string, sqlDigest *parser.Digest, isInternal bool) context.Context {
	if sqlDigest == nil || len(sqlDigest.String()) == 0 {
		return ctx
	}
	sqlDigestBytes := sqlDigest.Bytes()
	ctx = collector.CtxWithSQLDigest(ctx, sqlDigest.String())
	pprof.SetGoroutineLabels(ctx)

	linkSQLTextWithDigest(sqlDigestBytes, normalizedSQL, isInternal)

	failpoint.Inject("mockHighLoadForEachSQL", func(val failpoint.Value) {
		// In integration test, some SQL run very fast that Top SQL pprof profile unable to sample data of those SQL,
		// So need mock some high cpu load to make sure pprof profile successfully samples the data of those SQL.
		// Attention: Top SQL pprof profile unable to sample data of those SQL which run very fast, this behavior is expected.
		// The integration test was just want to make sure each type of SQL will be set goroutine labels and and can be collected.
		if val.(bool) {
			sqlPrefixes := []string{"insert", "update", "delete", "load", "replace", "select", "begin",
				"commit", "analyze", "explain", "trace", "create", "set global"}
			if MockHighCPULoad(normalizedSQL, sqlPrefixes, 1) {
				logutil.BgLogger().Info("attach SQL info", zap.String("sql", normalizedSQL))
			}
		}
	})
	return ctx
}
```

这里做了两件事：
1. 通过 `pprof.SetGoroutineLabels` 设置 `sql_digest` label
2. 通过 `linkSQLTextWithDigest` 将 SQL 文本注册到 `normalizedSQLMap`（用于后续展示）

**埋点 3：执行计划生成后（Plan digest 标记 + 语句统计开始）**

在 `executor.(*ExecStmt).observeStmtBeginForTopSQL` 中，执行计划生成后会调用 `AttachSQLAndPlanInfo` 和 `OnExecutionBegin`：

```127:149:pkg/util/topsql/topsql.go
// AttachSQLAndPlanInfo attach the sql and plan information into Top SQL
func AttachSQLAndPlanInfo(ctx context.Context, sqlDigest *parser.Digest, planDigest *parser.Digest) context.Context {
	if sqlDigest == nil || len(sqlDigest.String()) == 0 {
		return ctx
	}
	var planDigestStr string
	sqlDigestStr := sqlDigest.String()
	if planDigest != nil {
		planDigestStr = planDigest.String()
	}
	ctx = collector.CtxWithSQLAndPlanDigest(ctx, sqlDigestStr, planDigestStr)
	pprof.SetGoroutineLabels(ctx)

	failpoint.Inject("mockHighLoadForEachPlan", func(val failpoint.Value) {
		// Work like mockHighLoadForEachSQL failpoint.
		if val.(bool) {
			if MockHighCPULoad("", []string{""}, 1) {
				logutil.BgLogger().Info("attach SQL info")
			}
		}
	})
	return ctx
}
```

SQL 执行完成后，**只有在 TopSQL enabled 时**才会调用 `OnExecutionFinished` 记录执行统计（执行次数、耗时、网络 I/O 等）。这意味着 stmtstats 的 begin/finish **不保证严格成对**（例如 TopSQL 在 SQL 执行过程中被打开/关闭）。

### 4.2 CPU 采样链路：从 runtime/pprof 到 SQL 归因

CPU 采样链路的核心是 `cpuprofile.parallelCPUProfiler`：它以约 1s 周期滚动执行 CPU profiling，并将结果广播给所有注册的 consumer（TopSQL 是其中之一）。需要注意这里的“每秒”是**近似 bucket**：代码里对记录打点的时间戳来自 `time.Now().Unix()`（接收时刻），而 profile 数据覆盖的是上一段 profiling 时间窗口，因此 timestamp 与实际采样区间并不要求严格对齐。

#### 4.2.1 为什么必须用 `cpuprofile` 统一协调：与 `/debug/pprof/profile` 共存

工程上一个非常关键的约束是：同一进程内同时只能有一个 `pprof.StartCPUProfile` 在跑。TiDB 通过 `pkg/util/cpuprofile` 把“TopSQL 采样”和“用户通过 HTTP 拉取 `/debug/pprof/profile`”统一到一个全局 profiler 上：

- **只有在存在 consumer 时才启动 profiling**：TopSQL 是否启用决定了 `SQLCPUCollector` 是否向 `cpuprofile` 注册 consumer；而 pprof HTTP handler 也会在请求期间注册自己的 consumer 来收集 profile。
- **避免污染用户 profile**：为了不把 `sql_digest/plan_digest/sql_global_uid` 这些 TopSQL labels 暴露给普通用户，pprof HTTP 的输出会清理 labels（只保留 `sql` 这一类通用标签）。

理解这一点有助于解释两个常见现象：TopSQL disabled 时 CPU profiling 不一定完全停止（可能有用户在拉 pprof）；以及 pprof 输出里看不到 TopSQL 的 digest labels 是“刻意的工程设计”而非数据丢失。

`SQLCPUCollector` 作为 consumer，接收到 profile 数据后会：

1. 解析 profile 数据（`profile.ParseData`）
2. 遍历所有 sample，按 `sql_digest` 和 `plan_digest` labels 聚合 CPU 时间（`parseCPUProfileBySQLLabels`）
3. 调用 `tune()` 处理只有 SQL digest 没有 Plan digest 的情况
4. 将结果发送到 `collectCPUTimeChan`，由 `RemoteTopSQLReporter` 的 `collectWorker` 处理

关键代码在 `handleProfileData`：

```144:157:pkg/util/topsql/collector/cpu.go
func (sp *SQLCPUCollector) handleProfileData(data *cpuprofile.ProfileData) {
	if data.Error != nil {
		return
	}

	p, err := profile.ParseData(data.Data.Bytes())
	if err != nil {
		logutil.BgLogger().Error("parse profile error", zap.Error(err))
		return
	}
	stats := sp.parseCPUProfileBySQLLabels(p)
	sp.collector.Collect(stats)
	sp.parseCPUProfileForProcess(p)
}
```

这里有一个重要的性能考虑：profile 解析和 sample 遍历是 CPU 密集型操作，必须在后台 goroutine 中进行，不能阻塞主流程。

### 4.3 语句统计链路：从 session 到全局聚合

语句统计采用「本地计数 + 全局聚合」的两级架构：

**本地计数（StatementStats）**

每个 session 有一个 `StatementStats` 实例，在 SQL 执行时更新计数：

```62:88:pkg/util/topsql/stmtstats/stmtstats.go
// OnExecutionBegin implements StatementObserver.OnExecutionBegin.
func (s *StatementStats) OnExecutionBegin(sqlDigest, planDigest []byte, inNetworkBytes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.GetOrCreateStatementStatsItem(sqlDigest, planDigest)

	item.ExecCount++
	item.NetworkInBytes = inNetworkBytes
	// Count more data here.
}

// OnExecutionFinished implements StatementObserver.OnExecutionFinished.
func (s *StatementStats) OnExecutionFinished(sqlDigest, planDigest []byte, execDuration time.Duration, outNetworkBytes uint64) {
	ns := execDuration.Nanoseconds()
	if ns < 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.GetOrCreateStatementStatsItem(sqlDigest, planDigest)

	item.SumDurationNs += uint64(ns)
	item.DurationCount++
	item.NetworkOutBytes = outNetworkBytes
	// Count more data here.
}
```

这是一个「线程安全但轻量」的操作：只更新 map 中的计数，不做任何重计算。

**全局聚合（Aggregator）**

`stmtstats.aggregator` 是一个后台 goroutine，每秒执行一次：

1. 遍历所有 session 的 `StatementStats`
2. 调用 `Take()` 取走当前累计数据并清空
3. 将所有 session 的数据 `Merge` 成全局统计
4. 如果 TopSQL 启用，将结果发送到 `collectStmtStatsChan`

这样设计的好处是：session 内的计数操作非常轻量（只是 map 操作），而聚合操作在后台异步完成，不会影响 SQL 执行性能。需要补充的工程前提是：

- TopSQL disabled 时，`stmtstats` 的后台聚合仍会 tick 并做 `Take()+Merge()`，但会在“是否 enabled”的门禁处直接丢弃结果（不进入 reporter）。
- 对于 fast plan，当前实现为了降低开销，TopSQL disabled 时可能直接跳过 begin 埋点；因此“disabled 仍可计数”只在部分路径成立。

### 4.4 Reporter 的汇合与裁剪：TopSQL 的「大脑」

`RemoteTopSQLReporter` 是 TopSQL 的核心，它接收两条链路的数据，进行 TopN 筛选和时间序列聚合。

**collectWorker：数据接收与 TopN 筛选**

`collectWorker` 有两个输入 channel：
- `collectCPUTimeChan`：接收 CPU 记录
- `collectStmtStatsChan`：接收语句统计

对于 CPU 记录，`collectWorker` 调用 `processCPUTimeData` 进行 TopN 筛选（如前所述）。

对于语句统计，由于 TopN 是基于 CPU 时间判定的，而语句统计可能包含 CPU TopN 以外的 SQL，所以 `processStmtStatsData` 会：

1. 将语句统计缓存到 `stmtStatsBuffer`（按 timestamp 索引）
2. 在 report tick 时统一处理
3. 使用「network bytes 的 kth 值」做近似过滤，并结合 `evicted` 标记避免把被 CPU TopN 淘汰的记录再塞回来
4. 被淘汰的语句统计聚合到 `others` 记录

**时间序列聚合**

每个 `(sqlDigest, planDigest)` 对应一个 `record`，`record` 包含一个 `tsItems` 数组（时间序列）。每次收到新的 CPU 记录，会调用 `appendCPUTime` 追加到对应 `record` 的 `tsItems` 中：

```69:74:pkg/util/topsql/reporter/datamodel.go
// tsItem is a self-contained complete piece of data for a certain timestamp.
type tsItem struct {
	stmtStats stmtstats.StatementStatsItem
	timestamp uint64
	cpuTimeMs uint32
}
```

**planDigest 为空的合并优化**

由于优化器阶段的 CPU 时间可能被归入 `planDigest == ""`，而执行阶段的 CPU 时间有具体的 plan digest，同一个 SQL 可能同时存在两条记录。`removeInvalidPlanRecord` 方法会检测这种情况，并将空 plan 的记录合并到非空 plan 的记录中：

```520:551:pkg/util/topsql/reporter/datamodel.go
// Basically, it should be called once at the end of the collection, currently in `getReportRecords`.
func (c *collecting) removeInvalidPlanRecord() {
	sql2PlansMap := make(map[string][][]byte, len(c.records)) // sql_digest => []plan_digest
	for _, v := range c.records {
		k := string(v.sqlDigest)
		sql2PlansMap[k] = append(sql2PlansMap[k], v.planDigest)
	}
	for k, plans := range sql2PlansMap {
		if len(plans) != 2 {
			continue
		}
		if len(plans[0]) > 0 && len(plans[1]) > 0 {
			continue
		}

		sqlDigest := []byte(k)
		key0 := encodeKey(c.keyBuf, sqlDigest, plans[0])
		key1 := encodeKey(c.keyBuf, sqlDigest, plans[1])
		record0, ok0 := c.records[key0]
		record1, ok1 := c.records[key1]
		if !ok0 || !ok1 {
			continue
		}
		if len(plans[0]) != 0 {
			record0.merge(record1)
			delete(c.records, key1)
		} else {
			record1.merge(record0)
			delete(c.records, key0)
		}
	}
}
```

这是一个典型的工程优化：通过后处理消除数据中的「割裂」现象，提升用户体验。

**reportWorker：数据上报**

`reportWorker` 每隔 `ReportIntervalSeconds`（默认 60 秒）执行一次：

1. 调用 `takeDataAndSendToReportChan` 将当前 `collecting` 的数据整体取走（原子操作，避免与并发写入冲突）
2. Sleep 100ms（减少与并发 `RegisterSQL/RegisterPlan` 的竞态）
3. 组装 protobuf 消息（`ReportData`），包含 `DataRecords`（时间序列）、`SQLMetas`（SQL 文本）、`PlanMetas`（Plan 文本）
4. 将消息发送到所有注册的 DataSink

对于 Plan meta，如果 Plan 过大（超过 `MaxBinaryPlanSize`），会走压缩编码路径，避免 decode 开销。

**这一部分对排障/运维的实际意义是什么**：理解 TopSQL 的执行流程，有助于我们在排障时判断数据是否正常。例如，如果某个 SQL 在 TopSQL 中看不到，可能是因为执行时间太短（CPU profile 采样不到）、或者被 TopN 淘汰（需要查看 `others` 记录）。如果看到同一条 SQL 有多条记录（不同的 plan digest），说明这条 SQL 有多种执行计划，可能存在 plan cache 失效或统计信息过期的问题。

## 5. 性能与稳定性设计

### 5.1 热路径优化：接近 O(1) 的埋点操作

TopSQL 的所有埋点操作都经过精心优化，确保对 SQL 执行路径的性能影响最小（但需要避免把“低开销”理解为“无开销/绝对 O(1)”）：

- **Attach* 操作**：主要是 `pprof.SetGoroutineLabels` 以及少量状态写入，属于常数级开销，但仍应视为热路径成本的一部分
- **RegisterSQL/Plan**：写入 `normalizedSQLMap/normalizedPlanMap`（容量受 `MaxCollect` 限制，超过会丢弃并打 metrics），设计目标是“快返回、不做重计算”
- **OnExecutionBegin/Finished**：更新 session 内的计数 map，持锁时间短；同时要注意 `OnExecutionFinished` 依赖 `TopSQLEnabled()`，并非总会被调用

所有可能耗时的操作（profile 解析、TopN 筛选、数据上报）都在后台 goroutine 中进行，不会阻塞 SQL 执行。

### 5.2 背压处理：Channel 满时丢弃而非阻塞

TopSQL 使用 channel 作为异步通信机制，所有 channel 的 buffer 都很小（通常为 2）。当 channel 满时，数据会被直接丢弃并记录 metrics，而不是阻塞发送方：

```123:137:pkg/util/topsql/reporter/reporter.go
// Collect implements tracecpu.Collector.
//
// WARN: It will drop the DataRecords if the processing is not in time.
// This function is thread-safe and efficient.
func (tsr *RemoteTopSQLReporter) Collect(data []collector.SQLCPUTimeRecord) {
	if len(data) == 0 {
		return
	}
	select {
	case tsr.collectCPUTimeChan <- data:
	default:
		// ignore if chan blocked
		reporter_metrics.IgnoreCollectChannelFullCounter.Inc()
	}
}
```

这是一个典型的设计取舍：**宁可丢失数据，也不阻塞主流程**。在高负载场景下，如果数据处理跟不上，TopSQL 会自动降级（丢弃数据），而不是拖慢整个系统。运维人员可以通过监控 `IgnoreCollectChannelFullCounter` 等 metrics 来发现这种情况。

### 5.3 开关机制：是否存在 DataSink 驱动（DataSink 来源于配置/订阅）

TopSQL 的启用/禁用在实现上由“DataSink 是否注册”驱动，但 DataSink 的产生来源于配置/订阅行为：

- 当有 DataSink 注册时（例如 `ReceiverAddress` 非空的 `SingleTargetDataSink`，或有 Dashboard 订阅创建的 pubsub sink），TopSQL 自动启用
- 当所有 DataSink 都注销时，TopSQL 自动禁用

这样设计的好处是：**没有消费者时，TopSQL 自动关闭，避免无效开销**。具体实现通过 `DefaultDataSinkRegisterer` 在注册/注销时调用 `EnableTopSQL/DisableTopSQL`：

当 TopSQL 禁用时（且没有其他 consumer，例如用户 pprof 请求）：
- `SQLCPUCollector` 不会向 `cpuprofile` 注册 consumer（避免 profile 解析开销）
- `stmtstats.aggregator` 不会上报数据（session 内 begin 计数在部分路径仍会发生，但 finish 计数与 fast plan 分支等都会影响完整性）

### 5.4 内存控制：Meta Map 容量限制

`normalizedSQLMap` 和 `normalizedPlanMap` 都有容量限制（`MaxCollect`，默认 5000）。当 map 容量超过限制时，新的 SQL/Plan 会被拒绝注册，并记录 metrics。这是一个简单的「容量保护」机制，防止 map 无限增长导致内存泄漏。

对于 Plan，如果大小超过 `MaxBinaryPlanSize`（2KB），会被标记为 `isLarge`，上报时走压缩编码路径，避免 decode 开销。

### 5.5 高负载下的行为

在高负载或异常场景下，TopSQL 的行为是「优雅降级」：

1. **CPU profile 解析慢**：由于 profile 解析在后台 goroutine 中进行，不会影响 SQL 执行，但可能导致数据延迟
2. **Channel 满**：数据被丢弃，metrics 增加，但不阻塞主流程
3. **Meta Map 满**：新的 SQL/Plan 无法注册，已注册的不受影响
4. **订阅者发送慢**：使用 deadline 和超时机制，避免 stream.Send 卡死

所有异常情况都会记录 metrics，便于运维人员监控和告警。

**这一部分对排障/运维的实际意义是什么**：理解 TopSQL 的性能设计，有助于我们在高负载场景下建立正确的预期。例如，如果发现 `IgnoreCollectChannelFullCounter` 持续增长，说明系统负载过高，TopSQL 正在降级。此时应该关注系统资源（CPU、内存）使用情况，而不是单纯调大 channel buffer（这可能导致内存压力）。如果发现某个 SQL 的 Plan meta 缺失，可能是因为 `normalizedPlanMap` 已满，需要检查 `MaxCollect` 配置或系统负载。

## 6. 设计取舍与不足

### 6.1 采样 vs 精确计时

TopSQL 的 CPU 统计基于 `runtime/pprof` 的采样机制，而非精确计时。这意味着：

**优点**：
- 开销较低：CPU profile 属于采样统计，采样频率由 Go runtime 与实现细节决定（通常为默认值，但**不应作为设计保证**），因此 TopSQL 的 TiDB CPU 时间天然存在统计误差
- 能够捕获「执行中的 SQL」：不需要等待 SQL 完成

**缺点**：
- 统计误差：采样频率决定了统计精度，对于执行时间极短的 SQL（如 < 10ms），可能一个 sample 都采不到
- 短 SQL 不可见：执行时间短的 SQL，即使执行频率很高，累计资源消耗很大，也可能无法被 TopSQL 捕获

这是 TopSQL 与其他可观测性工具（如 Slow Query）的根本区别：TopSQL 追求「低开销、近实时」，而 Slow Query 追求「精确、完整」。

### 6.2 TopN 裁剪 vs 全量记录

在当前实现里，更精确的说法是：**CPU 维度按秒裁剪 TopN**，并把 CPU TopN 之外的部分聚合到 `others`；随后在汇合 stmtstats 时会用近似过滤把部分 digest 的 stmtstats 汇入（也可能被聚合进 `others`）。因此你应把 TopSQL 理解为“以 TopN 为核心信号的有损汇总”，而不是严格意义上的“只保留 N 条记录”。

**优点**：
- 内存和网络开销可控：无论有多少 SQL，TopSQL 的内存占用都有上限
- 聚焦热点：大部分场景下，TopN 已经足够定位问题

**缺点**：
- 长尾 SQL 不可见：如果某个 SQL 的 CPU 时间不是 TopN，但在某个时间窗口内突然飙升，可能无法及时发现
- `others` 聚合损失信息：`others` 只记录总的 CPU 时间，无法知道具体是哪些 SQL 贡献的

对于这个问题，TopSQL 提供了 `MaxStatementCount` 配置，可以根据实际情况调大 N 值，但需要在内存开销和可见性之间权衡。

### 6.3 时间窗口 vs 实时性

TopSQL 的上报周期默认是 60 秒，这意味着数据会有最多 60 秒的延迟。这是为了：

- 减少网络开销：频繁上报会增加网络流量和 Agent 处理压力
- 支持时间序列聚合：60 秒窗口内的多秒数据可以聚合成一条时间序列记录

如果对实时性要求更高，可以调小 `ReportIntervalSeconds`，但会增加网络开销。

### 6.4 Plan 归因的不完整性

由于 plan digest 只有在执行计划生成后才会设置，而 CPU profile 是持续采样的，优化器阶段的 CPU 时间可能无法准确归因到具体的 plan。TopSQL 通过 `tune()` 和 `removeInvalidPlanRecord()` 做了补偿，但这仍然是近似处理，可能存在误差。

### 6.5 与其他可观测性工具的对比

| 维度 | TopSQL | Slow Query | Statement Summary |
|------|--------|------------|-------------------|
| **数据时效性** | 近实时（秒级） | 滞后（查询完成后） | 近实时（内存表） |
| **资源维度** | CPU、执行次数、网络 I/O | 执行时间 | 各类统计信息 |
| **可见性** | TopN 可见，长尾不可见 | 超过阈值的可见 | 全部可见 |
| **开销** | 极低（采样） | 低（精确计时） | 中等（内存表） |
| **持久化** | 通过 Agent 持久化 | 通过文件持久化 | 定期持久化（30分钟） |

**这一部分对排障/运维的实际意义是什么**：理解 TopSQL 的设计取舍，有助于我们正确使用和解读数据。例如，当发现某个 SQL 在 TopSQL 中看不到时，不应该立即认为是 TopSQL 的 bug，而应该考虑：这个 SQL 是否执行时间太短（采样不到）？是否被 TopN 淘汰（查看 `others`）？是否应该结合 Slow Query 或 Statement Summary 一起分析？只有理解了工具的边界，才能充分发挥其价值。

## 7. 总结与实践建议

### 7.1 适用场景

TopSQL 最适合以下场景：

1. **快速定位资源热点**：当集群 CPU 使用率突然飙升时，通过 TopSQL 可以快速定位到消耗 CPU 最多的 SQL
2. **实时监控**：配合 Dashboard，可以实时查看当前最消耗资源的 SQL，及时发现问题
3. **资源治理**：通过 TopSQL 数据，可以识别出需要优化或限流的 SQL，进行资源治理

### 7.2 不适用场景

TopSQL 不适合以下场景：

1. **精确性能分析**：如果需要精确的执行时间、等待时间等指标，应该使用 Slow Query 或 EXPLAIN ANALYZE
2. **长尾 SQL 分析**：如果关注的是执行频率高但单次执行时间短的 SQL，TopSQL 可能无法捕获
3. **历史趋势分析**：如果需要分析 SQL 的长期执行趋势（如按天、周统计），应该使用 Statement Summary

### 7.3 实践建议

1. **组合使用多种工具**：TopSQL、Slow Query、Statement Summary 各有优势，应该根据具体场景选择合适的工具，或组合使用
2. **关注 metrics**：通过监控 `IgnoreCollectChannelFullCounter`、`IgnoreCollectStmtChannelFullCounter` 等 metrics，可以及时发现 TopSQL 的异常情况
3. **合理配置参数**：根据实际情况调整 `MaxStatementCount`、`ReportIntervalSeconds` 等参数，在内存开销和可见性之间找到平衡
4. **理解数据含义**：TopSQL 的数据基于采样，存在统计误差，解读数据时应该考虑这一点

### 7.4 对后续演进方向的思考

TopSQL 作为一个仍在演进的功能，未来可能在以下方向改进：

1. **更细粒度的资源维度**：除了 CPU，可能还会增加内存、I/O 等维度的统计
2. **更智能的 TopN 算法**：当前 TopN 只基于 CPU 时间，未来可能支持多维度综合排序
3. **更好的长尾可见性**：通过采样或分层统计，提升长尾 SQL 的可见性
4. **与 AI 的集成**：结合 AI 技术，自动识别异常 SQL 或给出优化建议

但无论如何演进，TopSQL 的核心设计原则——低开销、高价值、可聚合——应该保持不变。

---

## 参考阅读路线

对于希望深入理解 TopSQL 实现的读者，建议按以下顺序阅读源码：

1. **入口与生命周期**：`pkg/util/topsql/topsql.go`、`cmd/tidb-server/main.go`
2. **全局状态与配置**：`pkg/util/topsql/state/state.go`
3. **CPU 采样基础设施**：`pkg/util/cpuprofile/cpuprofile.go`
4. **CPU 归因与解析**：`pkg/util/topsql/collector/cpu.go`
5. **语句统计**：`pkg/util/topsql/stmtstats/stmtstats.go`、`aggregator.go`
6. **数据模型与聚合**：`pkg/util/topsql/reporter/datamodel.go`、`reporter.go`
7. **数据上报**：`pkg/util/topsql/reporter/single_target.go`、`pubsub.go`

建议在阅读源码时，结合本文的架构描述，从整体到细节逐步深入，理解每个模块的职责和设计动机。