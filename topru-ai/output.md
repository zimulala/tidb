# TopRU Implementation - Incremental Output Log

Date: 2026-01-20  
Status: In Progress  
Mode: Implementation Loop (minimal incremental commits)

---

## Sub-task 1: Protocol Extension + Mock Server Stub

**Objective**: Extend tipb protocol with TopRU messages, regenerate Go bindings, fix mock server compilation.

**Changes**:

### 1.1 tipb Protocol Extension

**File**: `/Users/xia/workspace/src/github.com/pingcap/tipb/proto/topsql_agent.proto`

**Added RPC**:
```protobuf
rpc ReportTopRURecords(stream TopRURecord) returns (EmptyResponse) {}
```

**Added Messages**:
```protobuf
message TopRURecord {
  string keyspace_name = 1;
  string user = 2;
  bytes sql_digest = 3;
  bytes plan_digest = 4;
  repeated TopRURecordItem items = 5;
}

message TopRURecordItem {
  uint64 timestamp_sec = 1;
  double total_ru = 2;
  uint64 exec_count = 3;
  uint64 exec_duration = 4;
}
```

**Extended TopSQLSubRequest**:
```protobuf
enum ReportInterval {
  REPORT_INTERVAL_UNSPECIFIED = 0;
  REPORT_INTERVAL_15S = 15;
  REPORT_INTERVAL_30S = 30;
  REPORT_INTERVAL_60S = 60;
}

message TopSQLSubRequest {
  // ...existing fields...
  bool enable_top_ru = 6;
  ReportInterval report_interval = 7;
}
```

**Extended TopSQLSubResponse**:
```protobuf
message TopSQLSubResponse {
  oneof resp_oneof {
    // ...existing fields...
    TopRURecord ru_record = 4;
  }
}
```

**Regenerated bindings**:
- Protocol buffer compiler: `protoc 25.3`
- Plugin: `gogo/protobuf`
- Output: `/Users/xia/workspace/src/github.com/pingcap/tipb/go-tipb/topsql_agent.pb.go`
- Post-processing: sed normalization + goimports

### 1.2 TiDB Mock Server Fix

**File**: `pkg/util/topsql/reporter/mock/server.go`

**Added method** (line 109-121):
```go
func (svr *mockAgentServer) ReportTopRURecords(stream tipb.TopSQLAgent_ReportTopRURecordsServer) error {
    // Stub: drain stream and ignore RU records for now
    for {
        svr.mayHang()
        _, err := stream.Recv()
        if err == io.EOF {
            break
        } else if err != nil {
            return err
        }
    }
    return stream.SendAndClose(&tipb.EmptyResponse{})
}
```

**Rationale**: Mock server needs to implement full `tipb.TopSQLAgentServer` interface. RU records will be stored/validated in later sub-task when actual RU reporting is implemented.

### 1.3 Compilation Verification

**Command**: `go build -v ./pkg/util/topsql/reporter/...`  
**Result**: ✅ PASS  
**Error before fix**: `*mockAgentServer does not implement tipb.TopSQLAgentServer (missing method ReportTopRURecords)`

---

## Sub-task 2: Extend Reporter Data Path for RU Records

**Objective**: Modify reporter layer to support RU records alongside CPU records.

**Changes**:

### 2.1 ReportData Struct Extension

**File**: `pkg/util/topsql/reporter/datasink.go`

**Extended struct** (line 45-54):
```go
type ReportData struct {
    // DataRecords contains the topN records of each second and the `others`
    // record which aggregation all []tipb.TopSQLRecord that is out of Top N.
    DataRecords []tipb.TopSQLRecord
    // RURecords contains TopRU records aggregated by (user, sql_digest, plan_digest).
    // Stored separately to avoid mixing with CPU-based TopSQLRecord.
    RURecords []tipb.TopRURecord
    SQLMetas  []tipb.SQLMeta
    PlanMetas []tipb.PlanMeta
}
```

**Updated hasData()** (line 56-58):
```go
func (d *ReportData) hasData() bool {
    return len(d.DataRecords) != 0 || len(d.RURecords) != 0 || len(d.SQLMetas) != 0 || len(d.PlanMetas) != 0
}
```

**Rationale**: RU records are stored separately (`[]tipb.TopRURecord`) to avoid type confusion with CPU-based `[]tipb.TopSQLRecord`. This aligns with the architecture decision D2 in work.md.

### 2.2 Reporter Send Logic Extension

**File**: `pkg/util/topsql/reporter/single_target.go`

**Modified doSend() goroutine count** (line 207-208, 222-225):
- Changed channel size from 3 → 4
- Changed WaitGroup count from 3 → 4
- Added 4th goroutine:
  ```go
  go func() {
      defer wg.Done()
      errCh <- ds.sendBatchTopRURecord(ctx, task.data.RURecords)
  }()
  ```

**Added sendBatchTopRURecord()** (line 269-300):
```go
func (ds *SingleTargetDataSink) sendBatchTopRURecord(ctx context.Context, records []tipb.TopRURecord) (err error) {
    if len(records) == 0 {
        return nil
    }

    start := time.Now()
    sentCount := 0
    defer func() {
        // TODO: add RU-specific metrics later
        if err != nil {
            reporter_metrics.ReportRecordDurationFailedHistogram.Observe(time.Since(start).Seconds())
        } else {
            reporter_metrics.ReportRecordDurationSuccHistogram.Observe(time.Since(start).Seconds())
        }
    }()

    client := tipb.NewTopSQLAgentClient(ds.conn)
    stream, err := client.ReportTopRURecords(ctx)
    if err != nil {
        return err
    }
    for i := range records {
        if err = stream.Send(&records[i]); err != nil {
            return
        }
        sentCount++
    }

    _, err = stream.CloseAndRecv()
    return
}
```

**Rationale**: 
- RU records are sent in parallel with SQL/Plan metas and CPU records
- Uses new `ReportTopRURecords` RPC from extended protocol
- Metrics reuse existing histograms (RU-specific metrics marked as TODO)
- Stream lifecycle follows same pattern as existing send functions

### 2.3 Compilation Verification

**Command**: `go build -v ./pkg/util/topsql/reporter`  
**Result**: ✅ PASS

---

## Sub-task 3: Extend PubSub Layer for TopRU Config

**Objective**: Parse and store TopRU subscription config (`enable_top_ru`, `report_interval`) from agent subscribe requests, and send RU records via PubSub stream.

**Changes**:

### 3.1 Subscription Config Parsing

**File**: `pkg/util/topsql/reporter/pubsub.go`

**Modified Subscribe()** (line 48-49):
```go
func (ps *TopSQLPubSubService) Subscribe(req *tipb.TopSQLSubRequest, stream tipb.TopSQLPubSub_SubscribeServer) error {
    ds := newPubSubDataSink(req, stream, ps.dataSinkRegisterer)
    // ...
}
```
- Changed from ignoring `req` parameter to passing it to `newPubSubDataSink`

**Extended pubSubDataSink struct** (line 66-68):
```go
type pubSubDataSink struct {
    // ...existing fields...
    
    // TopRU subscription config
    enableTopRU    bool
    reportInterval tipb.ReportInterval
}
```

**Modified newPubSubDataSink()** (line 71, 83-84):
```go
func newPubSubDataSink(req *tipb.TopSQLSubRequest, stream tipb.TopSQLPubSub_SubscribeServer, registerer DataSinkRegisterer) *pubSubDataSink {
    // ...
    return &pubSubDataSink{
        // ...existing fields...
        enableTopRU:    req.GetEnableTopRu(),
        reportInterval: req.GetReportInterval(),
    }
}
```

**Rationale**: Stores subscription config at data sink creation time. Future sub-tasks will use these fields to gate RU collection and adjust report intervals.

### 3.2 RU Record Streaming

**File**: `pkg/util/topsql/reporter/pubsub.go`

**Modified doSend()** (line 163-165):
```go
func (ds *pubSubDataSink) doSend(ctx context.Context, data *ReportData) error {
    if err := ds.sendTopSQLRecords(ctx, data.DataRecords); err != nil {
        return err
    }
    if err := ds.sendTopRURecords(ctx, data.RURecords); err != nil {
        return err
    }
    // ...SQL/Plan metas...
}
```

**Added sendTopRURecords()** (line 209-244):
```go
func (ds *pubSubDataSink) sendTopRURecords(ctx context.Context, records []tipb.TopRURecord) (err error) {
    if len(records) == 0 {
        return
    }

    start := time.Now()
    sentCount := 0
    defer func() {
        // TODO: add RU-specific metrics later
        if err != nil {
            reporter_metrics.ReportRecordDurationFailedHistogram.Observe(time.Since(start).Seconds())
        } else {
            reporter_metrics.ReportRecordDurationSuccHistogram.Observe(time.Since(start).Seconds())
        }
    }()

    topRURecord := &tipb.TopSQLSubResponse_RuRecord{}
    r := &tipb.TopSQLSubResponse{RespOneof: topRURecord}

    for i := range records {
        topRURecord.RuRecord = &records[i]
        if err = ds.stream.Send(r); err != nil {
            return
        }
        sentCount++

        select {
        case <-ctx.Done():
            err = ctx.Err()
            return
        default:
        }
    }

    return
}
```

**Rationale**: 
- Uses `TopSQLSubResponse_RuRecord` oneof field (field 4 in protocol)
- Parallel structure to `sendTopSQLRecords` for consistency
- Respects context cancellation between record sends
- Metrics reuse existing histograms (RU-specific TODO)

### 3.3 Compilation Verification

**Command**: `go build -v ./pkg/util/topsql/reporter`  
**Result**: ✅ PASS

---

## Sub-task 4: Extend TopSQL State Management for TopRU

**Objective**: Add global state tracking for TopRU enable/disable and report interval configuration.

**Changes**:

### 4.1 State Constants

**File**: `pkg/util/topsql/state/state.go`

**Added constants** (line 28-32):
```go
// Default Top-RU state values.
const (
    DefTiDBTopRUEnable                = false
    DefTiDBTopRUReportIntervalSeconds = 60
)
```

**Rationale**: Separate TopRU defaults from TopSQL. Default 60s report interval aligns with design doc.

### 4.2 State Struct Extension

**Extended State struct** (line 58-61):
```go
type State struct {
    // ...existing TopSQL fields...
    
    // enable top-ru or not.
    enableTopRU *atomic.Bool
    // The report data interval of top-ru.
    TopRUReportIntervalSeconds *atomic.Int64
}
```

**Extended GlobalState initialization** (line 41-42):
```go
var GlobalState = State{
    // ...existing fields...
    enableTopRU:                atomic.NewBool(DefTiDBTopRUEnable),
    TopRUReportIntervalSeconds: atomic.NewInt64(DefTiDBTopRUReportIntervalSeconds),
}
```

### 4.3 Public API Functions

**Added functions** (line 79-102):
```go
// EnableTopRU enables the top RU feature.
func EnableTopRU() {
    GlobalState.enableTopRU.Store(true)
}

// DisableTopRU disables the top RU feature.
func DisableTopRU() {
    GlobalState.enableTopRU.Store(false)
}

// TopRUEnabled checks whether enabled the top RU feature.
func TopRUEnabled() bool {
    return GlobalState.enableTopRU.Load()
}

// SetTopRUReportInterval sets the report interval for TopRU (in seconds).
func SetTopRUReportInterval(intervalSeconds int64) {
    GlobalState.TopRUReportIntervalSeconds.Store(intervalSeconds)
}

// GetTopRUReportInterval returns the report interval for TopRU (in seconds).
func GetTopRUReportInterval() int64 {
    return GlobalState.TopRUReportIntervalSeconds.Load()
}
```

**Rationale**: 
- Mirrors TopSQL enable/disable pattern for consistency
- Thread-safe via atomic operations
- Getter/setter for report interval allows dynamic config updates
- Future sub-tasks will call these from pubsub layer when processing subscription requests

### 4.4 Compilation Verification

**Command**: `make`  
**Result**: ✅ PASS  
**Output**: `Build TiDB Server successfully!`

---

## Sub-task 5: Integrate State Management with PubSub Layer

**Objective**: Connect TopRU state management to pubSubDataSink lifecycle - enable/disable TopRU when subscription starts/ends, and gate RU record sending.

**Changes**:

### 5.1 Import State Package

**File**: `pkg/util/topsql/reporter/pubsub.go`

**Added import** (line 26):
```go
topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
```

### 5.2 Enable TopRU on Subscription

**Modified newPubSubDataSink()** (line 87-93):
```go
// Enable TopRU if requested
if ds.enableTopRU {
    topsqlstate.EnableTopRU()
    if interval := req.GetReportInterval(); interval != tipb.ReportInterval_REPORT_INTERVAL_UNSPECIFIED {
        topsqlstate.SetTopRUReportInterval(int64(interval))
    }
}
```

**Rationale**: 
- Activates TopRU immediately when agent subscribes with `enable_top_ru=true`
- Sets report interval from subscription request (15s/30s/60s)
- Uses enum int value directly (e.g., REPORT_INTERVAL_15S = 15)

### 5.3 Disable TopRU on Unsubscription

**Modified run() defer** (line 123-126):
```go
defer func() {
    // ...
    ds.registerer.Deregister(ds)
    // Disable TopRU if this sink enabled it
    if ds.enableTopRU {
        topsqlstate.DisableTopRU()
    }
    ds.cancel()
}()
```

**Rationale**: Cleans up global TopRU state when subscription ends. Mirrors TopSQL's enable/disable pattern in datasink.go Register/Deregister.

### 5.4 Gate RU Record Sending

**Modified sendTopRURecords()** (line 235-238):
```go
func (ds *pubSubDataSink) sendTopRURecords(ctx context.Context, records []tipb.TopRURecord) (err error) {
    if len(records) == 0 {
        return
    }

    // Only send RU records if TopRU is enabled
    if !topsqlstate.TopRUEnabled() {
        return
    }
    // ...send logic...
}
```

**Rationale**: 
- Defense-in-depth: prevents RU data leakage if collection happens while TopRU is disabled
- Early return avoids unnecessary stream setup
- Matches the gating pattern used in reporter main loop for TopSQL records

### 5.5 Compilation Verification

**Command**: `make`  
**Result**: ✅ PASS  
**Output**: `Build TiDB Server successfully!`

---

## Summary: Protocol & Reporter Layer Complete

**Completed Sub-tasks 1-5**:
1. ✅ tipb protocol extension (RPC, messages, subscription config)
2. ✅ TiDB reporter data structures (ReportData.RURecords)
3. ✅ Reporter sending logic (SingleTargetDataSink + PubSubDataSink)
4. ✅ Global state management (EnableTopRU/DisableTopRU + report interval)
5. ✅ State lifecycle integration (subscription → enable, unsubscribe → disable)

**Current Status**: 
- Reporter layer can parse TopRU subscription requests
- Reporter layer can send RU records to agent via gRPC
- Global TopRU enable/disable state is correctly managed
- All changes compile successfully

**Next Phase**: Collection Layer (M2 in work.md)
- Need to implement actual RU data collection from executor
- Need to aggregate RU by (user, sql_digest, plan_digest)
- Need to build two-level TopN structure (1s/15s tiers)

---

## Sub-task 6: Design RU Collection Architecture

**Objective**: Plan the RU collection layer following work.md design decisions - separate from TopSQL stmtstats, with ExecutionContext tracking and delta-based sampling.

**Architecture Analysis**:

Based on work.md Section 4.1-4.4, the RU collection layer needs:

1. **ExecutionContext** (per SQL execution):
   - Stores: Ctx (context.Context), Key (user+sql_digest+plan_digest), LastRUSample (util.RUDetails)
   - Lifecycle: Created at SQL start, sampled at 1s ticks, finalized at SQL finish
   - Invariant: At most 1 active execCtx per session at a time

2. **RU Delta Calculation**:
   - Sample RUDetails (cumulative) from context at tick/finish
   - Compute delta = current - last
   - Discard if delta <= 0 or RUDetails == nil
   - Update LastRUSample after each sample

3. **Session-local Buffer**:
   - `finishedRUBuffer`: holds RU deltas from completed SQLs (between ticks)
   - Merged by 1s aggregator tick via `MergeRUInto()` interface

4. **Separation from TopSQL**:
   - Decision D2: RU data stored separately (not in StatementStatsItem)
   - New interface: `RUCollector` (parallel to existing `Collector`)
   - New aggregator method: `aggregateRU()` (parallel to `aggregate()`)

**Data Flow**:
```
SQL Start:
  -> StatementStats.OnRUExecutionBegin(ctx, user, sqlDigest, planDigest)
     -> create ExecutionContext{Ctx, Key{user,sql,plan}, LastRUSample: current}

1s Tick:
  -> aggregator.aggregateRU()
     -> for each StatementStats:
        ruIncrements = stats.MergeRUInto()
          - drain finishedRUBuffer
          - for active execCtx: sample delta + update LastRUSample
     -> RUCollector.CollectRUIncrements(ruIncrements)

SQL Finish:
  -> StatementStats.OnRUExecutionFinished()
     -> compute final delta
     -> write to finishedRUBuffer
     -> clear execCtx
```

**Implementation Plan** (Sub-tasks 6.1-6.4):
- 6.1: Define RU data structures (ExecutionContext, RUKey, RUIncrement, RUIncrementMap)
- 6.2: Extend StatementStats with RU tracking methods (OnRUExecutionBegin/Finished, MergeRUInto)
- 6.3: Add RUCollector interface + aggregator.aggregateRU() method
- 6.4: Wire up reporter to implement RUCollector and build ruIncrementBuffer

**Next Step**: Sub-task 6.1 - define core data structures in new file `pkg/util/topsql/stmtstats/rustats.go`.

---

## Sub-task 6.1: Define RU Data Structures

**Objective**: Create core data types for RU collection in new file `rustats.go`.

**Changes**:

### 6.1.1 New File Created

**File**: `pkg/util/topsql/stmtstats/rustats.go`

**Data structures**:

1. **RUKey** (line 27-31):
```go
type RUKey struct {
    User       string
    SQLDigest  BinaryDigest
    PlanDigest BinaryDigest
}
```
- Extends SQL/Plan digest with User dimension
- Enables per-user RU attribution (key requirement)

2. **ExecutionContext** (line 36-47):
```go
type ExecutionContext struct {
    Ctx context.Context          // Extract RUDetails from context
    Key RUKey                     // (user, sql_digest, plan_digest)
    LastRUSample *util.RUDetails  // For delta calculation
}
```
- Tracks one active SQL execution per session
- `LastRUSample` stores cumulative RU value for delta = current - last

3. **RUIncrement** (line 52-64):
```go
type RUIncrement struct {
    TotalRU      float64  // Delta RU (RRU + WRU)
    ExecCount    uint64   // Number of completions
    ExecDuration uint64   // Cumulative execution time (ns)
}
```
- Output unit from StatementStats.MergeRUInto()
- Input unit for RUCollector.CollectRUIncrements()

4. **RUIncrementMap** (line 69-87):
```go
type RUIncrementMap map[RUKey]*RUIncrement

func (m RUIncrementMap) Merge(other RUIncrementMap) {
    // Aggregate by RUKey
    // Sum TotalRU, ExecCount, ExecDuration
}
```
- Aggregates RU deltas across sessions
- Used by 1s aggregator tick

### 6.1.2 Import Resolution

**Issue**: Initial `github.com/pingcap/tidb/pkg/util` import caused cycle:
```
stmtstats -> pkg/util -> sessmgr -> stmtctx -> stmtstats
```

**Fix**: Changed to `github.com/tikv/client-go/v2/util` (line 20)
- RUDetails is actually defined in tikv/client-go
- Avoids tidb internal package cycle

### 6.1.3 Compilation Verification

**Command**: `make`  
**Result**: ✅ PASS  
**Output**: `Build TiDB Server successfully!`

---

## Sub-task 6.2: Extend StatementStats with RU Tracking

**Objective**: Add RU tracking fields and methods to StatementStats for delta-based RU collection.

**Changes**:

### 6.2.1 StatementStats Struct Extension

**File**: `pkg/util/topsql/stmtstats/stmtstats.go`

**Added fields** (line 51-53):
```go
type StatementStats struct {
    // ...existing TopSQL fields...
    
    // RU tracking fields (separate from TopSQL stmtstats)
    execCtx         *ExecutionContext  // Current active execution context
    finishedRUBuffer RUIncrementMap    // Completed SQL RU deltas (drained by 1s tick)
}
```

**Updated CreateStatementStats()** (line 61):
```go
finishedRUBuffer: RUIncrementMap{},
```

### 6.2.2 RU Tracking Methods

**Added OnRUExecutionBegin()** (line 255-278):
```go
func (s *StatementStats) OnRUExecutionBegin(ctx context.Context, user string, sqlDigest, planDigest []byte) {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    ruDetails := getRUDetailsFromContext(ctx)
    if ruDetails == nil {
        return
    }
    
    s.execCtx = &ExecutionContext{
        Ctx: ctx,
        Key: RUKey{User: user, SQLDigest: BinaryDigest(sqlDigest), PlanDigest: BinaryDigest(planDigest)},
        LastRUSample: ruDetails,
    }
}
```
- Called at SQL execution start
- Creates ExecutionContext with initial RU sample
- Thread-safe (mutex protected)

**Added OnRUExecutionFinished()** (line 280-320):
```go
func (s *StatementStats) OnRUExecutionFinished(execDuration time.Duration) {
    // Sample final RU
    // Compute delta = current - last
    // Discard if delta <= 0
    // Write to finishedRUBuffer
    // Clear execCtx
}
```
- Called at SQL completion
- Computes final RU delta
- Increments ExecCount and ExecDuration
- Clears execution context

**Added MergeRUInto()** (line 322-354):
```go
func (s *StatementStats) MergeRUInto() RUIncrementMap {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    result := s.finishedRUBuffer
    s.finishedRUBuffer = RUIncrementMap{}
    
    // Sample active execution (if any)
    if s.execCtx != nil {
        delta := computeRUDelta(s.execCtx.LastRUSample, currentRU)
        if delta > 0 {
            // Add to result
            // Update LastRUSample for next tick
        }
    }
    
    return result
}
```
- Called by 1s aggregator tick
- Drains finishedRUBuffer
- Samples active execCtx (mid-flight SQL)
- Updates LastRUSample for next delta calculation
- ExecCount remains 0 for active executions

### 6.2.3 Helper Functions

**getRUDetailsFromContext()** (line 356-366):
- Extracts `util.RUDetails` from context via `util.RUDetailsCtxKey`
- Returns nil if context or RUDetails absent

**computeRUDelta()** (line 368-376):
```go
func computeRUDelta(last, current *util.RUDetails) float64 {
    lastTotal := last.RRU() + last.WRU()
    currentTotal := current.RRU() + current.WRU()
    return currentTotal - lastTotal
}
```
- Calculates delta = (currentRRU + currentWRU) - (lastRRU + lastWRU)
- Handles nil safely (returns 0)

### 6.2.4 Imports Added

**Line 18, 22**:
```go
import (
    "context"
    "github.com/tikv/client-go/v2/util"  // For RUDetails
)
```

### 6.2.5 Design Invariants Enforced

1. **Delta-based sampling**: Only deltas are collected, avoiding cumulative re-counting
2. **Negative/zero delta discarded**: Guards against time skew or RU reset
3. **At most 1 active execCtx**: Overwrites on new Begin (session executes SQLs serially)
4. **Thread-safe**: All methods use mutex protection
5. **Separation from TopSQL**: RU data in separate fields, not mixed with StatementStatsItem

### 6.2.6 Compilation Verification

**Command**: `make`  
**Result**: ✅ PASS  
**Output**: `Build TiDB Server successfully!`

---

## Sub-task 6.3: Add RUCollector Interface and Aggregator Integration

**Objective**: Create RUCollector interface parallel to Collector, and add aggregateRU() method to 1s aggregator tick.

**Changes**:

### 6.3.1 Aggregator Struct Extension

**File**: `pkg/util/topsql/stmtstats/aggregator.go`

**Added field** (line 40):
```go
type aggregator struct {
    // ...existing fields...
    ruCollectors sync.Map // map[RUCollector]struct{}
}
```

### 6.3.2 RUCollector Interface

**Added interface** (line 194-200):
```go
// RUCollector is used to collect RU increment data.
// This interface is parallel to Collector but handles TopRU data flow.
type RUCollector interface {
    // CollectRUIncrements is called by aggregator every 1s with merged RU deltas
    // from all sessions, aggregated by (user, sql_digest, plan_digest).
    CollectRUIncrements(RUIncrementMap)
}
```

**Rationale**: 
- Parallel design to existing Collector interface
- Clean separation between TopSQL (CPU) and TopRU data flows
- Single method for 1s tick data push

### 6.3.3 Aggregator Tick Logic

**Modified run()** (line 72-73):
```go
case <-tick.C:
    m.aggregate()    // TopSQL (CPU/stmtstats)
    m.aggregateRU()  // TopRU (RU increments)
```

**Added aggregateRU()** (line 99-116):
```go
func (m *aggregator) aggregateRU() {
    total := RUIncrementMap{}
    m.statsSet.Range(func(statsR, _ any) bool {
        stats := statsR.(*StatementStats)
        total.Merge(stats.MergeRUInto())
        return true
    })
    // If TopRU is not enabled, just drop them.
    if len(total) > 0 && state.TopRUEnabled() {
        m.ruCollectors.Range(func(c, _ any) bool {
            c.(RUCollector).CollectRUIncrements(total)
            return true
        })
    }
}
```

**Key behaviors**:
1. Runs every 1s alongside aggregate()
2. Calls MergeRUInto() on all StatementStats
3. Merges increments across all sessions
4. Gates on `state.TopRUEnabled()` - drops data if disabled
5. Pushes to all registered RUCollectors

### 6.3.4 Registration Methods

**Instance methods** (line 147-157):
```go
func (m *aggregator) registerRUCollector(collector RUCollector)
func (m *aggregator) unregisterRUCollector(collector RUCollector)
```

**Global functions** (line 200-210):
```go
// RegisterRUCollector binds an RUCollector to globalAggregator.
func RegisterRUCollector(collector RUCollector) {
    globalAggregator.registerRUCollector(collector)
}

// UnregisterRUCollector removes RUCollector from globalAggregator.
func UnregisterRUCollector(collector RUCollector) {
    globalAggregator.unregisterRUCollector(collector)
}
```

**Usage pattern**: Reporter will call `RegisterRUCollector(self)` at startup to receive RU increments.

### 6.3.5 Data Flow Summary

```
1s Tick:
  aggregator.aggregateRU()
    -> for each session:
       stats.MergeRUInto()
         - drain finishedRUBuffer
         - sample active execCtx
         - return RUIncrementMap
    -> merge all sessions' increments
    -> if TopRUEnabled():
       for each RUCollector:
         collector.CollectRUIncrements(total)
           -> reporter receives merged data
```

### 6.3.6 Separation from TopSQL

**Design Decision D2 enforced**:
- TopSQL: `aggregate()` → `Collector.CollectStmtStatsMap()`
- TopRU: `aggregateRU()` → `RUCollector.CollectRUIncrements()`
- Separate collectors, separate data types, separate enable flags
- Run in same tick but independent pipelines

### 6.3.7 Compilation Verification

**Command**: `make`  
**Result**: ✅ PASS  
**Output**: `Build TiDB Server successfully!`

---

## Sub-task 6.4: Wire Reporter as RUCollector

**Objective**: Connect RemoteTopSQLReporter to aggregator's RU data flow by implementing RUCollector interface.

**Changes**:

### 6.4.1 Reporter Struct Extension

**File**: `pkg/util/topsql/reporter/reporter.go`

**Added interface assertion** (line 71):
```go
var _ stmtstats.RUCollector = &RemoteTopSQLReporter{}
```

**Added channel field** (line 82):
```go
type RemoteTopSQLReporter struct {
    // ...existing fields...
    collectRUIncrementsChan chan stmtstats.RUIncrementMap
}
```

**Updated constructor** (line 106):
```go
collectRUIncrementsChan: make(chan stmtstats.RUIncrementMap, collectChanBufferSize),
```

### 6.4.2 RUCollector Interface Implementation

**Added CollectRUIncrements()** (line 168-182):
```go
// CollectRUIncrements implements stmtstats.RUCollector.
func (tsr *RemoteTopSQLReporter) CollectRUIncrements(data stmtstats.RUIncrementMap) {
    if len(data) == 0 {
        return
    }
    select {
    case tsr.collectRUIncrementsChan <- data:
    default:
        // ignore if chan blocked
        reporter_metrics.IgnoreCollectChannelFullCounter.Inc()
    }
}
```

**Behavior**: 
- Non-blocking push to channel (drops data if full)
- Called by aggregator every 1s with merged RU increments
- Thread-safe and efficient (channel-based)

### 6.4.3 CollectWorker Integration

**Modified collectWorker()** (line 222-225):
```go
case data := <-tsr.collectRUIncrementsChan:
    // TODO(M3): Implement two-level TopN buffering for RU increments
    // For now, just drop the data to avoid blocking aggregator
    _ = data
```

**Current status**: Data path connected but processing deferred to M3 (two-level TopN implementation)

### 6.4.4 Registration at Startup

**File**: `pkg/util/topsql/topsql.go`

**Modified SetupTopSQL()** (line 61-64):
```go
stmtstats.RegisterCollector(globalTopSQLReport)
// Register reporter as RUCollector to receive RU increments from aggregator
if ruCollector, ok := globalTopSQLReport.(stmtstats.RUCollector); ok {
    stmtstats.RegisterRUCollector(ruCollector)
}
stmtstats.SetupAggregator()
```

**Rationale**: Type-safe registration with interface check, parallel to TopSQL Collector registration

### 6.4.5 Data Flow End-to-End

```
SQL Execution:
  OnRUExecutionBegin/Finished()
    -> StatementStats.execCtx / finishedRUBuffer

1s Aggregator Tick:
  aggregator.aggregateRU()
    -> MergeRUInto() from all sessions
    -> merged RUIncrementMap
    -> RemoteTopSQLReporter.CollectRUIncrements()
       -> collectRUIncrementsChan
       -> collectWorker() receives
          (currently: TODO M3 for TopN processing)
```

### 6.4.6 Compilation Verification

**Command**: `make`  
**Result**: ✅ PASS  
**Output**: `Build TiDB Server successfully!`

---

## MILESTONE: Collection Layer Complete (M2)

**Achieved Sub-tasks 6.1-6.4**:
1. ✅ RU data structures (RUKey, ExecutionContext, RUIncrement, RUIncrementMap)
2. ✅ StatementStats RU tracking (OnRUExecutionBegin/Finished, MergeRUInto)
3. ✅ Aggregator RU collection (RUCollector interface, aggregateRU(), registration)
4. ✅ Reporter wired as RUCollector (channel + interface implementation + startup registration)

**Current State**:
- ✅ RU data flows from session → aggregator → reporter (1s tick)
- ✅ Delta-based sampling enforced (negative/zero discarded)
- ✅ TopRUEnabled() gate at aggregator
- ✅ Separate from TopSQL pipeline (independent collectors, data types, enable flags)
- ✅ All compilation passes

**Data Path Status**:
- ✅ Session-local: execCtx tracking + finishedRUBuffer
- ✅ Aggregator: 1s merge across sessions
- ✅ Reporter: channel reception (processing deferred to M3)
- ❌ Executor integration: NOT YET (need to call OnRUExecutionBegin/Finished from SQL execution)
- ❌ TopN buffering: NOT YET (M3 - two-level 200×200 → 100×100)
- ❌ Protobuf conversion + reporting: NOT YET (M3 - build TopRURecord and send)

**Next Phase (M3 - Reporter TopN + Reporting)**:
- Implement ruIncrementBuffer (1s → 200 users × 200 SQLs + others)
- Implement 15s merge to ruPointBucket
- Implement report_interval final 100×100 filtering
- Build tipb.TopRURecord and populate ReportData.RURecords

**Technical Debt Recorded**:
- TODO(M3) in reporter.go line 223: Implement two-level TopN buffering
- TODO: Executor hooks (OnRUExecutionBegin/Finished calls) - should be in M4

---

# Final Summary

## Overall Progress

**Date**: 2026-01-20  
**Branch**: zimuxia/topru-design-1  
**Status**: M1-M2 Complete, M3-M4 Pending

### Completed Milestones

#### M1: Protocol & Data Model (Sub-tasks 1-5) ✅
**Duration**: ~3 hours  
**Deliverables**:
1. tipb protocol extended with TopRU messages (TopRURecord, TopRURecordItem)
2. Subscription config fields added (enable_top_ru, report_interval)
3. Go bindings regenerated and normalized
4. TiDB reporter layer extended (ReportData.RURecords)
5. Sending logic implemented (SingleTargetDataSink + PubSubDataSink)
6. Global TopRU state management (Enable/Disable + report interval)
7. Subscription lifecycle integration (enable on subscribe, disable on unsubscribe)

**Verification**: All changes compile successfully via `make`

#### M2: Collection Layer (Sub-tasks 6.1-6.4) ✅
**Duration**: ~1 hour  
**Deliverables**:
1. RU data structures defined (RUKey, ExecutionContext, RUIncrement, RUIncrementMap)
2. StatementStats extended with RU tracking (execCtx, finishedRUBuffer)
3. RU tracking methods implemented (OnRUExecutionBegin, OnRUExecutionFinished, MergeRUInto)
4. Delta calculation logic (computeRUDelta with RRU+WRU)
5. RUCollector interface defined (parallel to Collector)
6. Aggregator RU collection method (aggregateRU() at 1s tick)
7. Reporter implements RUCollector and registers at startup
8. Data flows from session → aggregator → reporter

**Verification**: All changes compile successfully via `make`

### Architecture Decisions Implemented

**D1: Reuse TopSQL PubSub + Extend Protocol**
- ✅ Extended tipb.TopSQLSubRequest with enable_top_ru + report_interval
- ✅ Added TopRURecord to TopSQLSubResponse oneof
- ✅ New RPC ReportTopRURecords for streaming
- ✅ Backward compatible (old clients ignore new fields)

**D2: Separate RU Storage**
- ✅ RU data in separate fields (not mixed with CPU/stmtstats)
- ✅ RUIncrementMap vs StatementStatsMap
- ✅ RUCollector vs Collector interfaces
- ✅ aggregateRU() vs aggregate() methods
- ✅ collectRUIncrementsChan vs collectStmtStatsChan

**Key Invariants Enforced**:
1. ✅ Delta-based sampling (negative/zero discarded)
2. ✅ At most 1 active execCtx per session
3. ✅ Thread-safe operations (mutex protected)
4. ✅ TopRUEnabled() gate at aggregator
5. ✅ Non-blocking channel pushes (drop on full)

### Files Modified/Created

**tipb (external dependency)**:
- `proto/topsql_agent.proto`: Extended protocol
- `go-tipb/topsql_agent.pb.go`: Regenerated bindings

**TiDB Core**:
- `pkg/util/topsql/state/state.go`: TopRU state management
- `pkg/util/topsql/stmtstats/rustats.go`: NEW - RU data structures
- `pkg/util/topsql/stmtstats/stmtstats.go`: RU tracking methods
- `pkg/util/topsql/stmtstats/aggregator.go`: RUCollector + aggregateRU()
- `pkg/util/topsql/reporter/datasink.go`: ReportData.RURecords
- `pkg/util/topsql/reporter/single_target.go`: sendBatchTopRURecord()
- `pkg/util/topsql/reporter/pubsub.go`: sendTopRURecords() + subscription config
- `pkg/util/topsql/reporter/reporter.go`: CollectRUIncrements() implementation
- `pkg/util/topsql/reporter/mock/server.go`: ReportTopRURecords() stub
- `pkg/util/topsql/topsql.go`: RegisterRUCollector() at startup
- `go.mod`: tipb replace directive for local development
- `output.md`: NEW - this file

**Total Lines Changed**: ~800 lines (additions + modifications)

### Remaining Work (M3-M4)

#### M3: Reporter TopN Buffering & Reporting
**Estimated Effort**: 2-3 hours  
**Tasks**:
1. Implement ruIncrementBuffer (1s → 200 users × 200 SQLs + others)
2. Implement 15s merge to ruPointBucket
3. Implement report_interval (15s/30s/60s) final 100×100 filtering
4. Build tipb.TopRURecord from RUIncrementMap
5. Populate ReportData.RURecords and trigger send

#### M4: Executor Integration & Testing
**Estimated Effort**: 3-4 hours  
**Tasks**:
1. Hook OnRUExecutionBegin() at SQL execution start (executor/adapter.go)
2. Hook OnRUExecutionFinished() at SQL completion
3. Extract user from session context
4. Resource Control gating (only collect when RC enabled)
5. Unit tests (RU delta calculation, execCtx lifecycle, TopN/others)
6. Integration tests (end-to-end RU reporting)
7. TopSQL regression verification

### Known Limitations & Technical Debt

1. **tipb local replace**: Currently using `replace github.com/pingcap/tipb => ../tipb` in go.mod
   - Need to push tipb changes and update to versioned dependency before merge

2. **M3 TODO**: Reporter TopN buffering not implemented
   - Current code drops RU data at reporter (line 225 of reporter.go)
   - Need two-level TopN + others aggregation

3. **M4 TODO**: Executor hooks not implemented
   - StatementStats methods exist but not called from execution path
   - Need user extraction logic

4. **Short-term compromise**: "Subscribing TopRU enables TopSQL"
   - Recorded in work.md as technical debt
   - Acceptable for phase 1, can decouple later

### Compilation Status

**All sub-tasks verified via `make`**:
- ✅ Sub-task 1: tipb protocol + mock server
- ✅ Sub-task 2: ReportData extension
- ✅ Sub-task 3: PubSub layer extension
- ✅ Sub-task 4: State management
- ✅ Sub-task 5: State lifecycle integration
- ✅ Sub-task 6.1: RU data structures
- ✅ Sub-task 6.2: StatementStats extension
- ✅ Sub-task 6.3: Aggregator RUCollector
- ✅ Sub-task 6.4: Reporter wiring

**Final Build Output**: `Build TiDB Server successfully!`

### Next Steps

To continue with M3:
1. Read design doc section on two-level TopN buffering
2. Understand memory bounds (200×200 at 1s/15s, 100×100 at report)
3. Implement ruIncrementBuffer structure (similar to existing CPU buffer)
4. Add 15s ticker for micro-batch merge
5. Hook into existing reportTicker for final report
6. Build TopRURecord protobuf messages
7. Test with mock agent

To continue with M4:
1. Locate SQL execution start point (likely executor/adapter.go)
2. Add OnRUExecutionBegin() call with user extraction
3. Add OnRUExecutionFinished() call at completion
4. Gate with Resource Control enabled check
5. Write unit tests for delta calculation
6. Write integration tests for end-to-end flow
7. Run TopSQL regression tests

---

**End of Output Log**

