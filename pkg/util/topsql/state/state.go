// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package state

import "go.uber.org/atomic"

// Default Top-SQL state values.
const (
	DefTiDBTopSQLEnable                = false
	DefTiDBTopSQLPrecisionSeconds      = 1
	DefTiDBTopSQLMaxTimeSeriesCount    = 100
	DefTiDBTopSQLMaxMetaCount          = 5000
	DefTiDBTopSQLReportIntervalSeconds = 60
)

// Default Top-RU state values.
//
// Design Notes:
//   - TopRU defaults to disabled (enable via subscription with enable_top_ru=true)
//   - Default 60s report interval aligns with TopSQL; can be 15s/30s/60s via subscription
//   - TopRU enable/disable is independent from TopSQL enable/disable
//   - Phase 3: Reference-counted subscriber tracking ensures subscriber isolation
const (
	DefTiDBTopRUEnable                = false
	DefTiDBTopRUReportIntervalSeconds = 60
)

// GlobalState is the global Top-SQL state.
var GlobalState = State{
	enable:                     atomic.NewBool(false),
	PrecisionSeconds:           atomic.NewInt64(DefTiDBTopSQLPrecisionSeconds),
	MaxStatementCount:          atomic.NewInt64(DefTiDBTopSQLMaxTimeSeriesCount),
	MaxCollect:                 atomic.NewInt64(DefTiDBTopSQLMaxMetaCount),
	ReportIntervalSeconds:      atomic.NewInt64(DefTiDBTopSQLReportIntervalSeconds),
	ruConsumerCount:            atomic.NewInt64(0),
	TopRUReportIntervalSeconds: atomic.NewInt64(DefTiDBTopRUReportIntervalSeconds),
}

// State is the state for control top sql feature.
type State struct {
	// enable top-sql or not.
	enable *atomic.Bool
	// The refresh interval of top-sql.
	PrecisionSeconds *atomic.Int64
	// The maximum number of statements kept in memory.
	MaxStatementCount *atomic.Int64
	// The maximum capacity of the collect map.
	MaxCollect *atomic.Int64
	// The report data interval of top-sql.
	ReportIntervalSeconds *atomic.Int64

	// ruConsumerCount tracks the number of active TopRU subscribers.
	// Phase 3 Design: Reference-counted subscriber tracking.
	//   - TopRU is enabled when ruConsumerCount > 0
	//   - TopRU is disabled when ruConsumerCount == 0
	// This ensures subscriber isolation: one subscriber's unsubscribe
	// does not affect others.
	ruConsumerCount *atomic.Int64
	// The report data interval of top-ru.
	// Set from subscription request (15s/30s/60s); defaults to 60s.
	// Phase 3: Smaller interval prevails when multiple subscribers set different values.
	TopRUReportIntervalSeconds *atomic.Int64
}

// EnableTopSQL enables the top SQL feature.
func EnableTopSQL() {
	GlobalState.enable.Store(true)
}

// DisableTopSQL disables the top SQL feature.
func DisableTopSQL() {
	GlobalState.enable.Store(false)
}

// TopSQLEnabled uses to check whether enabled the top SQL feature.
func TopSQLEnabled() bool {
	return GlobalState.enable.Load()
}

// TopProfilingEnabled returns true if either TopSQL or TopRU is enabled.
//
// NOTE: This helper is intended for execution-path hooks that should run when
// any Top* consumer exists (e.g. registering SQL/plan metas, stmt lifecycle
// callbacks). CPU profiling / parsing should still be gated by TopSQLEnabled().
func TopProfilingEnabled() bool {
	return TopSQLEnabled() || TopRUEnabled()
}

// EnableTopRU increments the TopRU consumer count.
// Called by pubSubDataSink when agent subscribes with enable_top_ru=true.
// This activates RU collection in aggregator.aggregateRU() when count becomes > 0.
//
// Phase 3 Design: Reference-counted subscriber tracking.
// TopRU is enabled when at least one subscriber has enabled it.
func EnableTopRU() {
	GlobalState.ruConsumerCount.Inc()
}

// DisableTopRU decrements the TopRU consumer count.
// Called by pubSubDataSink when subscription ends (defer in run()).
// TopRU collection stops only when count reaches 0 (no more subscribers).
//
// Phase 3 Design: Reference-counted subscriber tracking.
// This ensures one subscriber's unsubscribe does not affect others.
// When the last subscriber leaves, the report interval is reset to default.
func DisableTopRU() {
	for {
		current := GlobalState.ruConsumerCount.Load()
		if current <= 0 {
			// Already at 0, nothing to decrement (defensive guard)
			return
		}
		if GlobalState.ruConsumerCount.CAS(current, current-1) {
			// If this was the last subscriber, reset report interval to default
			if current == 1 {
				ResetTopRUReportInterval()
			}
			return
		}
		// CAS failed, retry
	}
}

// TopRUEnabled checks whether TopRU feature is enabled.
// Returns true if at least one subscriber has enabled TopRU.
// Used by aggregator.aggregateRU() to gate RU data push.
// Also used by sendTopRURecords() as defense-in-depth.
//
// Phase 3 Design: enable_topru == (ruConsumerCount > 0)
func TopRUEnabled() bool {
	return GlobalState.ruConsumerCount.Load() > 0
}

// SetTopRUReportInterval sets the report interval for TopRU (in seconds).
// Called from pubSubDataSink when processing subscription request.
// Valid values: 15, 30, 60 (from tipb.ReportInterval enum).
//
// Phase 3 Design: When multiple subscribers set different intervals,
// the smaller interval prevails. This ensures all subscribers receive
// data at least as frequently as they requested.
func SetTopRUReportInterval(intervalSeconds int64) {
	for {
		current := GlobalState.TopRUReportIntervalSeconds.Load()
		// Smaller interval prevails
		if intervalSeconds >= current {
			// Current interval is already smaller or equal, no change needed
			return
		}
		if GlobalState.TopRUReportIntervalSeconds.CAS(current, intervalSeconds) {
			return
		}
		// CAS failed, retry
	}
}

// GetTopRUReportInterval returns the report interval for TopRU (in seconds).
// Phase 2 Extension Point:
//   - TODO(M3): Used by reporter to determine report_interval bucket merging
func GetTopRUReportInterval() int64 {
	return GlobalState.TopRUReportIntervalSeconds.Load()
}

// ResetTopRUReportInterval resets the report interval to the default value.
// Called when the last TopRU subscriber unsubscribes.
// This allows the next subscriber to set their preferred interval without
// being constrained by a previous subscriber's smaller interval.
func ResetTopRUReportInterval() {
	GlobalState.TopRUReportIntervalSeconds.Store(DefTiDBTopRUReportIntervalSeconds)
}
