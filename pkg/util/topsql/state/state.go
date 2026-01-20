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
const (
	DefTiDBTopRUEnable                = false
	DefTiDBTopRUReportIntervalSeconds = 60
)

// GlobalState is the global Top-SQL state.
var GlobalState = State{
	enable:                atomic.NewBool(false),
	PrecisionSeconds:      atomic.NewInt64(DefTiDBTopSQLPrecisionSeconds),
	MaxStatementCount:     atomic.NewInt64(DefTiDBTopSQLMaxTimeSeriesCount),
	MaxCollect:            atomic.NewInt64(DefTiDBTopSQLMaxMetaCount),
	ReportIntervalSeconds: atomic.NewInt64(DefTiDBTopSQLReportIntervalSeconds),
	enableTopRU:                atomic.NewBool(DefTiDBTopRUEnable),
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

	// enable top-ru or not.
	// Controlled by pubSubDataSink lifecycle: enabled on subscribe, disabled on unsubscribe.
	// Independent from TopSQL enable flag.
	enableTopRU *atomic.Bool
	// The report data interval of top-ru.
	// Set from subscription request (15s/30s/60s); defaults to 60s.
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

// EnableTopRU enables the top RU feature.
// Called by pubSubDataSink when agent subscribes with enable_top_ru=true.
// This activates RU collection in aggregator.aggregateRU().
func EnableTopRU() {
	GlobalState.enableTopRU.Store(true)
}

// DisableTopRU disables the top RU feature.
// Called by pubSubDataSink when subscription ends (defer in run()).
// This stops RU data from being pushed to RUCollectors.
func DisableTopRU() {
	GlobalState.enableTopRU.Store(false)
}

// TopRUEnabled checks whether enabled the top RU feature.
// Used by aggregator.aggregateRU() to gate RU data push.
// Also used by sendTopRURecords() as defense-in-depth.
func TopRUEnabled() bool {
	return GlobalState.enableTopRU.Load()
}

// SetTopRUReportInterval sets the report interval for TopRU (in seconds).
// Called from pubSubDataSink when processing subscription request.
// Valid values: 15, 30, 60 (from tipb.ReportInterval enum).
func SetTopRUReportInterval(intervalSeconds int64) {
	GlobalState.TopRUReportIntervalSeconds.Store(intervalSeconds)
}

// GetTopRUReportInterval returns the report interval for TopRU (in seconds).
// Phase 2 Extension Point:
//   - TODO(M3): Used by reporter to determine report_interval bucket merging
func GetTopRUReportInterval() int64 {
	return GlobalState.TopRUReportIntervalSeconds.Load()
}
