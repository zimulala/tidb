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

package stmtstats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

// String is only used for debugging.
func (d SQLPlanDigest) String() string {
	bs := bytes.NewBufferString("")
	if len(d.SQLDigest) >= 5 {
		bs.Write([]byte(d.SQLDigest)[:5])
	}
	if len(d.PlanDigest) >= 5 {
		bs.WriteRune('-')
		bs.Write([]byte(d.PlanDigest)[:5])
	}
	return bs.String()
}

// String is only used for debugging.
func (m StatementStatsMap) String() string {
	if len(m) == 0 {
		return "StatementStatsMap {}"
	}
	bs := bytes.NewBufferString("")
	bs.WriteString("StatementStatsMap {\n")
	for k, v := range m {
		bs.WriteString(fmt.Sprintf("    %s => %s\n", k, v))
	}
	bs.WriteString("}")
	return bs.String()
}

// String is only used for debugging.
func (i *StatementStatsItem) String() string {
	if i == nil {
		return "<nil>"
	}
	b, _ := json.Marshal(i)
	return string(b)
}

func TestKvStatementStatsItem_Merge(t *testing.T) {
	item1 := KvStatementStatsItem{
		KvExecCount: map[string]uint64{
			"127.0.0.1:10001": 1,
			"127.0.0.1:10002": 2,
		},
	}
	item2 := KvStatementStatsItem{
		KvExecCount: map[string]uint64{
			"127.0.0.1:10002": 2,
			"127.0.0.1:10003": 3,
		},
	}
	assert.Len(t, item1.KvExecCount, 2)
	assert.Len(t, item2.KvExecCount, 2)
	item1.Merge(item2)
	assert.Len(t, item1.KvExecCount, 3)
	assert.Len(t, item2.KvExecCount, 2)
	assert.Equal(t, uint64(1), item1.KvExecCount["127.0.0.1:10001"])
	assert.Equal(t, uint64(3), item1.KvExecCount["127.0.0.1:10003"])
	assert.Equal(t, uint64(3), item1.KvExecCount["127.0.0.1:10003"])
}

func TestStatementsStatsItem_Merge(t *testing.T) {
	item1 := &StatementStatsItem{
		ExecCount:       1,
		SumDurationNs:   100,
		KvStatsItem:     NewKvStatementStatsItem(),
		NetworkInBytes:  10,
		NetworkOutBytes: 20,
	}
	item2 := &StatementStatsItem{
		ExecCount:       2,
		SumDurationNs:   50,
		KvStatsItem:     NewKvStatementStatsItem(),
		NetworkInBytes:  50,
		NetworkOutBytes: 60,
	}
	item1.Merge(item2)
	assert.Equal(t, uint64(3), item1.ExecCount)
	assert.Equal(t, uint64(150), item1.SumDurationNs)
	assert.Equal(t, uint64(60), item1.NetworkInBytes)
	assert.Equal(t, uint64(80), item1.NetworkOutBytes)
}

func TestStatementStatsMap_Merge(t *testing.T) {
	m1 := StatementStatsMap{
		SQLPlanDigest{SQLDigest: "SQL-1"}: &StatementStatsItem{
			ExecCount:     1,
			SumDurationNs: 100,
			KvStatsItem: KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
		SQLPlanDigest{SQLDigest: "SQL-2"}: &StatementStatsItem{
			ExecCount:     1,
			SumDurationNs: 200,
			KvStatsItem: KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
	}
	m2 := StatementStatsMap{
		SQLPlanDigest{SQLDigest: "SQL-2"}: &StatementStatsItem{
			ExecCount:     1,
			SumDurationNs: 100,
			KvStatsItem: KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
		SQLPlanDigest{SQLDigest: "SQL-3"}: &StatementStatsItem{
			ExecCount:     1,
			SumDurationNs: 50,
			KvStatsItem: KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
	}
	assert.Len(t, m1, 2)
	assert.Len(t, m2, 2)
	m1.Merge(m2)
	assert.Len(t, m1, 3)
	assert.Len(t, m2, 2)
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].ExecCount)
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].ExecCount)
	assert.Equal(t, uint64(100), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].SumDurationNs)
	assert.Equal(t, uint64(300), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].SumDurationNs)
	assert.Equal(t, uint64(50), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].SumDurationNs)
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].KvStatsItem.KvExecCount["KV-1"])
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].KvStatsItem.KvExecCount["KV-2"])
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].KvStatsItem.KvExecCount["KV-1"])
	assert.Equal(t, uint64(4), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].KvStatsItem.KvExecCount["KV-2"])
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].KvStatsItem.KvExecCount["KV-1"])
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].KvStatsItem.KvExecCount["KV-2"])
	m1.Merge(nil)
	assert.Len(t, m1, 3)
}

func TestCreateStatementStats(t *testing.T) {
	stats := CreateStatementStats()
	assert.NotNil(t, stats)
	_, ok := globalAggregator.statsSet.Load(stats)
	assert.True(t, ok)
	assert.False(t, stats.Finished())
	stats.SetFinished()
	assert.True(t, stats.Finished())
}

func TestExecCounter_AddExecCount_Take(t *testing.T) {
	stats := CreateStatementStats()
	m := stats.Take()
	assert.Len(t, m, 0)
	for range 1 {
		stats.OnExecutionBegin([]byte("SQL-1"), []byte(""), &ExecBeginInfo{InNetworkBytes: 0})
	}
	for range 2 {
		stats.OnExecutionBegin([]byte("SQL-2"), []byte(""), &ExecBeginInfo{InNetworkBytes: 0})
		stats.OnExecutionFinished([]byte("SQL-2"), []byte(""), &ExecFinishInfo{ExecDuration: time.Second})
	}
	for range 3 {
		stats.OnExecutionBegin([]byte("SQL-3"), []byte(""), &ExecBeginInfo{InNetworkBytes: 0})
		stats.OnExecutionFinished([]byte("SQL-3"), []byte(""), &ExecFinishInfo{ExecDuration: time.Millisecond})
	}
	stats.OnExecutionFinished([]byte("SQL-3"), []byte(""), &ExecFinishInfo{ExecDuration: -time.Millisecond})
	m = stats.Take()
	assert.Len(t, m, 3)
	assert.Equal(t, uint64(1), m[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
	assert.Equal(t, uint64(0), m[SQLPlanDigest{SQLDigest: "SQL-1"}].SumDurationNs)
	assert.Equal(t, uint64(2), m[SQLPlanDigest{SQLDigest: "SQL-2"}].ExecCount)
	assert.Equal(t, uint64(2*10e8), m[SQLPlanDigest{SQLDigest: "SQL-2"}].SumDurationNs)
	assert.Equal(t, uint64(3), m[SQLPlanDigest{SQLDigest: "SQL-3"}].ExecCount)
	assert.Equal(t, uint64(3*10e5), m[SQLPlanDigest{SQLDigest: "SQL-3"}].SumDurationNs)
	m = stats.Take()
	assert.Len(t, m, 0)
}

func TestOnExecutionBeginFinishRU(t *testing.T) {
	stats := CreateStatementStats()
	stats.OnExecutionBegin([]byte("sql1"), []byte("plan1"), &ExecBeginInfo{
		User:         "user1",
		TopRUEnabled: true,
	})
	ru := util.NewRUDetailsWith(10.0, 20.0, time.Millisecond)
	stats.OnExecutionFinished([]byte("sql1"), []byte("plan1"), &ExecFinishInfo{
		User:         "user1",
		TopRUEnabled: true,
		RUDetails:    ru,
		ExecDuration: time.Second,
	})

	m := stats.MergeRUInto()
	require.Len(t, m, 1)
	key := RUKey{User: "user1", SQLDigest: BinaryDigest("sql1"), PlanDigest: BinaryDigest("plan1")}
	incr, ok := m[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.Equal(t, 30.0, incr.TotalRU)
	require.Equal(t, uint64(time.Second.Nanoseconds()), incr.ExecDuration)
}

func TestMergeRUIntoInFlightSamplingAndFinishDedup(t *testing.T) {
	stats := CreateStatementStats()
	ru := util.NewRUDetailsWith(0, 0, 0)
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)
	key := RUKey{User: "user1", SQLDigest: BinaryDigest("sql1"), PlanDigest: BinaryDigest("plan1")}

	stats.OnExecutionBegin([]byte("sql1"), []byte("plan1"), &ExecBeginInfo{
		User:         "user1",
		TopRUEnabled: true,
		Ctx:          ctx,
	})

	total := RUIncrementMap{}

	ru.Merge(util.NewRUDetailsWith(10, 0, 0))
	total.Merge(stats.MergeRUInto())

	ru.Merge(util.NewRUDetailsWith(5, 0, 0))
	total.Merge(stats.MergeRUInto())

	ru.Merge(util.NewRUDetailsWith(7, 0, 0))
	stats.OnExecutionFinished([]byte("sql1"), []byte("plan1"), &ExecFinishInfo{
		User:         "user1",
		TopRUEnabled: true,
		RUDetails:    ru,
		ExecDuration: 2 * time.Second,
	})
	total.Merge(stats.MergeRUInto())

	incr, ok := total[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.InDelta(t, 22.0, incr.TotalRU, 1e-9)
	require.Equal(t, uint64((2 * time.Second).Nanoseconds()), incr.ExecDuration)

	require.Nil(t, stats.execCtx)
	ru.Merge(util.NewRUDetailsWith(3, 0, 0))
	require.Len(t, stats.MergeRUInto(), 0)
}

func TestMergeRUIntoHandlesRUResetAndNilRUDetails(t *testing.T) {
	stats := CreateStatementStats()
	ru := util.NewRUDetailsWith(10, 0, 0)
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)
	key := RUKey{User: "user2", SQLDigest: BinaryDigest("sql2"), PlanDigest: BinaryDigest("plan2")}

	stats.OnExecutionBegin([]byte("sql2"), []byte("plan2"), &ExecBeginInfo{
		User:         "user2",
		TopRUEnabled: true,
		Ctx:          ctx,
	})
	first := stats.MergeRUInto()
	require.Len(t, first, 1)
	require.InDelta(t, 10.0, first[key].TotalRU, 1e-9)

	stats.mu.Lock()
	stats.execCtx.LastRUSample = util.NewRUDetailsWith(100, 0, 0)
	stats.mu.Unlock()
	require.Len(t, stats.MergeRUInto(), 0)

	ru.Merge(util.NewRUDetailsWith(5, 0, 0))
	next := stats.MergeRUInto()
	require.Len(t, next, 1)
	require.InDelta(t, 5.0, next[key].TotalRU, 1e-9)
	require.GreaterOrEqual(t, next[key].TotalRU, 0.0)

	stats.OnExecutionFinished([]byte("sql2"), []byte("plan2"), &ExecFinishInfo{
		User:         "user2",
		TopRUEnabled: true,
		RUDetails:    nil,
		ExecDuration: time.Second,
	})
	require.Nil(t, stats.execCtx)
}
