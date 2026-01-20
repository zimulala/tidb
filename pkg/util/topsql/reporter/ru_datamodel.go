// Copyright 2026 PingCAP, Inc.
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

package reporter

import (
	"bytes"
	"sort"

	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/wangjohn/quickselect"
)

// TopN limits for RU aggregation (Phase 2 Decision A: Hybrid TopN).
// These values are implementation-defined per design doc.
const (
	// maxTopUsers is the maximum number of users to keep in global TopN.
	maxTopUsers = 200
	// maxTopSQLsPerUser is the maximum number of SQLs to keep per user.
	maxTopSQLsPerUser = 200
	// keyRUOthersUser is the special user key for aggregated "others" users.
	keyRUOthersUser = ""
	// keyRUOthersSQL is the special SQL key for aggregated "others" SQLs within a user.
	keyRUOthersSQL = ""
)

// ruItem represents RU statistics for a single timestamp.
// Parallel to tsItem in datamodel.go but for RU data.
type ruItem struct {
	timestamp    uint64
	totalRU      float64
	execCount    uint64
	execDuration uint64
}

// toProto converts ruItem to protobuf representation.
func (i *ruItem) toProto() *tipb.TopRURecordItem {
	return &tipb.TopRURecordItem{
		TimestampSec: i.timestamp,
		TotalRu:      i.totalRU,
		ExecCount:    i.execCount,
		ExecDuration: i.execDuration,
	}
}

// ruItems is a sortable list of ruItem, sorted by timestamp (asc).
type ruItems []ruItem

func (rs ruItems) Len() int           { return len(rs) }
func (rs ruItems) Less(i, j int) bool { return rs[i].timestamp < rs[j].timestamp }
func (rs ruItems) Swap(i, j int)      { rs[i], rs[j] = rs[j], rs[i] }

// toProto converts ruItems to protobuf representation.
func (rs ruItems) toProto() []*tipb.TopRURecordItem {
	if len(rs) == 0 {
		return nil
	}
	items := make([]*tipb.TopRURecordItem, 0, len(rs))
	for _, item := range rs {
		items = append(items, item.toProto())
	}
	return items
}

// ruRecord represents RU statistics for a single (sql_digest, plan_digest) combination.
// Used within a user's SQL tracking.
type ruRecord struct {
	sqlDigest  []byte
	planDigest []byte
	items      ruItems
	tsIndex    map[uint64]int // timestamp => index in items
	totalRU    float64        // cumulative RU for TopN sorting
}

func newRURecord(sqlDigest, planDigest []byte) *ruRecord {
	return &ruRecord{
		sqlDigest:  sqlDigest,
		planDigest: planDigest,
		items:      make(ruItems, 0, 64),
		tsIndex:    make(map[uint64]int, 64),
	}
}

// add adds RU increment data for a specific timestamp.
func (r *ruRecord) add(timestamp uint64, totalRU float64, execCount, execDuration uint64) {
	if idx, ok := r.tsIndex[timestamp]; ok {
		r.items[idx].totalRU += totalRU
		r.items[idx].execCount += execCount
		r.items[idx].execDuration += execDuration
	} else {
		r.tsIndex[timestamp] = len(r.items)
		r.items = append(r.items, ruItem{
			timestamp:    timestamp,
			totalRU:      totalRU,
			execCount:    execCount,
			execDuration: execDuration,
		})
	}
	r.totalRU += totalRU
}

// merge merges another ruRecord into this one.
func (r *ruRecord) merge(other *ruRecord) {
	if other == nil {
		return
	}
	for _, item := range other.items {
		r.add(item.timestamp, item.totalRU, item.execCount, item.execDuration)
	}
}

// ruRecords is a sortable list of ruRecord pointers, sorted by totalRU (desc).
type ruRecords []*ruRecord

func (rs ruRecords) Len() int           { return len(rs) }
func (rs ruRecords) Less(i, j int) bool { return rs[i].totalRU > rs[j].totalRU } // DESC
func (rs ruRecords) Swap(i, j int)      { rs[i], rs[j] = rs[j], rs[i] }

// topN returns top n records by totalRU and the evicted records.
func (rs ruRecords) topN(n int) (top, evicted ruRecords) {
	if len(rs) <= n {
		return rs, nil
	}
	if err := quickselect.QuickSelect(rs, n); err != nil {
		return rs, nil
	}
	return rs[:n], rs[n:]
}

// userRUCollecting tracks RU data for a single user with per-SQL TopN.
type userRUCollecting struct {
	user    string
	records map[string]*ruRecord // sqlPlanKey => ruRecord
	keyBuf  *bytes.Buffer
	totalRU float64 // cumulative RU for user-level TopN sorting
}

func newUserRUCollecting(user string) *userRUCollecting {
	return &userRUCollecting{
		user:    user,
		records: make(map[string]*ruRecord),
		keyBuf:  bytes.NewBuffer(make([]byte, 0, 64)),
	}
}

// add adds RU increment data for a specific SQL.
func (u *userRUCollecting) add(timestamp uint64, sqlDigest, planDigest []byte, totalRU float64, execCount, execDuration uint64) {
	key := encodeKey(u.keyBuf, sqlDigest, planDigest)
	rec, ok := u.records[key]
	if !ok {
		rec = newRURecord(sqlDigest, planDigest)
		u.records[key] = rec
	}
	rec.add(timestamp, totalRU, execCount, execDuration)
	u.totalRU += totalRU
}

// getReportRecords returns TopN SQL records for this user, with evicted SQLs merged into "others".
// Returns a slice of ruRecord ready for proto conversion.
func (u *userRUCollecting) getReportRecords() []*ruRecord {
	if len(u.records) == 0 {
		return nil
	}

	// Extract all records
	allRecords := make(ruRecords, 0, len(u.records))
	for _, rec := range u.records {
		allRecords = append(allRecords, rec)
	}

	// Apply TopN filtering
	top, evicted := allRecords.topN(maxTopSQLsPerUser)

	// Merge evicted into "others SQL"
	if len(evicted) > 0 {
		othersRec := newRURecord(nil, nil) // nil digests = "others SQL"
		for _, rec := range evicted {
			othersRec.merge(rec)
		}
		top = append(top, othersRec)
	}

	return top
}

// userRUCollectings is a sortable list of userRUCollecting pointers, sorted by totalRU (desc).
type userRUCollectings []*userRUCollecting

func (us userRUCollectings) Len() int           { return len(us) }
func (us userRUCollectings) Less(i, j int) bool { return us[i].totalRU > us[j].totalRU } // DESC
func (us userRUCollectings) Swap(i, j int)      { us[i], us[j] = us[j], us[i] }

// topN returns top n users by totalRU and the evicted users.
func (us userRUCollectings) topN(n int) (top, evicted userRUCollectings) {
	if len(us) <= n {
		return us, nil
	}
	if err := quickselect.QuickSelect(us, n); err != nil {
		return us, nil
	}
	return us[:n], us[n:]
}

// ruCollecting is the top-level RU data collector implementing Hybrid TopN (Decision A).
// It maintains global TopN users, with per-user TopN SQLs.
type ruCollecting struct {
	users map[string]*userRUCollecting // user => userRUCollecting
}

func newRUCollecting() *ruCollecting {
	return &ruCollecting{
		users: make(map[string]*userRUCollecting),
	}
}

// add adds RU increment data from aggregator.
func (c *ruCollecting) add(timestamp uint64, key stmtstats.RUKey, incr *stmtstats.RUIncrement) {
	user := key.User
	userCollecting, ok := c.users[user]
	if !ok {
		userCollecting = newUserRUCollecting(user)
		c.users[user] = userCollecting
	}
	// Convert BinaryDigest (string) to []byte for storage
	userCollecting.add(timestamp, []byte(key.SQLDigest), []byte(key.PlanDigest), incr.TotalRU, incr.ExecCount, incr.ExecDuration)
}

// addBatch adds a batch of RU increments for a given timestamp.
func (c *ruCollecting) addBatch(timestamp uint64, increments stmtstats.RUIncrementMap) {
	for key, incr := range increments {
		c.add(timestamp, key, incr)
	}
}

// take takes all collected data and returns a new ruCollecting, resetting internal state.
func (c *ruCollecting) take() *ruCollecting {
	result := &ruCollecting{
		users: c.users,
	}
	c.users = make(map[string]*userRUCollecting)
	return result
}

// getReportRecords applies two-level TopN filtering and returns records ready for reporting.
// Level 1: Global TopN users (200)
// Level 2: Per-user TopN SQLs (200)
// Evicted users are merged into "others user", evicted SQLs into "others SQL".
func (c *ruCollecting) getReportRecords(keyspaceName []byte) []tipb.TopRURecord {
	if len(c.users) == 0 {
		return nil
	}

	// Extract all users
	allUsers := make(userRUCollectings, 0, len(c.users))
	for _, userCollecting := range c.users {
		allUsers = append(allUsers, userCollecting)
	}

	// Apply global TopN user filtering
	topUsers, evictedUsers := allUsers.topN(maxTopUsers)

	// Collect records from top users
	var result []tipb.TopRURecord
	for _, userCollecting := range topUsers {
		userRecords := userCollecting.getReportRecords()
		for _, rec := range userRecords {
			// Sort items by timestamp before converting to proto
			sort.Sort(rec.items)
			result = append(result, tipb.TopRURecord{
				KeyspaceName: keyspaceName,
				User:         userCollecting.user,
				SqlDigest:    rec.sqlDigest,
				PlanDigest:   rec.planDigest,
				Items:        rec.items.toProto(),
			})
		}
	}

	// Merge evicted users into "others user"
	if len(evictedUsers) > 0 {
		othersUser := newUserRUCollecting(keyRUOthersUser)
		for _, evictedUser := range evictedUsers {
			// Merge all SQLs from evicted user into others user's "others SQL"
			for _, rec := range evictedUser.records {
				for _, item := range rec.items {
					othersUser.add(item.timestamp, nil, nil, item.totalRU, item.execCount, item.execDuration)
				}
			}
		}
		// Get the single "others SQL" record from "others user"
		othersRecords := othersUser.getReportRecords()
		for _, rec := range othersRecords {
			sort.Sort(rec.items)
			result = append(result, tipb.TopRURecord{
				KeyspaceName: keyspaceName,
				User:         keyRUOthersUser,
				SqlDigest:    nil,
				PlanDigest:   nil,
				Items:        rec.items.toProto(),
			})
		}
	}

	return result
}

// encodeRUKey encodes user + sqlDigest + planDigest into a string key.
func encodeRUKey(buf *bytes.Buffer, user string, sqlDigest, planDigest []byte) string {
	buf.Reset()
	buf.WriteString(user)
	buf.WriteByte(0) // separator
	buf.Write(sqlDigest)
	buf.Write(planDigest)
	return buf.String()
}

// RUKeyString returns a string representation of RUKey for map indexing.
func RUKeyString(key stmtstats.RUKey) string {
	return key.User + string(rune(0)) + string(key.SQLDigest) + string(key.PlanDigest)
}
