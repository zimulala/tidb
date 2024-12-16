package main

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/tikv/client-go/v2/util"
)

const (
	createTableSQL = `CREATE TABLE SLOW_QUERY (
  Query_Time double DEFAULT NULL,
  Parse_time double DEFAULT NULL,
  Compile_time double DEFAULT NULL,
  Optimize_time double DEFAULT NULL,
  Query text DEFAULT NULL
);`
	insertSQL = `insert into SLOW_QUERY(
		Query_Time, Parse_time, Compile_time, Optimize_time, Query) values (?, ?, ?, ?, ?)`
)

func normalSimpleWR(db *sql.DB) {
	_, err := db.Exec("CREATE TABLE users (id INTEGER, name STRING);")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');")
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("SELECT id, name FROM users;")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("Create the users table and the data:")
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("ID: %d, Name: %s\n", id, name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func readFileWR(db *sql.DB, fileName string) {
	_, err := db.Exec(fmt.Sprintf("CREATE TEMPORARY table json_data AS SELECT * FROM read_json_auto('%s');", fileName))
	// _, err := db.Exec(fmt.Sprintf("CREATE TABLE json_data AS SELECT * FROM read_json_auto('%s');", fileName))
	if err != nil {
		log.Fatalf("Failed to load JSON file: %v", err)
	}

	rows, err := db.Query(`SELECT
			Name,
			SpanContext.TraceID,
			SpanContext.SpanID,
			Parent.TraceID,
			StartTime,
			SpanKind,
			Events[1].Name AS FirstEventName,
			Events[1].Attributes[1].Key AS FirstEventFirstAttributeKey,
			Events[1].Attributes[1].Value.Value AS FirstEventFirstAttributeValue,
			Resource[1].Key AS FirstResourceKey,
			Resource[1].Value.Value AS FirstResourceKey,
			InstrumentationScope.Name
		FROM json_data;`)
	if err != nil {
		log.Fatalf("Failed to query JSON data: %v", err)
	}
	defer rows.Close()

	fmt.Println("Read JSON file to DB, results:")
	for rows.Next() {
		var name, traceID, spanID, pTraceID, startTime, firstEventName, firstAttrKey, firstAttrValue, firstResourceKey, firstResourceValue, instrumentationScope string
		var spanKind int
		err := rows.Scan(&name, &traceID, &spanID, &pTraceID, &startTime, &spanKind, &firstEventName, &firstAttrKey, &firstAttrValue, &firstResourceKey, &firstResourceValue, &instrumentationScope)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("Name: %s, TraceID: %s, SpanID: %s, pTraceID: %s, StartTime: %s, SpanKind: %d, FirstEventName: %s, FirstAttrKey: %s, FirstAttrValue: %s, FirstResourceKey: %s, FirstResourceValue: %s, InstrumentationScope: %s\n",
			name, hexEncode(traceID), spanID, pTraceID, startTime, spanKind, firstEventName, firstAttrKey, firstAttrValue, firstResourceKey, firstResourceValue, instrumentationScope)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Row iteration error: %v", err)
	}
}

func main() {
	// connect DuckDB
	// .open /Users/xia/workspace/src/github.com/pingcap/tidb/pkg/util/duckdb/my_database.db
	// Persist to local file
	// db, err := sql.Open("duckdb", "my_database.db")
	// In memory
	// db, err := sql.Open("duckdb", ":memory:")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// // for simple demo
	// normalSimpleWR(db)

	// for read data from local file
	readFileWR(db, "/Users/xia/workspace/src/github.com/pingcap/tidb/data.json")

	// // for write data from memory to duckDB
	// _, err = db.Exec(createTableSQL)
	// if err != nil {
	// 	log.Fatalf("Failed to create table: %v", err)
	// }
	// slowLogItems := mockGenLogItems()
	// _, err = db.Exec(insertSQL, slowLogItems.TimeTotal,
	// 	slowLogItems.TimeParse, slowLogItems.TimeCompile, slowLogItems.TimeOptimize, slowLogItems.SQL)
	// if err != nil {
	// 	log.Fatalf("Failed to insert trace data: %v", err)
	// }

	// rows, err := db.Query(`SELECT * FROM SLOW_QUERY;`)
	// if err != nil {
	// 	log.Fatalf("Failed to query JSON data: %v", err)
	// }
	// defer rows.Close()

	// fmt.Println("Write data from memory to duckDB, results:")
	// for rows.Next() {
	// 	var timeTotal, timeParse, timeCompile, timeOptimize, sql string
	// 	err := rows.Scan(&timeTotal, &timeParse, &timeCompile, &timeOptimize, &sql)
	// 	if err != nil {
	// 		log.Fatalf("Failed to scan row: %v", err)
	// 	}
	// 	fmt.Printf("timeTotal: %s, timeParse: %s, timeCompile: %s, timeOptimize: %s, sql: %s\n",
	// 		timeTotal, timeParse, timeCompile, timeOptimize, sql)
	// }

	// if err := rows.Err(); err != nil {
	// 	log.Fatalf("Row iteration error: %v", err)
	// }
}

func hexEncode(input string) string {
	return hex.EncodeToString([]byte(input))
}

func mockGenLogItems() *variable.SlowQueryLogItems {
	ctx := mock.NewContext()
	seVar := ctx.GetSessionVars()

	seVar.User = &auth.UserIdentity{Username: "root", Hostname: "192.168.0.1"}
	seVar.ConnectionInfo = &variable.ConnectionInfo{ClientIP: "192.168.0.1"}
	seVar.ConnectionID = 1
	seVar.SessionAlias = "aliasabc"
	// the output of the logged CurrentDB should be 'test', should be to lower cased.
	seVar.CurrentDB = "TeST"
	seVar.InRestrictedSQL = true
	seVar.StmtCtx.WaitLockLeaseTime = 1
	txnTS := uint64(406649736972468225)
	costTime := time.Second
	execDetail := execdetails.ExecDetails{
		BackoffTime:  time.Millisecond,
		RequestCount: 2,
		ScanDetail: &util.ScanDetail{
			ProcessedKeys: 20001,
			TotalKeys:     10000,
		},
		DetailsNeedP90: execdetails.DetailsNeedP90{
			TimeDetail: util.TimeDetail{
				ProcessTime: time.Second * time.Duration(2),
				WaitTime:    time.Minute,
			},
		},
	}
	usedStats1 := &stmtctx.UsedStatsInfoForTable{
		Name:                  "t1",
		TblInfo:               nil,
		Version:               123,
		RealtimeCount:         1000,
		ModifyCount:           0,
		ColumnStatsLoadStatus: map[int64]string{2: "allEvicted", 3: "onlyCmsEvicted"},
		IndexStatsLoadStatus:  map[int64]string{1: "allLoaded", 2: "allLoaded"},
	}
	usedStats2 := &stmtctx.UsedStatsInfoForTable{
		Name:                  "t2",
		TblInfo:               nil,
		Version:               0,
		RealtimeCount:         10000,
		ModifyCount:           0,
		ColumnStatsLoadStatus: map[int64]string{2: "unInitialized"},
	}

	copTasks := &execdetails.CopTasksDetails{
		NumCopTasks:       10,
		AvgProcessTime:    time.Second,
		P90ProcessTime:    time.Second * 2,
		MaxProcessAddress: "10.6.131.78",
		MaxProcessTime:    time.Second * 3,
		AvgWaitTime:       time.Millisecond * 10,
		P90WaitTime:       time.Millisecond * 20,
		MaxWaitTime:       time.Millisecond * 30,
		MaxWaitAddress:    "10.6.131.79",
		MaxBackoffTime:    make(map[string]time.Duration),
		AvgBackoffTime:    make(map[string]time.Duration),
		P90BackoffTime:    make(map[string]time.Duration),
		TotBackoffTime:    make(map[string]time.Duration),
		TotBackoffTimes:   make(map[string]int),
		MaxBackoffAddress: make(map[string]string),
	}

	backoffs := []string{"rpcTiKV", "rpcPD", "regionMiss"}
	for _, backoff := range backoffs {
		copTasks.MaxBackoffTime[backoff] = time.Millisecond * 200
		copTasks.MaxBackoffAddress[backoff] = "127.0.0.1"
		copTasks.AvgBackoffTime[backoff] = time.Millisecond * 200
		copTasks.P90BackoffTime[backoff] = time.Millisecond * 200
		copTasks.TotBackoffTime[backoff] = time.Millisecond * 200
		copTasks.TotBackoffTimes[backoff] = 200
	}

	var memMax int64 = 2333
	var diskMax int64 = 6666
	sql := "select * from t;"
	_, digest := parser.NormalizeDigest(sql)
	logItems := &variable.SlowQueryLogItems{
		TxnTS:             txnTS,
		KeyspaceName:      "keyspace_a",
		KeyspaceID:        1,
		SQL:               sql,
		Digest:            digest.String(),
		TimeTotal:         costTime,
		TimeParse:         time.Duration(10),
		TimeCompile:       time.Duration(10),
		TimeOptimize:      time.Duration(10),
		TimeWaitTS:        time.Duration(3),
		IndexNames:        "[t1:a,t2:b]",
		CopTasks:          copTasks,
		ExecDetail:        execDetail,
		MemMax:            memMax,
		DiskMax:           diskMax,
		Prepared:          true,
		PlanFromCache:     true,
		PlanFromBinding:   true,
		HasMoreResults:    true,
		KVTotal:           10 * time.Second,
		PDTotal:           11 * time.Second,
		BackoffTotal:      12 * time.Second,
		WriteSQLRespTotal: 1 * time.Second,
		ResultRows:        12345,
		Succ:              true,
		RewriteInfo: variable.RewritePhaseInfo{
			DurationRewrite:            3,
			DurationPreprocessSubQuery: 2,
			PreprocessSubQueries:       2,
		},
		ExecRetryCount:    3,
		ExecRetryTime:     5*time.Second + time.Millisecond*100,
		IsExplicitTxn:     true,
		IsWriteCacheTable: true,
		UsedStats:         &stmtctx.UsedStatsInfo{},
		ResourceGroupName: "rg1",
		RRU:               50.0,
		WRU:               100.56,
		WaitRUDuration:    134 * time.Millisecond,
	}
	logItems.UsedStats.RecordUsedInfo(1, usedStats1)
	logItems.UsedStats.RecordUsedInfo(2, usedStats2)
	return logItems
}
