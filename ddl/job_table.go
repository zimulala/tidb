// Copyright 2022 PingCAP, Inc.
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

package ddl

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/ddl/ingest"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/ddl/util/gpool/spmc"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var (
	addingDDLJobConcurrent = "/tidb/ddl/add_ddl_job_general"
	addingBackfillJob      = "/tidb/ddl/add_backfill_job"
)

func (dc *ddlCtx) insertRunningDDLJobMap(id int64) {
	dc.runningJobs.Lock()
	defer dc.runningJobs.Unlock()
	dc.runningJobs.ids[id] = struct{}{}
}

func (dc *ddlCtx) deleteRunningDDLJobMap(id int64) {
	dc.runningJobs.Lock()
	defer dc.runningJobs.Unlock()
	delete(dc.runningJobs.ids, id)
}

func (dc *ddlCtx) excludeJobIDs() string {
	dc.runningJobs.RLock()
	defer dc.runningJobs.RUnlock()
	if len(dc.runningJobs.ids) == 0 {
		return ""
	}
	dc.runningJobIDs = dc.runningJobIDs[:0]
	for id := range dc.runningJobs.ids {
		dc.runningJobIDs = append(dc.runningJobIDs, strconv.Itoa(int(id)))
	}
	return fmt.Sprintf("and job_id not in (%s)", strings.Join(dc.runningJobIDs, ","))
}

const (
	getJobSQL = "select job_meta, processing from mysql.tidb_ddl_job where job_id in (select min(job_id) from mysql.tidb_ddl_job group by schema_ids, table_ids) and %s reorg %s order by processing desc, job_id"
)

type jobType int

func (t jobType) String() string {
	switch t {
	case general:
		return "general"
	case reorg:
		return "reorg"
	}
	return "unknown job type: " + strconv.Itoa(int(t))
}

const (
	general jobType = iota
	reorg
)

func (d *ddl) getJob(sess *session, tp jobType, filter func(*model.Job) (bool, error)) (*model.Job, error) {
	not := "not"
	label := "get_job_general"
	if tp == reorg {
		not = ""
		label = "get_job_reorg"
	}
	sql := fmt.Sprintf(getJobSQL, not, d.excludeJobIDs())
	rows, err := sess.execute(context.Background(), sql, label)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		runJob := model.Job{}
		err := runJob.Decode(jobBinary)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row.GetInt64(1) == 1 {
			return &runJob, nil
		}
		b, err := filter(&runJob)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if b {
			if err := d.markJobProcessing(sess, &runJob); err != nil {
				logutil.BgLogger().Warn("[ddl] handle ddl job failed: mark job is processing meet error", zap.Error(err), zap.String("job", runJob.String()))
				return nil, errors.Trace(err)
			}
			return &runJob, nil
		}
	}
	return nil, nil
}

func (d *ddl) getGeneralJob(sess *session) (*model.Job, error) {
	return d.getJob(sess, general, func(job *model.Job) (bool, error) {
		if job.Type == model.ActionDropSchema {
			// Check if there is any reorg job on this schema.
			sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where CONCAT(',', schema_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and processing limit 1", strconv.Quote(strconv.FormatInt(job.SchemaID, 10)))
			return d.checkJobIsRunnable(sess, sql)
		}
		// Check if there is any running job works on the same table.
		sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job t1, (select table_ids from mysql.tidb_ddl_job where job_id = %d) t2 where "+
			"(processing and CONCAT(',', t2.table_ids, ',') REGEXP CONCAT(',', REPLACE(t1.table_ids, ',', '|'), ',') != 0)"+
			"or (type = %d and processing)", job.ID, model.ActionFlashbackCluster)
		return d.checkJobIsRunnable(sess, sql)
	})
}

func (d *ddl) checkJobIsRunnable(sess *session, sql string) (bool, error) {
	rows, err := sess.execute(context.Background(), sql, "check_runnable")
	return len(rows) == 0, err
}

func (d *ddl) getReorgJob(sess *session) (*model.Job, error) {
	return d.getJob(sess, reorg, func(job *model.Job) (bool, error) {
		// Check if there is any block ddl running, like drop schema and flashback cluster.
		sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where "+
			"(CONCAT(',', schema_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and type = %d and processing) "+
			"or (CONCAT(',', table_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and processing) "+
			"or (type = %d and processing) limit 1",
			strconv.Quote(strconv.FormatInt(job.SchemaID, 10)), model.ActionDropSchema, strconv.Quote(strconv.FormatInt(job.TableID, 10)), model.ActionFlashbackCluster)
		return d.checkJobIsRunnable(sess, sql)
	})
}

func (d *ddl) startDispatchBackfillJobsLoop() {
	if !IsDistReorgEnable() {
		return
	}
	d.backfillCtx.jobCtxMap = make(map[int64]*JobContext)
	d.backfillCtx.backfillCtxMap = make(map[int64]struct{})

	logutil.BgLogger().Warn("------------------------------- start backfill jobs loop")

	var notifyBackfillJobByEtcdCh clientv3.WatchChan
	if d.etcdCli != nil {
		notifyBackfillJobByEtcdCh = d.etcdCli.Watch(d.ctx, addingBackfillJob)
	}
	// TODO: set the tick time
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	for {
		if isChanClosed(d.ctx.Done()) {
			return
		}
		select {
		case <-d.backfillJobCh:
		case <-ticker.C:
		case _, ok := <-notifyBackfillJobByEtcdCh:
			if !ok {
				logutil.BgLogger().Warn("[ddl] start backfill worker watch channel closed", zap.String("watch key", addingBackfillJob))
				notifyBackfillJobByEtcdCh = d.etcdCli.Watch(d.ctx, addingBackfillJob)
				time.Sleep(time.Second)
				continue
			}
		case <-d.ctx.Done():
			return
		}
		d.loadBackfillJobAndRun()
	}
}

func (d *ddl) getTableByTxn(store kv.Storage, schemaID, tableID int64) (*model.DBInfo, table.Table, error) {
	var tbl table.Table
	var dbInfo *model.DBInfo
	err := kv.RunInNewTxn(d.ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := newMetaWithQueueTp(txn, addIdxWorker)
		var err1 error
		dbInfo, err1 = t.GetDatabase(schemaID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		tblInfo, err1 := getTableInfo(t, tableID, schemaID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		tbl, err1 = getTable(store, schemaID, tblInfo)
		return errors.Trace(err1)
	})
	return dbInfo, tbl, err
}

func (d *ddl) loadBackfillJobAndRun() {
	se, err := d.sessPool.get()
	defer d.sessPool.put(se)
	if err != nil {
		logutil.BgLogger().Fatal("dispatch backfill jobs loop get session failed, it should not happen, please try restart TiDB", zap.Error(err))
	}
	sess := NewSession(se)

	if err := ddlutil.LoadDDLReorgVars(context.Background(), sess); err != nil {
		logutil.BgLogger().Error("[ddl] load DDL reorganization variable failed", zap.Error(err))
	}

	d.backfillCtx.Lock()
	jobCtxMapLen := len(d.backfillCtx.jobCtxMap)
	runningJobIDs := make([]int64, 0, jobCtxMapLen)
	if jobCtxMapLen >= reorgWorkerCnt {
		logutil.BgLogger().Warn("00 ******** load backfill job and run reorg jos is more than limit", zap.Int("limit", reorgWorkerCnt))
		d.backfillCtx.Unlock()
		return
	} else {
		for id, _ := range d.backfillCtx.jobCtxMap {
			runningJobIDs = append(runningJobIDs, id)
		}
	}
	d.backfillCtx.Unlock()

	// TODO: Add ele info to distinguish backfill jobs.
	isIncluded := false
	bJobs, err := GetBackfillJobsForOneEle(sess, 1, isIncluded, runningJobIDs, InstanceLease*time.Second)
	bJobCnt := len(bJobs)
	if bJobCnt == 0 || err != nil {
		if err != nil {
			logutil.BgLogger().Warn("[ddl] get backfill jobs met error", zap.Error(err))
		}
		return
	}

	bJob := bJobs[0]
	d.backfillCtx.Lock()
	jobCtx, ok := d.backfillCtx.jobCtxMap[bJob.JobID]
	if !ok {
		jobCtx = NewJobContextWithArgs(bJob.Mate.Query)
		d.backfillCtx.jobCtxMap[bJob.JobID] = jobCtx
		d.backfillCtx.Unlock()
	} else {
		logutil.BgLogger().Warn("00 ******** load backfill job and run reorg jobs exit", zap.Int64("job id", bJob.JobID))
		d.backfillCtx.Unlock()
		return
	}

	d.wg.Run(func() {
		defer func() {
			d.backfillCtx.Lock()
			delete(d.backfillCtx.jobCtxMap, bJob.JobID)
			d.backfillCtx.Unlock()
			logutil.BgLogger().Warn("00 ******** load backfill job and run reorg jobs finished", zap.Int64("job id", bJob.JobID))
		}()

		// TODO: Consider redo it.
		bc, ok := ingest.LitBackCtxMgr.Load(bJob.JobID)
		if ok && bc.Done() {
			logutil.BgLogger().Warn("[ddl] lightning loaded")
			return
		}
		// if !ok && job.SnapshotVer != 0 {
		// The owner is crashed or changed, we need to restart the backfill.
		// job.SnapshotVer = 0
		// return
		// }
		// TODO: set indexInfo.Unique
		isUnique := false
		bc, err = ingest.LitBackCtxMgr.Register(d.ctx, isUnique, bJob.JobID, bJob.Mate.SQLMode)
		if err != nil {
			//tryFallbackToTxnMerge(job, err)
			logutil.BgLogger().Warn("[ddl] lightning register error", zap.Error(err))
			return
		}

		logutil.BgLogger().Warn("00 ******** load backfill job and run reorg jobs start", zap.Int64("job id", bJob.JobID))
		tbl, err := d.runBackfillJobs(sess, bJob, jobCtx)
		if err != nil {
			logutil.BgLogger().Warn("[ddl] runBackfillJobs error", zap.Error(err))
			ingest.LitBackCtxMgr.Unregister(bJob.JobID)
			// tryFallbackToTxnMerge(job, err)
			return
		}

		finish := injectSpan(bJob.JobID, "finish-import")
		err = bc.FinishImport(bJob.EleID, isUnique, tbl)
		if err != nil {
			if kv.ErrKeyExists.Equal(err) {
				logutil.BgLogger().Warn("[ddl] import index duplicate key, convert job to rollback", zap.String("job", bJob.AbbrStr()), zap.Error(err))
				// ver, err = convertAddIdxJob2RollbackJob(d, t, job, tbl.Meta(), indexInfo, err)
			} else {
				logutil.BgLogger().Warn("[ddl] lightning import error", zap.Error(err))
				// tryFallbackToTxnMerge(job, err)
			}
			ingest.LitBackCtxMgr.Unregister(bJob.JobID)
			return
		}
		ingest.LitBackCtxMgr.Unregister(bJob.ID)
		finish()
		bc.SetDone()

		err = syncBackfillHistoryJobs(sess, d.uuid, bJob)
		if err != nil {
			logutil.BgLogger().Warn("[ddl] syncBackfillHistoryJobs error", zap.Error(err))
		}
		details := collectTrace(bJob.JobID)
		logutil.BgLogger().Info("[ddl] &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&--------------------------  finish backfill jobs",
			zap.Int64("job ID", bJob.JobID), zap.String("time details", details))
	})
}

func (d *ddl) runBackfillJobs(sess *session, bJob *BackfillJob, jobCtx *JobContext) (table.Table, error) {
	traceID := bJob.ID + 100
	initializeTrace(traceID)
	dbInfo, tbl, err := d.getTableByTxn(d.store, bJob.Mate.SchemaID, bJob.Mate.TableID)
	if err != nil {
		logutil.BgLogger().Warn("[ddl] backfill job get table failed", zap.String("bfJob", bJob.AbbrStr()), zap.Error(err))
		return nil, err
	}

	workerCnt := int(variable.GetDDLReorgWorkerCounter())
	batch := int(variable.GetDDLReorgBatchSize())
	bwCtx, err := newBackfillWorkerContext(d, dbInfo.Name.O, tbl, jobCtx, bJob, workerCnt, batch)
	if err != nil {
		return nil, err
	}
	d.backfillWorkerPool.SetConsumerFunc(func(task *reorgBackfillTask, _ int, bfWorker *backfillWorker) *backfillResult {
		// To prevent different workers from using the same session.
		// TODO: backfillWorkerPool is global, and bfWorkers is used in this function, we'd better do something make worker and job's ID can be matched.
		defer injectSpan(traceID, fmt.Sprintf("run-task-id-%d", task.bfJob.ID))()

		bfWorker.runTask(task)
		ret := <-bfWorker.resultCh
		if dbterror.ErrDDLJobNotFound.Equal(ret.err) {
			logutil.BgLogger().Info("the backfill job instance ID or lease is changed", zap.Error(ret.err))
			ret.err = nil
		}

		return ret
	})

	runningJobIDs := []int64{bJob.JobID}
	num := 0
	proFunc := func() ([]*reorgBackfillTask, error) {
		defer injectSpan(traceID, fmt.Sprintf("get-backfill-jobs-no.%d", num))
		num++
		// TODO: if err is write conflict, we need retry
		// TODO: workerCnt -> batch
		bJobs, err := GetAndMarkBackfillJobsForOneEle(sess, workerCnt*2, true, runningJobIDs, d.uuid, InstanceLease*time.Second)
		if err != nil {
			// TODO: test: if all tidbs can't get the unmark backfill job(a tidb mark a backfill job, other tidbs returned, then the tidb can't handle this job.)
			if dbterror.ErrDDLJobNotFound.Equal(err) {
				logutil.BgLogger().Info("no backfill job, handle backfill task finished")
				return nil, err
			}
			// TODO: retry get backfill jobs
		}
		tasks := make([]*reorgBackfillTask, 0, len(bJobs))
		for _, bJ := range bJobs {
			task := d.backfillJob2Task(tbl, bJ)
			tasks = append(tasks, task)
		}
		return tasks, nil
	}
	// add new task
	resultCh, control := d.backfillWorkerPool.AddProduceBySlice(proFunc, 0, bwCtx, spmc.WithConcurrency(workerCnt))

	var wg util.WaitGroupWrapper
	exitCh := make(chan struct{})
	wg.Run(func() {
		for {
			select {
			case result := <-resultCh:
				if result.err != nil {
					logutil.BgLogger().Warn("handle backfill task failed", zap.Error(result.err))
					return
				}
			case <-exitCh:
				return
			}
		}
	})

	// Waiting task finishing
	control.Wait()
	close(exitCh)
	wg.Wait()

	for _, s := range bwCtx.sessCtxs {
		d.sessPool.put(s)
	}
	for _, w := range bwCtx.backfillWorkers {
		d.backfillCtxPool.put(w)
	}
	return tbl, nil
}

func (d *ddl) startDispatchLoop() {
	se, err := d.sessPool.get()
	if err != nil {
		logutil.BgLogger().Fatal("dispatch loop get session failed, it should not happen, please try restart TiDB", zap.Error(err))
	}
	defer d.sessPool.put(se)
	sess := NewSession(se)
	var notifyDDLJobByEtcdCh clientv3.WatchChan
	if d.etcdCli != nil {
		notifyDDLJobByEtcdCh = d.etcdCli.Watch(d.ctx, addingDDLJobConcurrent)
	}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		if isChanClosed(d.ctx.Done()) {
			return
		}
		if !variable.EnableConcurrentDDL.Load() || !d.isOwner() || d.waiting.Load() {
			d.once.Store(true)
			time.Sleep(time.Second)
			continue
		}
		select {
		case <-d.ddlJobCh:
		case <-ticker.C:
		case _, ok := <-notifyDDLJobByEtcdCh:
			if !ok {
				logutil.BgLogger().Warn("[ddl] start worker watch channel closed", zap.String("watch key", addingDDLJobConcurrent))
				notifyDDLJobByEtcdCh = d.etcdCli.Watch(d.ctx, addingDDLJobConcurrent)
				time.Sleep(time.Second)
				continue
			}
		case <-d.ctx.Done():
			return
		}
		d.loadDDLJobAndRun(sess, d.generalDDLWorkerPool, d.getGeneralJob)
		d.loadDDLJobAndRun(sess, d.reorgWorkerPool, d.getReorgJob)
	}
}

func (d *ddl) loadDDLJobAndRun(sess *session, pool *workerPool, getJob func(*session) (*model.Job, error)) {
	wk, err := pool.get()
	if err != nil || wk == nil {
		logutil.BgLogger().Debug(fmt.Sprintf("[ddl] no %v worker available now", pool.tp()), zap.Error(err))
		return
	}

	d.mu.RLock()
	d.mu.hook.OnGetJobBefore(pool.tp().String())
	d.mu.RUnlock()

	job, err := getJob(sess)
	if job == nil || err != nil {
		if err != nil {
			logutil.BgLogger().Warn("[ddl] get job met error", zap.Error(err))
		}
		pool.put(wk)
		return
	}
	d.mu.RLock()
	d.mu.hook.OnGetJobAfter(pool.tp().String(), job)
	d.mu.RUnlock()

	d.delivery2worker(wk, pool, job)
}

// delivery2worker owns the worker, need to put it back to the pool in this function.
func (d *ddl) delivery2worker(wk *worker, pool *workerPool, job *model.Job) {
	injectFailPointForGetJob(job)
	d.insertRunningDDLJobMap(job.ID)
	d.wg.Run(func() {
		metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Inc()
		defer func() {
			d.deleteRunningDDLJobMap(job.ID)
			asyncNotify(d.ddlJobCh)
			metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Dec()
		}()
		// check if this ddl job is synced to all servers.
		if !d.isSynced(job) || d.once.Load() {
			if variable.EnableMDL.Load() {
				exist, err := checkMDLInfo(job.ID, d.sessPool)
				if err != nil {
					logutil.BgLogger().Warn("[ddl] check MDL info failed", zap.Error(err), zap.String("job", job.String()))
					// Release the worker resource.
					pool.put(wk)
					return
				} else if exist {
					// Release the worker resource.
					pool.put(wk)
					err = waitSchemaSynced(d.ddlCtx, job, 2*d.lease)
					if err != nil {
						logutil.BgLogger().Warn("[ddl] wait ddl job sync failed", zap.Error(err), zap.String("job", job.String()))
						time.Sleep(time.Second)
						return
					}
					d.once.Store(false)
					cleanMDLInfo(d.sessPool, job.ID)
					// Don't have a worker now.
					return
				}
			} else {
				err := waitSchemaSynced(d.ddlCtx, job, 2*d.lease)
				if err != nil {
					logutil.BgLogger().Warn("[ddl] wait ddl job sync failed", zap.Error(err), zap.String("job", job.String()))
					time.Sleep(time.Second)
					// Release the worker resource.
					pool.put(wk)
					return
				}
				d.once.Store(false)
			}
		}

		schemaVer, err := wk.HandleDDLJobTable(d.ddlCtx, job)
		pool.put(wk)
		if err != nil {
			logutil.BgLogger().Info("[ddl] handle ddl job failed", zap.Error(err), zap.String("job", job.String()))
		} else {
			failpoint.Inject("mockDownBeforeUpdateGlobalVersion", func(val failpoint.Value) {
				if val.(bool) {
					if mockDDLErrOnce == 0 {
						mockDDLErrOnce = schemaVer
						failpoint.Return()
					}
				}
			})

			// Here means the job enters another state (delete only, write only, public, etc...) or is cancelled.
			// If the job is done or still running or rolling back, we will wait 2 * lease time to guarantee other servers to update
			// the newest schema.
			waitSchemaChanged(context.Background(), d.ddlCtx, d.lease*2, schemaVer, job)
			cleanMDLInfo(d.sessPool, job.ID)
			d.synced(job)

			if RunInGoTest {
				// d.mu.hook is initialed from domain / test callback, which will force the owner host update schema diff synchronously.
				d.mu.RLock()
				d.mu.hook.OnSchemaStateChanged()
				d.mu.RUnlock()
			}

			d.mu.RLock()
			d.mu.hook.OnJobUpdated(job)
			d.mu.RUnlock()
		}
	})
}

func (d *ddl) markJobProcessing(sess *session, job *model.Job) error {
	sess.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	_, err := sess.execute(context.Background(), fmt.Sprintf("update mysql.tidb_ddl_job set processing = 1 where job_id = %d", job.ID), "mark_job_processing")
	return errors.Trace(err)
}

const (
	addDDLJobSQL    = "insert into mysql.tidb_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing) values"
	updateDDLJobSQL = "update mysql.tidb_ddl_job set job_meta = %s where job_id = %d"
)

func insertDDLJobs2Table(sess *session, updateRawArgs bool, jobs ...*model.Job) error {
	failpoint.Inject("mockAddBatchDDLJobsErr", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.Errorf("mockAddBatchDDLJobsErr"))
		}
	})
	if len(jobs) == 0 {
		return nil
	}
	var sql bytes.Buffer
	sql.WriteString(addDDLJobSQL)
	for i, job := range jobs {
		b, err := job.Encode(updateRawArgs)
		if err != nil {
			return err
		}
		if i != 0 {
			sql.WriteString(",")
		}
		sql.WriteString(fmt.Sprintf("(%d, %t, %s, %s, %s, %d, %t)", job.ID, job.MayNeedReorg(), strconv.Quote(job2SchemaIDs(job)), strconv.Quote(job2TableIDs(job)), wrapKey2String(b), job.Type, !job.NotStarted()))
	}
	sess.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	_, err := sess.execute(ctx, sql.String(), "insert_job")
	logutil.BgLogger().Debug("[ddl] add job to mysql.tidb_ddl_job table", zap.String("sql", sql.String()))
	return errors.Trace(err)
}

func job2SchemaIDs(job *model.Job) string {
	return job2UniqueIDs(job, true)
}

func job2TableIDs(job *model.Job) string {
	return job2UniqueIDs(job, false)
}

func job2UniqueIDs(job *model.Job, schema bool) string {
	switch job.Type {
	case model.ActionExchangeTablePartition, model.ActionRenameTables, model.ActionRenameTable:
		var ids []int64
		if schema {
			ids = job.CtxVars[0].([]int64)
		} else {
			ids = job.CtxVars[1].([]int64)
		}
		set := make(map[int64]struct{}, len(ids))
		for _, id := range ids {
			set[id] = struct{}{}
		}

		s := make([]string, 0, len(set))
		for id := range set {
			s = append(s, strconv.FormatInt(id, 10))
		}
		slices.Sort(s)
		return strings.Join(s, ",")
	}
	if schema {
		return strconv.FormatInt(job.SchemaID, 10)
	}
	return strconv.FormatInt(job.TableID, 10)
}

func (w *worker) deleteDDLJob(job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_job where job_id = %d", job.ID)
	_, err := w.sess.execute(context.Background(), sql, "delete_job")
	return errors.Trace(err)
}

func updateDDLJob2Table(sctx *session, job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(updateDDLJobSQL, wrapKey2String(b), job.ID)
	_, err = sctx.execute(context.Background(), sql, "update_job")
	return errors.Trace(err)
}

// getDDLReorgHandle gets DDL reorg handle.
func getDDLReorgHandle(sess *session, job *model.Job) (element *meta.Element, startKey, endKey kv.Key, physicalTableID int64, err error) {
	sql := fmt.Sprintf("select ele_id, ele_type, start_key, end_key, physical_id from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	rows, err := sess.execute(context.Background(), sql, "get_handle")
	if err != nil {
		return nil, nil, nil, 0, err
	}
	if len(rows) == 0 {
		return nil, nil, nil, 0, meta.ErrDDLReorgElementNotExist
	}
	id := rows[0].GetInt64(0)
	tp := rows[0].GetBytes(1)
	element = &meta.Element{
		ID:      id,
		TypeKey: tp,
	}
	startKey = rows[0].GetBytes(2)
	endKey = rows[0].GetBytes(3)
	physicalTableID = rows[0].GetInt64(4)
	// physicalTableID may be 0, because older version TiDB (without table partition) doesn't store them.
	// update them to table's in this case.
	if physicalTableID == 0 {
		if job.ReorgMeta != nil {
			endKey = kv.IntHandle(job.ReorgMeta.EndHandle).Encoded()
		} else {
			endKey = kv.IntHandle(math.MaxInt64).Encoded()
		}
		physicalTableID = job.TableID
		logutil.BgLogger().Warn("new TiDB binary running on old TiDB DDL reorg data",
			zap.Int64("partition ID", physicalTableID),
			zap.Stringer("startHandle", startKey),
			zap.Stringer("endHandle", endKey))
	}
	return
}

// updateDDLReorgStartHandle update the startKey of the handle.
func updateDDLReorgStartHandle(sess *session, job *model.Job, element *meta.Element, startKey kv.Key) error {
	sql := fmt.Sprintf("update mysql.tidb_ddl_reorg set ele_id = %d, ele_type = %s, start_key = %s where job_id = %d",
		element.ID, wrapKey2String(element.TypeKey), wrapKey2String(startKey), job.ID)
	_, err := sess.execute(context.Background(), sql, "update_start_handle")
	return err
}

// updateDDLReorgHandle update startKey, endKey physicalTableID and element of the handle.
func updateDDLReorgHandle(sess *session, jobID int64, startKey kv.Key, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	sql := fmt.Sprintf("update mysql.tidb_ddl_reorg set ele_id = %d, ele_type = %s, start_key = %s, end_key = %s, physical_id = %d where job_id = %d",
		element.ID, wrapKey2String(element.TypeKey), wrapKey2String(startKey), wrapKey2String(endKey), physicalTableID, jobID)
	_, err := sess.execute(context.Background(), sql, "update_handle")
	return err
}

// initDDLReorgHandle initializes the handle for ddl reorg.
func initDDLReorgHandle(sess *session, jobID int64, startKey kv.Key, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	sql := fmt.Sprintf("insert into mysql.tidb_ddl_reorg(job_id, ele_id, ele_type, start_key, end_key, physical_id) values (%d, %d, %s, %s, %s, %d)",
		jobID, element.ID, wrapKey2String(element.TypeKey), wrapKey2String(startKey), wrapKey2String(endKey), physicalTableID)
	_, err := sess.execute(context.Background(), sql, "update_handle")
	return err
}

// deleteDDLReorgHandle deletes the handle for ddl reorg.
func removeDDLReorgHandle(sess *session, job *model.Job, elements []*meta.Element) error {
	if len(elements) == 0 {
		return nil
	}
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	_, err := sess.execute(context.Background(), sql, "remove_handle")
	return err
}

// removeReorgElement removes the element from ddl reorg, it is the same with removeDDLReorgHandle, only used in failpoint
func removeReorgElement(sess *session, job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	_, err := sess.execute(context.Background(), sql, "remove_handle")
	return err
}

func AddBackfillJobs(sess *session, backfillJobs []*BackfillJob) error {
	sqlPrefix := "insert into mysql.tidb_ddl_backfill(section_id, job_id, ele_id, ele_key, store_id, type, exec_id, exec_lease, state, backfill_meta) values"
	var sql string
	// Add it for get StartTS.
	_, err := sess.execute(context.Background(), "begin", "add_backfill_jobs")
	if err != nil {
		return err
	}

	defer func() {
		if err == nil {
			_, err = sess.execute(context.Background(), "commit", "add_backfill_jobs")
		} else {
			_, err1 := sess.execute(context.Background(), "rollback", "add_backfill_jobs")
			if err1 != nil {
				logutil.BgLogger().Warn("[ddl] AddBackfillJobs rollback failed", zap.Error(err1))
			}
		}
	}()

	txn, err := sess.session().Txn(true)
	if err != nil {
		return err
	}
	startTS := txn.StartTS()
	for i, bj := range backfillJobs {
		bj.Mate.StartTS = startTS
		mateByte, err := bj.Mate.Encode()
		if err != nil {
			return err
		}

		if i == 0 {
			sql = sqlPrefix + fmt.Sprintf("(%d, %d, %d, '%s', %d, %d, '%s', '%s', %d, '%s')",
				bj.ID, bj.JobID, bj.EleID, bj.EleKey, bj.StoreID, bj.Tp, bj.Instance_ID, bj.Instance_Lease, bj.State, mateByte)
			continue
		}
		sql += fmt.Sprintf(", (%d, %d, %d, '%s', %d, %d, '%s', '%s', %d, '%s')",
			bj.ID, bj.JobID, bj.EleID, bj.EleKey, bj.StoreID, bj.Tp, bj.Instance_ID, bj.Instance_Lease, bj.State, mateByte)
	}
	_, err = sess.execute(context.Background(), sql, "add_backfill_jobs")
	logutil.BgLogger().Warn("insert *****************************   ", zap.String("sql", sql), zap.Error(err))

	return err
}

func GetBackfillJobsForOneEle(sess *session, batch int, isInclude bool, jobIDs []int64, lease time.Duration) ([]*BackfillJob, error) {
	currTime, err := GetOracleTime(sess.GetStore())
	if err != nil {
		return nil, err
	}
	jobInfo := ""
	symbol := "="
	if !isInclude {
		symbol = "!="
	}
	for _, id := range jobIDs {
		jobInfo += fmt.Sprintf(" and job_id %s %d", symbol, id)
	}

	jobs, err := GetBackfillJobs(sess, BackfillTable,
		fmt.Sprintf("exec_ID = '%s' or exec_lease < '%v' %s order by job_id limit %d", emptyInstance, currTime.Add(-lease), jobInfo, batch), "get_backfill_job")
	if err != nil || len(jobs) == 0 {
		return nil, err
	}
	validLen := 1
	firstJobID, firstEleID := jobs[0].JobID, jobs[0].EleID
	for i := 1; i < len(jobs); i++ {
		if jobs[i].JobID != firstJobID || jobs[i].EleID != firstEleID {
			break
		}
		validLen++
	}

	return jobs[:validLen], nil
}

func GetAndMarkBackfillJobsForOneEle(sess *session, batch int, isInclude bool, jobIDs []int64, uuid string, lease time.Duration) ([]*BackfillJob, error) {
	currTime, err := GetOracleTime(sess.GetStore())
	if err != nil {
		return nil, err
	}
	jobInfo := ""
	symbol := "="
	if !isInclude {
		symbol = "!="
	}
	for _, id := range jobIDs {
		jobInfo += fmt.Sprintf("and job_id %s %d", symbol, id)
	}

	_, err = sess.execute(context.Background(), "begin", "get_mark_backfill_job")
	if err != nil {
		return nil, err
	}

	defer func() {
		if err == nil {
			_, err = sess.execute(context.Background(), "commit", "get_mark_backfill_job")
			return
		}
		_, err1 := sess.execute(context.Background(), "rollback", "get_mark_backfill_job")
		if err1 != nil {
			logutil.BgLogger().Warn("[ddl] GetAndMarkBackfillJobsForOneEle rollback failed", zap.Error(err1))
		}
	}()

	jobs, err := GetBackfillJobs(sess, BackfillTable,
		fmt.Sprintf("exec_ID = '%s' or exec_lease < '%v' %s order by job_id limit %d", emptyInstance, currTime.Add(-lease), jobInfo, batch), "get_mark_backfill_job")
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, dbterror.ErrDDLJobNotFound.FastGen("get zero backfill job, lease is timeout")
	}

	validLen := 0
	firstJobID, firstEleID := jobs[0].JobID, jobs[0].EleID
	for i := 0; i < len(jobs); i++ {
		if jobs[i].JobID != firstJobID || jobs[i].EleID != firstEleID {
			break
		}
		validLen++

		jobs[i].Instance_ID = uuid
		jobs[i].Instance_Lease = GetLeaseGoTime(currTime, lease)
		// TODO: batch update
		if err = updateBackfillJob(sess, BackfillTable, jobs[i], "get_mark_backfill_job"); err != nil {
			return nil, err
		}
	}

	return jobs[:validLen], err
}

func syncBackfillHistoryJobs(sess *session, uuid string, backfillJob *BackfillJob) error {
	sql := fmt.Sprintf("update mysql.%s set state = %d where job_id = %d and exec_id = %d limit 1;",
		BackfillHistoryTable, model.JobStateSynced, backfillJob.JobID, uuid)
	logutil.BgLogger().Warn("update *****************************   " + fmt.Sprintf("sql:%v, sess:%#v", sql, sess))
	_, err := sess.execute(context.Background(), sql, "sync_backfill_history_job")
	return err
}

func getInterruptedBackfillJobsForOneEle(sess *session, jobID, eleID int64, eleKey []byte) ([]*BackfillJob, error) {
	jobs, err := GetBackfillJobs(sess, BackfillTable, fmt.Sprintf("job_id = %d and ele_id = %d and ele_key = '%s' and (state = %d or state = %d)",
		jobID, eleID, eleKey, model.JobStateRollingback, model.JobStateCancelling), "get_interrupt_backfill_job")
	if err != nil || len(jobs) == 0 {
		return nil, err
	}
	validLen := 1
	firstJobID, firstEleID := jobs[0].JobID, jobs[0].EleID
	for i := 1; i < len(jobs); i++ {
		if jobs[i].JobID != firstJobID || jobs[i].EleID != firstEleID {
			break
		}
		validLen++
	}

	return jobs[:validLen], nil
}

func GetBackfillJobCount(sess *session, tblName, condition string, label string) (int, error) {
	rows, err := sess.execute(context.Background(), fmt.Sprintf("select count(1) from mysql.%s where %s", tblName, condition), label)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) == 0 {
		return 0, dbterror.ErrDDLJobNotFound.FastGenByArgs(fmt.Sprintf("get wrong result cnt:%d", len(rows)))
	}

	logutil.BgLogger().Info(fmt.Sprintf("get *****************************  job cnt:%d, lable:%s, sql:%s", rows[0].GetInt64(0), label, condition))
	return int(rows[0].GetInt64(0)), nil
}

func GetBackfillJobs(sess *session, tblName, condition string, label string) ([]*BackfillJob, error) {
	logutil.BgLogger().Info(fmt.Sprintf("get ***************************** 00 table:%s, lable:%s, sql:%s", tblName, label, condition))
	rows, err := sess.execute(context.Background(), fmt.Sprintf("select * from mysql.%s where %s", tblName, condition), label)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := make([]*BackfillJob, 0, len(rows))
	for _, row := range rows {
		job := BackfillJob{
			ID:             row.GetInt64(0),
			JobID:          row.GetInt64(1),
			EleID:          row.GetInt64(2),
			EleKey:         row.GetBytes(3),
			StoreID:        row.GetInt64(4),
			Tp:             model.BackfillType(row.GetInt64(5)),
			Instance_ID:    row.GetString(6),
			Instance_Lease: row.GetTime(7),
			State:          model.JobState(row.GetInt64(8)),
		}
		job.Mate = &model.BackfillMeta{}
		err = job.Mate.Decode(row.GetBytes(9))
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, &job)
	}
	logutil.BgLogger().Info(fmt.Sprintf("get *****************************  job cnt:%d, lable:%s, sql:%s", len(rows), label, condition))
	return jobs, nil
}

func getUnsyncedInstanceIDs(sess *session, jobID int64, label string) ([]string, error) {
	sql := fmt.Sprintf("select sum(state = %d) as tmp, exec_id from mysql.tidb_ddl_backfill_history where job_id = %d group by exec_id having tmp = 0;",
		model.JobStateSynced, jobID)
	logutil.BgLogger().Info(fmt.Sprintf("get unsynced exec ID ***************************** 00 lable:%s, sql:%s", sql, label))
	rows, err := sess.execute(context.Background(), sql, label)
	if err != nil {
		return nil, errors.Trace(err)
	}
	InstanceIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		InstanceID := row.GetString(1)
		InstanceIDs = append(InstanceIDs, InstanceID)
	}
	return InstanceIDs, nil
}

func RemoveBackfillJob(sess *session, isAll bool, backfillJob *BackfillJob) error {
	sql := "delete from mysql.tidb_ddl_backfill"
	if !isAll {
		sql = fmt.Sprintf("delete from mysql.tidb_ddl_backfill where section_id = %d and job_id = %d and ele_id = %d",
			backfillJob.ID, backfillJob.JobID, backfillJob.EleID)
	}
	logutil.BgLogger().Warn("remove *****************************   " + fmt.Sprintf("sql:%v", sql))
	_, err := sess.execute(context.Background(), sql, "remove_backfill_job")
	return err
}

func updateBackfillJob(sess *session, tableName string, backfillJob *BackfillJob, label string) error {
	mate, err := backfillJob.Mate.Encode()
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("update mysql.%s set exec_id = '%s', exec_lease = '%s', state = %d, backfill_meta = '%s' where job_id = %d and section_id = %d",
		tableName, backfillJob.Instance_ID, backfillJob.Instance_Lease, backfillJob.State, mate, backfillJob.ID, backfillJob.JobID)
	logutil.BgLogger().Warn("update *****************************   " + fmt.Sprintf("sql:%v, sess:%#v", sql, sess))
	_, err = sess.execute(context.Background(), sql, label)
	return err
}

func AddBackfillHistoryJob(sess *session, backfillJobs []*BackfillJob) error {
	sqlPrefix := "insert into mysql.tidb_ddl_backfill_history(section_id, job_id, ele_id, ele_key, store_id, type, exec_id, exec_lease, state, backfill_meta) values"
	var sql string
	for i, bj := range backfillJobs {
		mateByte, err := bj.Mate.Encode()
		if err != nil {
			return err
		}

		if i == 0 {
			sql = sqlPrefix + fmt.Sprintf("(%d, %d, %d, '%s', %d, %d, '%s', '%s', %d, '%s')",
				bj.ID, bj.JobID, bj.EleID, bj.EleKey, bj.StoreID, bj.Tp, bj.Instance_ID, bj.Instance_Lease, bj.State, mateByte)
			continue
		}
		sql += fmt.Sprintf(", (%d, %d, %d, '%s', %d, %d, '%s', '%s', %d, '%s')",
			bj.ID, bj.JobID, bj.EleID, bj.EleKey, bj.StoreID, bj.Tp, bj.Instance_ID, bj.Instance_Lease, bj.State, mateByte)
	}
	logutil.BgLogger().Warn("add history *****************************   " + fmt.Sprintf("sql:%v", sql))
	_, err := sess.execute(context.Background(), sql, "add_backfill_history_job")
	return err
}

func wrapKey2String(key []byte) string {
	if len(key) == 0 {
		return "''"
	}
	return fmt.Sprintf("0x%x", key)
}

func getJobsBySQL(sess *session, tbl, condition string) ([]*model.Job, error) {
	rows, err := sess.execute(context.Background(), fmt.Sprintf("select job_meta from mysql.%s where %s", tbl, condition), "get_job")
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := make([]*model.Job, 0, 16)
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		job := model.Job{}
		err := job.Decode(jobBinary)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

// MoveJobFromQueue2Table move existing DDLs in queue to table.
func (d *ddl) MoveJobFromQueue2Table(inBootstrap bool) error {
	sess, err := d.sessPool.get()
	if err != nil {
		return err
	}
	defer d.sessPool.put(sess)
	return runInTxn(NewSession(sess), func(se *session) error {
		txn, err := se.txn()
		if err != nil {
			return errors.Trace(err)
		}
		t := meta.NewMeta(txn)
		isConcurrentDDL, err := t.IsConcurrentDDL()
		if !inBootstrap && (isConcurrentDDL || err != nil) {
			return errors.Trace(err)
		}
		systemDBID, err := t.GetSystemDBID()
		if err != nil {
			return errors.Trace(err)
		}
		for _, tp := range []workerType{addIdxWorker, generalWorker} {
			t := newMetaWithQueueTp(txn, tp)
			jobs, err := t.GetAllDDLJobsInQueue()
			if err != nil {
				return errors.Trace(err)
			}
			for _, job := range jobs {
				// In bootstrap, we can ignore the internal DDL.
				if inBootstrap && job.SchemaID == systemDBID {
					continue
				}
				err = insertDDLJobs2Table(se, false, job)
				if err != nil {
					return errors.Trace(err)
				}
				if tp == generalWorker {
					// General job do not have reorg info.
					continue
				}
				element, start, end, pid, err := t.GetDDLReorgHandle(job)
				if meta.ErrDDLReorgElementNotExist.Equal(err) {
					continue
				}
				if err != nil {
					return errors.Trace(err)
				}
				err = initDDLReorgHandle(se, job.ID, start, end, pid, element)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}

		if err = t.ClearALLDDLJob(); err != nil {
			return errors.Trace(err)
		}
		if err = t.ClearAllDDLReorgHandle(); err != nil {
			return errors.Trace(err)
		}
		return t.SetConcurrentDDL(true)
	})
}

// MoveJobFromTable2Queue move existing DDLs in table to queue.
func (d *ddl) MoveJobFromTable2Queue() error {
	sess, err := d.sessPool.get()
	if err != nil {
		return err
	}
	defer d.sessPool.put(sess)
	return runInTxn(NewSession(sess), func(se *session) error {
		txn, err := se.txn()
		if err != nil {
			return errors.Trace(err)
		}
		t := meta.NewMeta(txn)
		isConcurrentDDL, err := t.IsConcurrentDDL()
		if !isConcurrentDDL || err != nil {
			return errors.Trace(err)
		}
		jobs, err := getJobsBySQL(se, "tidb_ddl_job", "1 order by job_id")
		if err != nil {
			return errors.Trace(err)
		}

		for _, job := range jobs {
			jobListKey := meta.DefaultJobListKey
			if job.MayNeedReorg() {
				jobListKey = meta.AddIndexJobListKey
			}
			if err := t.EnQueueDDLJobNoUpdate(job, jobListKey); err != nil {
				return errors.Trace(err)
			}
		}

		reorgHandle, err := se.execute(context.Background(), "select job_id, start_key, end_key, physical_id, ele_id, ele_type from mysql.tidb_ddl_reorg", "get_handle")
		if err != nil {
			return errors.Trace(err)
		}
		for _, row := range reorgHandle {
			if err := t.UpdateDDLReorgHandle(row.GetInt64(0), row.GetBytes(1), row.GetBytes(2), row.GetInt64(3), &meta.Element{ID: row.GetInt64(4), TypeKey: row.GetBytes(5)}); err != nil {
				return errors.Trace(err)
			}
		}

		// clean up these 2 tables.
		_, err = se.execute(context.Background(), "delete from mysql.tidb_ddl_job", "delete_old_ddl")
		if err != nil {
			return errors.Trace(err)
		}
		_, err = se.execute(context.Background(), "delete from mysql.tidb_ddl_reorg", "delete_old_reorg")
		if err != nil {
			return errors.Trace(err)
		}
		return t.SetConcurrentDDL(false)
	})
}

func runInTxn(se *session, f func(*session) error) (err error) {
	err = se.begin()
	if err != nil {
		return err
	}
	err = f(se)
	if err != nil {
		se.rollback()
		return
	}
	return errors.Trace(se.commit())
}
