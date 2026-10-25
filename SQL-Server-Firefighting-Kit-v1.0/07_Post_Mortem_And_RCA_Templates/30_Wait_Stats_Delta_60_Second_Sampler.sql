/*========================================================================================================================
SCRIPT:       30_Wait_Stats_Delta_60_Second_Sampler.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Captures a live 10-second delta of sys.dm_os_wait_stats (excluding benign idle waits).
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb..#WaitSnap1') IS NOT NULL DROP TABLE #WaitSnap1;

SELECT wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms
INTO #WaitSnap1
FROM sys.dm_os_wait_stats
WHERE waiting_tasks_count > 0;

WAITFOR DELAY '00:00:10'; -- 10-second live sample

SELECT TOP (15)
    w2.wait_type AS [Wait_Type],
    (w2.waiting_tasks_count - ISNULL(w1.waiting_tasks_count, 0)) AS [Wait_Count_Delta],
    CAST((w2.wait_time_ms - ISNULL(w1.wait_time_ms, 0)) / 1000.0 AS DECIMAL(12,2)) AS [Total_Wait_Sec_Delta],
    CAST((w2.signal_wait_time_ms - ISNULL(w1.signal_wait_time_ms, 0)) / 1000.0 AS DECIMAL(12,2)) AS [Signal_CPU_Wait_Sec_Delta],
    CAST((w2.wait_time_ms - ISNULL(w1.wait_time_ms, 0)) * 1.0 / NULLIF((w2.waiting_tasks_count - ISNULL(w1.waiting_tasks_count, 0)), 0) AS DECIMAL(12,2)) AS [Avg_Wait_ms]
FROM sys.dm_os_wait_stats w2
LEFT JOIN #WaitSnap1 w1 ON w2.wait_type = w1.wait_type
WHERE (w2.wait_time_ms - ISNULL(w1.wait_time_ms, 0)) > 0
  AND w2.wait_type NOT IN (
    N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP', N'BROKER_TO_FLUSH',
    N'CHECKPOINT_QUEUE', N'CHKPT', N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT', N'DIRTY_PAGE_POLL',
    N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'HADR_LOGCAPTURE_WAIT',
    N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE', N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE',
    N'ONDEMAND_TASK_QUEUE', N'PARALLEL_REDO_WORKER_WAIT_WORK', N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
    N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'REQUEST_FOR_DEADLOCK_SEARCH', N'RESOURCE_QUEUE',
    N'SERVER_IDLE_CHECK', N'SLEEP_BPOOL_FLUSH', N'SLEEP_DBSTARTUP', N'SLEEP_DCOMSTARTUP',
    N'SLEEP_MASTERDBREADY', N'SLEEP_MASTERMDREADY', N'SLEEP_MASTERUPGRADED', N'SLEEP_MSDBSTARTUP',
    N'SLEEP_SYSTEMTASK', N'SLEEP_TASK', N'SLEEP_TEMPDBSTARTUP', N'SNI_HTTP_ACCEPT',
    N'SOS_WORK_DISPATCHER', N'SP_SERVER_DIAGNOSTICS_SLEEP', N'SQLTRACE_BUFFER_FLUSH',
    N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'WAITFOR', N'XE_DISPATCHER_WAIT', N'XE_TIMER_EVENT'
  )
ORDER BY [Total_Wait_Sec_Delta] DESC;

DROP TABLE #WaitSnap1;
