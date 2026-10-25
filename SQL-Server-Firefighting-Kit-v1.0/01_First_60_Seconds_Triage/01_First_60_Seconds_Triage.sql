/*========================================================================================================================
SCRIPT:       01_First_60_Seconds_Triage.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Instant, zero-impact triage snapshot during a live production incident (4 Diagnostic Result Sets).
SAFETY:       100% Read-Only. Uses READ UNCOMMITTED & 2-second Lock Timeout.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 2000;
SET DEADLOCK_PRIORITY LOW;

DECLARE @SQLCpu INT, @OtherCpu INT, @PageLifeExpectancy INT, @MemGrantsPending INT;
DECLARE @TempDBFreeMB DECIMAL(18,2), @TempDBPctUsed DECIMAL(5,2);
DECLARE @ActiveUserReq INT, @BlockedReq INT, @OpenTranSleeping INT;

SELECT TOP (1)
    @SQLCpu   = record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int'),
    @OtherCpu = 100 - record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int')
                - record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int')
FROM (
    SELECT CAST(record AS XML) AS record
    FROM sys.dm_os_ring_buffers
    WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' AND record LIKE N'%<Record%'
) AS x
ORDER BY record.value('(./Record/@id)[1]', 'int') DESC;

SELECT @PageLifeExpectancy = AVG(CAST(cntr_value AS BIGINT))
FROM sys.dm_os_performance_counters
WHERE object_name LIKE N'%Buffer Manager%' AND RTRIM(counter_name) = N'Page life expectancy';

SELECT @MemGrantsPending = SUM(CAST(cntr_value AS INT))
FROM sys.dm_os_performance_counters
WHERE object_name LIKE N'%Memory Manager%' AND RTRIM(counter_name) = N'Memory Grants Pending';

SELECT
    @TempDBFreeMB  = SUM(unallocated_extent_page_count) * 8.0 / 1024.0,
    @TempDBPctUsed = CAST((1.0 - (SUM(unallocated_extent_page_count) * 1.0 / NULLIF(SUM(total_page_count), 0))) * 100.0 AS DECIMAL(5,2))
FROM tempdb.sys.dm_db_file_space_usage;

SELECT
    @ActiveUserReq = COUNT(*),
    @BlockedReq    = SUM(CASE WHEN blocking_session_id > 0 THEN 1 ELSE 0 END)
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
WHERE s.is_user_process = 1 AND r.session_id <> @@SPID;

SELECT @OpenTranSleeping = COUNT(*)
FROM sys.dm_exec_sessions
WHERE is_user_process = 1 AND status = N'sleeping' AND open_transaction_count > 0;

-- RESULT SET 1: SERVER VITAL SIGNS
SELECT
    @@SERVERNAME AS [Server_Name],
    CONVERT(VARCHAR(19), GETDATE(), 120) AS [Snapshot_Time],
    ISNULL(@SQLCpu, -1) AS [SQL_CPU_Pct],
    ISNULL(@OtherCpu, -1) AS [Non_SQL_CPU_Pct],
    @ActiveUserReq AS [Active_Requests],
    ISNULL(@BlockedReq, 0) AS [Blocked_Sessions],
    @OpenTranSleeping AS [Sleeping_With_Open_Tran],
    ISNULL(@MemGrantsPending, 0) AS [Mem_Grants_Pending],
    @PageLifeExpectancy AS [PLE_Seconds],
    @TempDBPctUsed AS [TempDB_Used_Pct],
    @TempDBFreeMB AS [TempDB_Free_MB],
    CASE
        WHEN ISNULL(@BlockedReq, 0) >= 5 THEN N'CRITICAL: Severe Blocking Chain -> Check Result Set 2 & Script #09'
        WHEN ISNULL(@MemGrantsPending, 0) > 0 THEN N'CRITICAL: Memory Grant Queueing -> Run Script #15'
        WHEN @TempDBPctUsed >= 85.0 THEN N'CRITICAL: TempDB Filling Up (>85%) -> Run Script #19'
        WHEN ISNULL(@SQLCpu, 0) >= 85 THEN N'WARNING: High SQL CPU (>85%) -> Run Script #04'
        WHEN ISNULL(@OtherCpu, 0) >= 25 THEN N'WARNING: Non-SQL OS Process Eating CPU -> Check Windows Task Manager'
        WHEN @OpenTranSleeping > 0 THEN N'WARNING: Sleeping Sessions Holding Open Transactions -> Run Script #10'
        ELSE N'OK: No Global Threshold Breached -> Review Result Set 3 & 4'
    END AS [Triage_Verdict];

-- RESULT SET 2: LEAD BLOCKERS
;WITH BlockingHierarchy AS (
    SELECT
        s.session_id, ISNULL(r.blocking_session_id, 0) AS blocking_session_id,
        s.login_name, s.host_name, s.program_name, s.status AS session_status,
        s.open_transaction_count, r.wait_type, r.wait_time / 1000.0 AS wait_sec,
        DB_NAME(COALESCE(r.database_id, s.database_id)) AS database_name,
        COALESCE(r.sql_handle, c.most_recent_sql_handle) AS sql_handle
    FROM sys.dm_exec_sessions s
    LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
    LEFT JOIN sys.dm_exec_connections c ON s.session_id = c.session_id
    WHERE s.is_user_process = 1
)
SELECT
    b.session_id AS [Lead_Blocker_SPID],
    b.session_status AS [Status],
    COUNT(v.session_id) AS [Direct_Blocked_Victims],
    b.open_transaction_count AS [Open_Trans],
    b.database_name AS [DB_Name],
    b.login_name AS [Login],
    b.host_name AS [Host],
    b.program_name AS [Application],
    ISNULL(b.wait_type, N'IDLE / AWAITING_CLIENT_CMD') AS [Current_Wait],
    SUBSTRING(st.text, 1, 400) AS [Lead_Blocker_SQL_Snippet],
    N'KILL ' + CAST(b.session_id AS NVARCHAR(10)) + N';' AS [Emergency_Kill_Cmd]
FROM BlockingHierarchy b
INNER JOIN BlockingHierarchy v ON b.session_id = v.blocking_session_id
OUTER APPLY sys.dm_exec_sql_text(b.sql_handle) st
WHERE b.blocking_session_id = 0
GROUP BY b.session_id, b.session_status, b.open_transaction_count, b.database_name,
         b.login_name, b.host_name, b.program_name, b.wait_type, SUBSTRING(st.text, 1, 400)
ORDER BY [Direct_Blocked_Victims] DESC;

-- RESULT SET 3: TOP 15 ACTIVE QUERIES
;WITH TempDBTaskUsage AS (
    SELECT session_id,
        SUM(internal_objects_alloc_page_count + user_objects_alloc_page_count
        - internal_objects_dealloc_page_count - user_objects_dealloc_page_count) * 8.0 / 1024.0 AS Net_TempDB_MB
    FROM sys.dm_db_task_space_usage GROUP BY session_id
)
SELECT TOP (15)
    r.session_id AS [SPID], r.blocking_session_id AS [Blocked_By],
    CAST(r.total_elapsed_time / 1000.0 AS DECIMAL(12,1)) AS [Duration_Sec],
    r.status AS [Status], r.command AS [Command_Type], DB_NAME(r.database_id) AS [Database_Name],
    ISNULL(r.wait_type, N'-- ON CPU --') AS [Current_Wait_Type],
    CAST(r.cpu_time / 1000.0 AS DECIMAL(12,1)) AS [CPU_Sec],
    r.logical_reads AS [Logical_Reads], r.reads AS [Physical_Reads], r.writes AS [Writes],
    CAST(ISNULL(t.Net_TempDB_MB, 0) AS DECIMAL(12,2)) AS [Active_TempDB_MB],
    s.open_transaction_count AS [Open_Trans], s.login_name AS [Login_Name],
    s.host_name AS [Host_Name], s.program_name AS [Program_Name],
    SUBSTRING(st.text, (r.statement_start_offset / 2) + 1,
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) ELSE r.statement_end_offset END - r.statement_start_offset) / 2) + 1
    ) AS [Executing_Statement_Text],
    qp.query_plan AS [Click_For_XML_Plan]
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
LEFT JOIN TempDBTaskUsage t ON r.session_id = t.session_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) qp
WHERE s.is_user_process = 1 AND r.session_id <> @@SPID
ORDER BY r.total_elapsed_time DESC, r.cpu_time DESC;

-- RESULT SET 4: LIVE WAIT SUMMARY
SELECT
    ISNULL(r.wait_type, N'ON_CPU_EXECUTING') AS [Wait_Category],
    COUNT(*) AS [Waiting_Session_Count],
    CAST(AVG(r.wait_time / 1000.0) AS DECIMAL(12,2)) AS [Avg_Wait_Sec],
    CAST(MAX(r.wait_time / 1000.0) AS DECIMAL(12,2)) AS [Max_Wait_Sec]
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
WHERE s.is_user_process = 1 AND r.session_id <> @@SPID
GROUP BY r.wait_type
ORDER BY [Waiting_Session_Count] DESC;
