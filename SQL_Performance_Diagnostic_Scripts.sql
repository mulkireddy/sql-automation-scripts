/*
=============================================================
SQL SERVER PERFORMANCE TUNING - DIAGNOSTIC SCRIPTS
=============================================================
Author: Created for Rajeshwar Reddy M
Purpose: Quick reference scripts for DBA daily work & interviews
Usage: Run each section as needed on your database
=============================================================
*/

USE [YourDatabaseName]; -- Change this to your database
GO

-- ============================================================
-- SCRIPT 1: What's in Memory (Buffer Pool)?
-- ============================================================
-- Purpose: See which tables are consuming your RAM
-- Look for: Tables using lots of memory that you rarely query

SELECT 
    OBJECT_NAME(p.object_id) AS TableName,
    COUNT(*) AS PagesInMemory,
    COUNT(*) * 8 / 1024 AS MB_InMemory
FROM sys.dm_os_buffer_descriptors bd
JOIN sys.allocation_units au ON bd.allocation_unit_id = au.allocation_unit_id
JOIN sys.partitions p ON au.container_id = p.partition_id
WHERE bd.database_id = DB_ID()
GROUP BY p.object_id
ORDER BY PagesInMemory DESC;
GO

-- ============================================================
-- SCRIPT 2: Missing Indexes (SQL Server Recommendations)
-- ============================================================
-- Purpose: Find indexes that SQL Server WISHES existed
-- Look for: Impact > 100,000 = seriously consider creating

SELECT TOP 10
    ROUND(avg_total_user_cost * avg_user_impact * (user_seeks + user_scans), 0) AS [Impact],
    OBJECT_NAME(id.object_id, id.database_id) AS TableName,
    'CREATE INDEX IX_' + OBJECT_NAME(id.object_id, id.database_id) + '_' 
        + REPLACE(REPLACE(REPLACE(ISNULL(equality_columns, ''), ', ', '_'), '[', ''), ']', '') 
        + ' ON ' + id.statement 
        + ' (' + ISNULL(equality_columns, '') 
        + CASE WHEN equality_columns IS NOT NULL AND inequality_columns IS NOT NULL THEN ', ' ELSE '' END 
        + ISNULL(inequality_columns, '') + ')' 
        + ISNULL(' INCLUDE (' + included_columns + ')', '') AS CreateIndexStatement
FROM sys.dm_db_missing_index_group_stats gs
JOIN sys.dm_db_missing_index_groups ig ON gs.group_handle = ig.index_group_handle
JOIN sys.dm_db_missing_index_details id ON ig.index_handle = id.index_handle
WHERE id.database_id = DB_ID()
ORDER BY [Impact] DESC;
GO

-- ============================================================
-- SCRIPT 3: Unused Indexes (Candidates for Removal)
-- ============================================================
-- Purpose: Find indexes that are costing you on writes but never used for reads
-- Look for: user_seeks=0 AND user_scans=0 AND user_updates > 0

SELECT 
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    ISNULL(s.user_seeks, 0) AS user_seeks,
    ISNULL(s.user_scans, 0) AS user_scans,
    ISNULL(s.user_lookups, 0) AS user_lookups,
    ISNULL(s.user_updates, 0) AS user_updates,
    CASE 
        WHEN ISNULL(s.user_seeks, 0) + ISNULL(s.user_scans, 0) + ISNULL(s.user_lookups, 0) = 0 
        THEN '⚠️ CANDIDATE FOR REMOVAL'
        ELSE '✅ IN USE'
    END AS Status
FROM sys.indexes i
LEFT JOIN sys.dm_db_index_usage_stats s 
    ON i.object_id = s.object_id AND i.index_id = s.index_id AND s.database_id = DB_ID()
WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
    AND i.type_desc = 'NONCLUSTERED'
    AND i.name IS NOT NULL
ORDER BY ISNULL(s.user_seeks, 0) + ISNULL(s.user_scans, 0) + ISNULL(s.user_lookups, 0) ASC;
GO

-- ============================================================
-- SCRIPT 4: Index Write vs Read Ratio
-- ============================================================
-- Purpose: Find indexes with more writes than reads (bad ROI)
-- Look for: TotalWrites >> TotalReads = index costs more than it helps

SELECT 
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    s.user_seeks + s.user_scans + s.user_lookups AS TotalReads,
    s.user_updates AS TotalWrites,
    CASE 
        WHEN s.user_updates > (s.user_seeks + s.user_scans + s.user_lookups) * 10
        THEN '⚠️ WRITE-HEAVY - Review this index'
        ELSE '✅ OK'
    END AS Status
FROM sys.indexes i
JOIN sys.dm_db_index_usage_stats s 
    ON i.object_id = s.object_id AND i.index_id = s.index_id
WHERE s.database_id = DB_ID()
    AND i.type_desc = 'NONCLUSTERED'
    AND i.name IS NOT NULL
ORDER BY s.user_updates DESC;
GO

-- ============================================================
-- SCRIPT 5: Wait Statistics (What's the Bottleneck?)
-- ============================================================
-- Purpose: Find what SQL Server is waiting for
-- Look for: Top wait types indicate your bottleneck

SELECT TOP 10
    wait_type,
    wait_time_ms / 1000.0 AS wait_time_seconds,
    waiting_tasks_count,
    wait_time_ms / NULLIF(waiting_tasks_count, 0) AS avg_wait_ms,
    CASE 
        WHEN wait_type LIKE 'PAGEIOLATCH%' THEN '💾 Disk I/O - Need more memory or faster disk'
        WHEN wait_type LIKE 'LCK%' THEN '🔒 Locking - Blocking issues'
        WHEN wait_type = 'CXPACKET' THEN '🔀 Parallelism - Check MAXDOP setting'
        WHEN wait_type = 'ASYNC_NETWORK_IO' THEN '🌐 Network - App consuming results slowly'
        WHEN wait_type LIKE 'SOS_SCHEDULER%' THEN '⚡ CPU - Need more CPU power'
        WHEN wait_type LIKE 'WRITELOG%' THEN '📝 Log Write - Transaction log disk slow'
        ELSE '❓ Research this wait type'
    END AS Diagnosis
FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN (
    'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 
    'SLEEP_TASK', 'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH',
    'WAITFOR', 'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE', 'BROKER_TASK_STOP',
    'BROKER_TO_FLUSH', 'BROKER_EVENTHANDLER', 'BROKER_RECEIVE_WAITFOR',
    'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'DIRTY_PAGE_POLL', 
    'XE_DISPATCHER_WAIT', 'XE_TIMER_EVENT', 'FT_IFTS_SCHEDULER_IDLE_WAIT'
)
ORDER BY wait_time_ms DESC;
GO

-- ============================================================
-- SCRIPT 6: Currently Waiting Queries (Live View)
-- ============================================================
-- Purpose: What queries are waiting RIGHT NOW?
-- Look for: Long wait times, blocking_session_id > 0

SELECT 
    r.session_id,
    r.wait_type,
    r.wait_time AS wait_time_ms,
    r.blocking_session_id,
    SUBSTRING(t.text, (r.statement_start_offset/2)+1,
        ((CASE r.statement_end_offset
            WHEN -1 THEN DATALENGTH(t.text)
            ELSE r.statement_end_offset
        END - r.statement_start_offset)/2) + 1) AS CurrentQuery,
    r.cpu_time,
    r.logical_reads,
    r.status
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE r.session_id > 50  -- Exclude system sessions
    AND r.session_id <> @@SPID  -- Exclude this query
ORDER BY r.wait_time DESC;
GO

-- ============================================================
-- SCRIPT 7: Find Current Blocking
-- ============================================================
-- Purpose: Who's blocking whom?
-- Look for: BlockerSessionID and what query it's running

SELECT 
    blocker.session_id AS BlockerSessionID,
    SUBSTRING(blocker_sql.text, 1, 200) AS BlockerQuery,
    blocked.session_id AS BlockedSessionID,
    SUBSTRING(blocked_sql.text, 1, 200) AS BlockedQuery,
    blocked.wait_type,
    blocked.wait_time / 1000 AS WaitSeconds
FROM sys.dm_exec_requests blocked
JOIN sys.dm_exec_sessions blocker ON blocked.blocking_session_id = blocker.session_id
CROSS APPLY sys.dm_exec_sql_text(blocked.sql_handle) blocked_sql
CROSS APPLY sys.dm_exec_sql_text(blocker.most_recent_sql_handle) blocker_sql
WHERE blocked.blocking_session_id > 0
ORDER BY blocked.wait_time DESC;
GO

-- ============================================================
-- SCRIPT 8: Top 10 CPU-Hungry Queries
-- ============================================================
-- Purpose: Find queries eating CPU
-- Look for: High AvgCPU with high execution_count = major impact

SELECT TOP 10
    total_worker_time / execution_count AS AvgCPU,
    total_worker_time AS TotalCPU,
    execution_count,
    total_logical_reads / execution_count AS AvgReads,
    SUBSTRING(st.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset
            WHEN -1 THEN DATALENGTH(st.text)
            ELSE qs.statement_end_offset
        END - qs.statement_start_offset)/2) + 1) AS QueryText,
    qp.query_plan
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
ORDER BY total_worker_time / execution_count DESC;
GO

-- ============================================================
-- SCRIPT 9: Top 10 I/O-Hungry Queries
-- ============================================================
-- Purpose: Find queries doing massive reads
-- Look for: High AvgReads = table scans or missing indexes

SELECT TOP 10
    total_logical_reads / execution_count AS AvgReads,
    total_logical_reads AS TotalReads,
    total_logical_writes / execution_count AS AvgWrites,
    execution_count,
    SUBSTRING(st.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset
            WHEN -1 THEN DATALENGTH(st.text)
            ELSE qs.statement_end_offset
        END - qs.statement_start_offset)/2) + 1) AS QueryText
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
ORDER BY total_logical_reads / execution_count DESC;
GO

-- ============================================================
-- SCRIPT 10: Database File I/O Latency
-- ============================================================
-- Purpose: Check if disks are slow
-- Look for: AvgReadLatencyMs > 20ms = disk performance issue

SELECT 
    DB_NAME(vfs.database_id) AS DatabaseName,
    mf.name AS LogicalFileName,
    mf.physical_name,
    vfs.num_of_reads,
    vfs.num_of_writes,
    vfs.io_stall_read_ms / NULLIF(vfs.num_of_reads, 0) AS AvgReadLatencyMs,
    vfs.io_stall_write_ms / NULLIF(vfs.num_of_writes, 0) AS AvgWriteLatencyMs,
    CASE 
        WHEN vfs.io_stall_read_ms / NULLIF(vfs.num_of_reads, 0) > 50 THEN '🔴 CRITICAL - Very Slow'
        WHEN vfs.io_stall_read_ms / NULLIF(vfs.num_of_reads, 0) > 20 THEN '🟡 WARNING - Slow'
        ELSE '🟢 OK'
    END AS ReadStatus,
    CASE 
        WHEN vfs.io_stall_write_ms / NULLIF(vfs.num_of_writes, 0) > 20 THEN '🟡 WARNING - Slow Writes'
        ELSE '🟢 OK'
    END AS WriteStatus
FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
JOIN sys.master_files mf ON vfs.database_id = mf.database_id AND vfs.file_id = mf.file_id
WHERE vfs.num_of_reads > 0
ORDER BY vfs.io_stall DESC;
GO

-- ============================================================
-- SCRIPT 11: Index Fragmentation Check
-- ============================================================
-- Purpose: Find fragmented indexes
-- Look for: >30% = REBUILD, 10-30% = REORGANIZE

SELECT 
    OBJECT_NAME(ips.object_id) AS TableName,
    i.name AS IndexName,
    ips.index_type_desc,
    CAST(ips.avg_fragmentation_in_percent AS DECIMAL(5,2)) AS FragmentationPercent,
    ips.page_count,
    CASE 
        WHEN ips.avg_fragmentation_in_percent > 30 THEN '🔴 REBUILD'
        WHEN ips.avg_fragmentation_in_percent > 10 THEN '🟡 REORGANIZE'
        ELSE '🟢 OK'
    END AS Recommendation,
    'ALTER INDEX ' + QUOTENAME(i.name) + ' ON ' + QUOTENAME(OBJECT_SCHEMA_NAME(i.object_id)) + '.' + QUOTENAME(OBJECT_NAME(i.object_id))
        + CASE WHEN ips.avg_fragmentation_in_percent > 30 THEN ' REBUILD' ELSE ' REORGANIZE' END AS MaintenanceScript
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
WHERE ips.page_count > 500  -- Only meaningful indexes
    AND ips.avg_fragmentation_in_percent > 10
    AND i.name IS NOT NULL
ORDER BY ips.avg_fragmentation_in_percent DESC;
GO

-- ============================================================
-- SCRIPT 12: Table Sizes and Row Counts
-- ============================================================
-- Purpose: Quick overview of database tables
-- Look for: Large tables without proper indexes

SELECT 
    SCHEMA_NAME(t.schema_id) AS SchemaName,
    t.name AS TableName,
    p.rows AS RowCount,
    SUM(a.total_pages) * 8 / 1024 AS TotalMB,
    SUM(a.used_pages) * 8 / 1024 AS UsedMB,
    COUNT(DISTINCT i.index_id) AS IndexCount
FROM sys.tables t
JOIN sys.indexes i ON t.object_id = i.object_id
JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.allocation_units a ON p.partition_id = a.container_id
GROUP BY t.schema_id, t.name, p.rows
ORDER BY SUM(a.total_pages) DESC;
GO

-- ============================================================
-- SCRIPT 13: Expensive Key Lookups 
-- ============================================================
-- Purpose: Find indexes causing expensive key lookups
-- Look for: High user_lookups = add INCLUDE columns to the index

SELECT 
    OBJECT_NAME(s.object_id) AS TableName,
    i.name AS IndexName,
    s.user_seeks,
    s.user_scans,
    s.user_lookups,  -- KEY LOOKUPS!
    CASE 
        WHEN s.user_lookups > s.user_seeks * 0.5 
        THEN '⚠️ HIGH KEY LOOKUPS - Add INCLUDE columns'
        ELSE '✅ OK'
    END AS Status
FROM sys.dm_db_index_usage_stats s
JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
WHERE s.database_id = DB_ID()
    AND i.type_desc = 'NONCLUSTERED'
    AND s.user_lookups > 1000
ORDER BY s.user_lookups DESC;
GO

-- ============================================================
-- SCRIPT 14: Statistics Freshness
-- ============================================================
-- Purpose: Find outdated statistics
-- Look for: Stats older than 7 days with row modifications

SELECT 
    OBJECT_NAME(s.object_id) AS TableName,
    s.name AS StatName,
    sp.last_updated,
    sp.rows,
    sp.rows_sampled,
    sp.modification_counter,
    DATEDIFF(DAY, sp.last_updated, GETDATE()) AS DaysSinceUpdate,
    CASE 
        WHEN DATEDIFF(DAY, sp.last_updated, GETDATE()) > 7 AND sp.modification_counter > 1000
        THEN '⚠️ UPDATE NEEDED'
        ELSE '✅ OK'
    END AS Status
FROM sys.stats s
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
WHERE OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
    AND sp.last_updated IS NOT NULL
ORDER BY sp.modification_counter DESC;
GO

-- ============================================================
-- QUICK REFERENCE: Common Interview Answers
-- ============================================================
/*
Q: How to find slow queries?
A: sys.dm_exec_query_stats + sys.dm_exec_sql_text

Q: How to check for blocking?
A: sys.dm_exec_requests WHERE blocking_session_id > 0

Q: How to find missing indexes?
A: sys.dm_db_missing_index_details + sys.dm_db_missing_index_group_stats

Q: How to check wait statistics?
A: sys.dm_os_wait_stats (exclude background waits)

Q: How to check disk performance?
A: sys.dm_io_virtual_file_stats (avg read latency < 20ms is good)

Q: How to update statistics?
A: UPDATE STATISTICS TableName WITH FULLSCAN; -- or SAMPLE n PERCENT

Q: How to rebuild indexes?
A: ALTER INDEX IndexName ON TableName REBUILD WITH (ONLINE = ON);
*/
