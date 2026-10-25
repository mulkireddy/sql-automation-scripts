/*========================================================================================================================
SCRIPT:       27_Backup_And_Restore_ETA_Progress_Tracker.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Tracks percent_complete and exact ETA for running BACKUP, RESTORE, DBCC CHECKDB, and ALTER INDEX jobs.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    r.session_id AS [SPID],
    r.command AS [Operation],
    DB_NAME(r.database_id) AS [Database_Name],
    CAST(r.percent_complete AS DECIMAL(5,2)) AS [Pct_Complete],
    CAST(r.total_elapsed_time / 60000.0 AS DECIMAL(10,1)) AS [Elapsed_Minutes],
    CAST(r.estimated_completion_time / 60000.0 AS DECIMAL(10,1)) AS [Remaining_Minutes],
    DATEADD(MILLISECOND, r.estimated_completion_time, GETDATE()) AS [Estimated_Finish_Time],
    r.wait_type AS [Current_Wait]
FROM sys.dm_exec_requests r
WHERE r.percent_complete > 0
   OR r.command IN (N'BACKUP DATABASE', N'BACKUP LOG', N'RESTORE DATABASE', N'RESTORE LOG', N'DBCC', N'DbccFilesCompact');
