/*========================================================================================================================
SCRIPT:       28_Ghost_Cleanup_And_Rollback_Progress_Tracker.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Tracks progress and ETA of KILLED/ROLLBACK sessions and Ghost Cleanup tasks.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    r.session_id AS [SPID],
    r.status AS [Status],
    r.command AS [Command],
    DB_NAME(r.database_id) AS [Database_Name],
    CAST(r.percent_complete AS DECIMAL(5,2)) AS [Rollback_Pct_Complete],
    CAST(r.estimated_completion_time / 1000.0 AS DECIMAL(12,1)) AS [Est_Seconds_Remaining],
    r.wait_type,
    r.cpu_time,
    r.reads,
    r.writes
FROM sys.dm_exec_requests r
WHERE r.command = N'KILLED/ROLLBACK'
   OR r.status = N'rollback'
   OR r.command LIKE N'%GHOST%';
