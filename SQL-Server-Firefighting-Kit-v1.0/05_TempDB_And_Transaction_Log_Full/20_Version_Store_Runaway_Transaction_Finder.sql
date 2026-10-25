/*========================================================================================================================
SCRIPT:       20_Version_Store_Runaway_Transaction_Finder.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Finds the oldest active Snapshot/RCSI transaction blocking TempDB Version Store cleanup.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (15)
    ast.session_id AS [SPID],
    ast.elapsed_time_seconds AS [Open_Snapshot_Duration_Sec],
    ast.is_snapshot AS [Is_Snapshot],
    ast.max_version_chain_traversed AS [Max_Version_Chain],
    s.status AS [Session_Status],
    DB_NAME(s.database_id) AS [Database_Name],
    s.login_name,
    s.host_name,
    s.program_name,
    SUBSTRING(st.text, 1, 400) AS [Last_SQL_Text],
    N'KILL ' + CAST(ast.session_id AS NVARCHAR(10)) + N';' AS [Kill_Cmd]
FROM sys.dm_tran_active_snapshot_database_transactions ast
INNER JOIN sys.dm_exec_sessions s ON ast.session_id = s.session_id
LEFT JOIN sys.dm_exec_connections c ON ast.session_id = c.session_id
OUTER APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) st
ORDER BY ast.elapsed_time_seconds DESC;
