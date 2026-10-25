/*========================================================================================================================
SCRIPT:       10_Open_Sleeping_Transactions_With_Locks.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Finds idle/sleeping sessions that left an open transaction (BEGIN TRAN without COMMIT) holding locks.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    s.session_id AS [SPID],
    s.status AS [Session_Status],
    s.open_transaction_count AS [Open_Trans],
    DATEDIFF(SECOND, s.last_request_end_time, GETDATE()) AS [Idle_Since_Last_Batch_Sec],
    DB_NAME(s.database_id) AS [Database_Name],
    s.login_name,
    s.host_name,
    s.program_name,
    SUBSTRING(st.text, 1, 500) AS [Last_Executed_SQL_Batch],
    N'KILL ' + CAST(s.session_id AS NVARCHAR(10)) + N';' AS [Kill_Orphaned_Session_Cmd]
FROM sys.dm_exec_sessions s
LEFT JOIN sys.dm_exec_connections c ON s.session_id = c.session_id
OUTER APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) st
WHERE s.is_user_process = 1
  AND s.status = N'sleeping'
  AND s.open_transaction_count > 0
ORDER BY [Idle_Since_Last_Batch_Sec] DESC;
