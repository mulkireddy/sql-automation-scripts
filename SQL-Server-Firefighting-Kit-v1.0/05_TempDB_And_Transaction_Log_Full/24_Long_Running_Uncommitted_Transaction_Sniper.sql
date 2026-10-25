/*========================================================================================================================
SCRIPT:       24_Long_Running_Uncommitted_Transaction_Sniper.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Pinpoints the exact uncommitted transaction holding the Transaction Log hostage and its log bytes used.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (20)
    st.session_id AS [SPID],
    DB_NAME(dt.database_id) AS [Database_Name],
    DATEDIFF(SECOND, dt.database_transaction_begin_time, GETDATE()) AS [Transaction_Age_Sec],
    CAST(dt.database_transaction_log_bytes_used / 1048576.0 AS DECIMAL(14,2)) AS [Log_Space_Used_MB],
    CAST(dt.database_transaction_log_bytes_reserved / 1048576.0 AS DECIMAL(14,2)) AS [Log_Space_Reserved_MB],
    s.status AS [Session_Status],
    s.login_name,
    s.host_name,
    s.program_name,
    SUBSTRING(txt.text, 1, 400) AS [Last_SQL_Statement],
    N'KILL ' + CAST(st.session_id AS NVARCHAR(10)) + N';' AS [Kill_Cmd]
FROM sys.dm_tran_database_transactions dt
INNER JOIN sys.dm_tran_session_transactions st ON dt.transaction_id = st.transaction_id
INNER JOIN sys.dm_exec_sessions s ON st.session_id = s.session_id
LEFT JOIN sys.dm_exec_connections c ON st.session_id = c.session_id
OUTER APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) txt
WHERE dt.database_transaction_begin_time IS NOT NULL
ORDER BY dt.database_transaction_begin_time ASC;
