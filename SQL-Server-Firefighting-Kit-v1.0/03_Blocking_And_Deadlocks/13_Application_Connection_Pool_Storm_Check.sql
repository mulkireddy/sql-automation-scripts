/*========================================================================================================================
SCRIPT:       13_Application_Connection_Pool_Storm_Check.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Groups all sessions by Host, Application, and Login to detect connection pool leaks & ASYNC_NETWORK_IO.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    host_name AS [Client_Host_Name],
    program_name AS [Application_Name],
    login_name AS [Login_Name],
    DB_NAME(database_id) AS [Database_Name],
    COUNT(*) AS [Total_Connections],
    SUM(CASE WHEN status = N'running' OR status = N'runnable' OR status = N'suspended' THEN 1 ELSE 0 END) AS [Active_Sessions],
    SUM(CASE WHEN status = N'sleeping' THEN 1 ELSE 0 END) AS [Sleeping_Pool_Sessions],
    SUM(open_transaction_count) AS [Total_Open_Transactions]
FROM sys.dm_exec_sessions
WHERE is_user_process = 1
GROUP BY host_name, program_name, login_name, database_id
ORDER BY [Total_Connections] DESC;
