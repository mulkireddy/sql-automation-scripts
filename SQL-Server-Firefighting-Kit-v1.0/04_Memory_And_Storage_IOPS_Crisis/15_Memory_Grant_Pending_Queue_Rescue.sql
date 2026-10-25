/*========================================================================================================================
SCRIPT:       15_Memory_Grant_Pending_Queue_Rescue.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Shows queries holding huge workspace memory grants vs queries stuck waiting on RESOURCE_SEMAPHORE.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    mg.session_id AS [SPID],
    CASE WHEN mg.grant_time IS NULL THEN N'WAITING IN QUEUE (RESOURCE_SEMAPHORE)' ELSE N'GRANTED & HOLDING RAM' END AS [Grant_Status],
    CAST(mg.requested_memory_kb / 1024.0 AS DECIMAL(12,2)) AS [Requested_MB],
    CAST(mg.granted_memory_kb / 1024.0 AS DECIMAL(12,2)) AS [Granted_MB],
    CAST(mg.used_memory_kb / 1024.0 AS DECIMAL(12,2)) AS [Actually_Used_MB],
    mg.dop AS [DOP],
    mg.query_cost AS [Estimated_Cost],
    DB_NAME(r.database_id) AS [DB_Name],
    SUBSTRING(st.text, 1, 400) AS [Query_Text],
    qp.query_plan AS [Plan_XML]
FROM sys.dm_exec_query_memory_grants mg
LEFT JOIN sys.dm_exec_requests r ON mg.session_id = r.session_id
OUTER APPLY sys.dm_exec_sql_text(mg.sql_handle) st
OUTER APPLY sys.dm_exec_query_plan(mg.plan_handle) qp
ORDER BY CASE WHEN mg.grant_time IS NULL THEN 0 ELSE 1 END, mg.requested_memory_kb DESC;
