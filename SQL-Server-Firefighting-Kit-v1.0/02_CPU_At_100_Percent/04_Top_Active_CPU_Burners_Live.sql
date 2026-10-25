/*========================================================================================================================
SCRIPT:       04_Top_Active_CPU_Burners_Live.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Identifies active sessions burning the most CPU right now + single-plan cache eviction command.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (20)
    r.session_id AS [SPID],
    r.status AS [Status],
    CAST(r.cpu_time / 1000.0 AS DECIMAL(12,2)) AS [CPU_Time_Sec],
    CAST(r.total_elapsed_time / 1000.0 AS DECIMAL(12,2)) AS [Elapsed_Sec],
    r.logical_reads AS [Logical_Reads],
    r.dop AS [Parallel_DOP],
    ISNULL(r.wait_type, N'ON_CPU') AS [Wait_Type],
    DB_NAME(r.database_id) AS [DB_Name],
    s.login_name,
    s.host_name,
    s.program_name,
    SUBSTRING(st.text, (r.statement_start_offset / 2) + 1,
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) ELSE r.statement_end_offset END - r.statement_start_offset) / 2) + 1
    ) AS [Active_SQL_Statement],
    qp.query_plan AS [Execution_Plan_XML],
    N'DBCC FREEPROCCACHE (' + CONVERT(NVARCHAR(128), r.plan_handle, 1) + N');' AS [Evict_Single_Bad_Plan_Cmd]
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) qp
WHERE s.is_user_process = 1 AND r.session_id <> @@SPID
ORDER BY r.cpu_time DESC, r.logical_reads DESC;
