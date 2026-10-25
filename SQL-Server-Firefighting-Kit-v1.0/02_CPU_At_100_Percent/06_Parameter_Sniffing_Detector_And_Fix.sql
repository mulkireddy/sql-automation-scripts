/*========================================================================================================================
SCRIPT:       06_Parameter_Sniffing_Detector_And_Fix.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Detects cached queries with massive variance between Min and Max CPU/Reads (classic Parameter Sniffing).
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (25)
    DB_NAME(st.dbid) AS [Database_Name],
    OBJECT_NAME(st.objectid, st.dbid) AS [Proc_Name],
    qs.execution_count AS [Executions],
    qs.min_logical_reads AS [Min_Reads],
    qs.max_logical_reads AS [Max_Reads],
    CAST(qs.min_worker_time / 1000.0 AS DECIMAL(14,2)) AS [Min_CPU_ms],
    CAST(qs.max_worker_time / 1000.0 AS DECIMAL(14,2)) AS [Max_CPU_ms],
    CAST(qs.last_worker_time / 1000.0 AS DECIMAL(14,2)) AS [Last_CPU_ms],
    SUBSTRING(st.text, 1, 400) AS [Query_Snippet],
    qp.query_plan AS [Plan_XML],
    N'DBCC FREEPROCCACHE (' + CONVERT(NVARCHAR(128), qs.plan_handle, 1) + N');' AS [Targeted_Plan_Flush_Cmd]
FROM sys.dm_exec_query_stats qs
OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) st
OUTER APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
WHERE qs.execution_count > 10
  AND qs.max_logical_reads > (qs.min_logical_reads * 50)
  AND qs.max_worker_time > 100000 -- >100ms max CPU
ORDER BY (qs.max_worker_time - qs.min_worker_time) DESC;
