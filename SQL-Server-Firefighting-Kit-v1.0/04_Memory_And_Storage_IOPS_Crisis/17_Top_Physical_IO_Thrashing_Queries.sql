/*========================================================================================================================
SCRIPT:       17_Top_Physical_IO_Thrashing_Queries.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Finds queries causing the heaviest physical disk reads and writes in the plan cache.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (20)
    DB_NAME(st.dbid) AS [Database_Name],
    qs.execution_count AS [Executions],
    qs.total_physical_reads AS [Total_Physical_Reads],
    qs.total_physical_reads / NULLIF(qs.execution_count, 0) AS [Avg_Physical_Reads],
    qs.total_logical_reads / NULLIF(qs.execution_count, 0) AS [Avg_Logical_Reads],
    qs.total_logical_writes AS [Total_Writes],
    SUBSTRING(st.text, 1, 400) AS [Query_Snippet],
    qp.query_plan AS [Plan_XML]
FROM sys.dm_exec_query_stats qs
OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) st
OUTER APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
ORDER BY qs.total_physical_reads DESC;
