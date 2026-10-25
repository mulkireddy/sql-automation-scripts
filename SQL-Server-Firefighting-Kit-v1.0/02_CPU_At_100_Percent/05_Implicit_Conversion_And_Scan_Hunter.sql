/*========================================================================================================================
SCRIPT:       05_Implicit_Conversion_And_Scan_Hunter.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Finds top CPU-burning cached plans containing CONVERT_IMPLICIT warnings or Missing Index warnings.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (25)
    DB_NAME(CAST(pa.value AS INT)) AS [Database_Name],
    qs.execution_count AS [Executions],
    CAST(qs.total_worker_time / 1000.0 / NULLIF(qs.execution_count, 0) AS DECIMAL(14,2)) AS [Avg_CPU_ms],
    CAST(qs.total_worker_time / 1000000.0 AS DECIMAL(14,2)) AS [Total_CPU_Sec],
    qs.total_logical_reads / NULLIF(qs.execution_count, 0) AS [Avg_Logical_Reads],
    CASE WHEN CAST(qp.query_plan AS NVARCHAR(MAX)) LIKE N'%CONVERT_IMPLICIT%' THEN N'YES - Implicit Conversion!' ELSE N'No' END AS [Has_Implicit_Conversion],
    CASE WHEN CAST(qp.query_plan AS NVARCHAR(MAX)) LIKE N'%MissingIndexes%' THEN N'YES - Missing Index!' ELSE N'No' END AS [Has_Missing_Index],
    SUBSTRING(st.text, 1, 500) AS [Query_Text_Snippet],
    qp.query_plan AS [Click_For_XML_Plan]
FROM (
    SELECT TOP (100) *
    FROM sys.dm_exec_query_stats
    ORDER BY total_worker_time DESC
) qs
OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) st
OUTER APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
OUTER APPLY sys.dm_exec_plan_attributes(qs.plan_handle) pa
WHERE pa.attribute = N'dbid'
  AND (
    CAST(qp.query_plan AS NVARCHAR(MAX)) LIKE N'%CONVERT_IMPLICIT%'
    OR CAST(qp.query_plan AS NVARCHAR(MAX)) LIKE N'%MissingIndexes%'
  )
ORDER BY qs.total_worker_time DESC;
