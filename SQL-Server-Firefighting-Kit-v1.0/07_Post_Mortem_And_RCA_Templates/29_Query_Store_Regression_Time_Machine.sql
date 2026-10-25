/*========================================================================================================================
SCRIPT:       29_Query_Store_Regression_Time_Machine.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Finds queries in the current database's Query Store with multiple plans where CPU regressed in last 24h.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (20)
    q.query_id,
    qt.query_sql_text,
    COUNT(DISTINCT p.plan_id) AS [Distinct_Plan_Count],
    MIN(p.plan_id) AS [Older_Plan_ID],
    MAX(p.plan_id) AS [Newer_Plan_ID],
    CAST(MIN(rs.avg_cpu_time) / 1000.0 AS DECIMAL(14,2)) AS [Best_Plan_Avg_CPU_ms],
    CAST(MAX(rs.avg_cpu_time) / 1000.0 AS DECIMAL(14,2)) AS [Worst_Plan_Avg_CPU_ms],
    N'EXEC sys.sp_query_store_force_plan @query_id = ' + CAST(q.query_id AS NVARCHAR(20))
    + N', @plan_id = ' + CAST(MIN(p.plan_id) AS NVARCHAR(20)) + N';' AS [Force_Good_Plan_Cmd]
FROM sys.query_store_query q
INNER JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
INNER JOIN sys.query_store_plan p ON q.query_id = p.query_id
INNER JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
INNER JOIN sys.query_store_runtime_stats_interval rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
WHERE rsi.start_time >= DATEADD(HOUR, -24, GETUTCDATE())
GROUP BY q.query_id, qt.query_sql_text
HAVING COUNT(DISTINCT p.plan_id) > 1
   AND MAX(rs.avg_cpu_time) > (MIN(rs.avg_cpu_time) * 3)
ORDER BY (MAX(rs.avg_cpu_time) - MIN(rs.avg_cpu_time)) DESC;
