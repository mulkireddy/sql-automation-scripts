/*========================================================================================================================
SCRIPT:       14_PLE_And_Memory_Clerk_Hog_Analyzer.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Displays Page Life Expectancy (PLE) and Top Memory Clerks consuming SQL Server RAM.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    object_name AS [Counter_Object],
    instance_name AS [NUMA_Node],
    cntr_value AS [Page_Life_Expectancy_Sec],
    CASE WHEN cntr_value < 300 THEN N'CRITICAL: Buffer Pool Thrashing (<300s)' ELSE N'Healthy' END AS [PLE_Status]
FROM sys.dm_os_performance_counters
WHERE object_name LIKE N'%Buffer Node%' AND RTRIM(counter_name) = N'Page life expectancy';

SELECT TOP (15)
    type AS [Memory_Clerk_Type],
    name AS [Clerk_Name],
    CAST(SUM(pages_kb) / 1024.0 AS DECIMAL(14,2)) AS [Memory_Used_MB]
FROM sys.dm_os_memory_clerks
GROUP BY type, name
ORDER BY SUM(pages_kb) DESC;
