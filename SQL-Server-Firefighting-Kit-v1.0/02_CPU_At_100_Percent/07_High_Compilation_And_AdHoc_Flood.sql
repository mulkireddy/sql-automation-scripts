/*========================================================================================================================
SCRIPT:       07_High_Compilation_And_AdHoc_Flood.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Measures single-use Ad-Hoc plan cache bloat and compilation pressure.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    objtype AS [Cache_Object_Type],
    COUNT(*) AS [Total_Cached_Plans],
    SUM(CASE WHEN usecounts = 1 THEN 1 ELSE 0 END) AS [Single_Use_Plans],
    CAST(SUM(CAST(size_in_bytes AS BIGINT)) / 1048576.0 AS DECIMAL(12,2)) AS [Total_Cache_Size_MB],
    CAST(SUM(CASE WHEN usecounts = 1 THEN CAST(size_in_bytes AS BIGINT) ELSE 0 END) / 1048576.0 AS DECIMAL(12,2)) AS [Single_Use_Bloat_MB],
    N'EXEC sp_configure ''optimize for ad hoc workloads'', 1; RECONFIGURE;' AS [Remediation_Command]
FROM sys.dm_exec_cached_plans
GROUP BY objtype
ORDER BY [Single_Use_Bloat_MB] DESC;
