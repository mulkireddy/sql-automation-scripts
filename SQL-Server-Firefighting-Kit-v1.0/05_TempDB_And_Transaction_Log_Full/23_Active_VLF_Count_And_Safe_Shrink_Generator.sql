/*========================================================================================================================
SCRIPT:       23_Active_VLF_Count_And_Safe_Shrink_Generator.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Counts Virtual Log Files (VLFs) per database (SQL 2016 SP2+ sys.dm_db_log_info) to spot VLF bloat.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    d.name AS [Database_Name],
    COUNT(li.database_id) AS [Total_VLF_Count],
    SUM(CASE WHEN li.vlf_active = 1 THEN 1 ELSE 0 END) AS [Active_VLFs],
    CAST(SUM(li.vlf_size_mb) AS DECIMAL(14,2)) AS [Total_Log_Size_MB],
    CASE WHEN COUNT(li.database_id) > 1000 THEN N'WARNING: Excessive VLF Fragmentation (>1000 VLFs)' ELSE N'OK' END AS [VLF_Health]
FROM sys.databases d
CROSS APPLY sys.dm_db_log_info(d.database_id) li
GROUP BY d.name
ORDER BY [Total_VLF_Count] DESC;
