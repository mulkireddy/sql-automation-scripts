/*========================================================================================================================
SCRIPT:       18_Buffer_Pool_Dirty_Page_Usage_By_DB.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Breaks down Buffer Pool RAM usage and Dirty Pages per database.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    CASE WHEN database_id = 32767 THEN N'ResourceDB' ELSE DB_NAME(database_id) END AS [Database_Name],
    CAST(COUNT(*) * 8.0 / 1024.0 AS DECIMAL(14,2)) AS [Total_Buffer_Pool_MB],
    CAST(SUM(CASE WHEN is_modified = 1 THEN 1 ELSE 0 END) * 8.0 / 1024.0 AS DECIMAL(14,2)) AS [Dirty_Pages_MB],
    CAST(SUM(CASE WHEN is_modified = 0 THEN 1 ELSE 0 END) * 8.0 / 1024.0 AS DECIMAL(14,2)) AS [Clean_Pages_MB]
FROM sys.dm_os_buffer_descriptors
GROUP BY database_id
ORDER BY [Total_Buffer_Pool_MB] DESC;
