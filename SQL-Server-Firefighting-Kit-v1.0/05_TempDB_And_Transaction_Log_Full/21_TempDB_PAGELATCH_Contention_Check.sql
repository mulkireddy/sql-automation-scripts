/*========================================================================================================================
SCRIPT:       21_TempDB_PAGELATCH_Contention_Check.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Checks TempDB file layout uniformity and active PAGELATCH allocation contention on PFS/GAM/SGAM pages.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    file_id,
    name AS [Logical_Name],
    type_desc,
    CAST(size * 8.0 / 1024.0 AS DECIMAL(12,2)) AS [Size_MB],
    is_percent_growth,
    growth
FROM tempdb.sys.database_files;

SELECT
    session_id AS [SPID],
    wait_type,
    wait_time AS [Wait_ms],
    wait_resource
FROM sys.dm_exec_requests
WHERE wait_type LIKE N'PAGELATCH_%'
  AND wait_resource LIKE N'2:%';
