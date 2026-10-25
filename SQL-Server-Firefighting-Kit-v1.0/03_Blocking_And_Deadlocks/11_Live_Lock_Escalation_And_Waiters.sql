/*========================================================================================================================
SCRIPT:       11_Live_Lock_Escalation_And_Waiters.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Shows granted and waiting locks by Database and Object to spot table-level Lock Escalations.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (50)
    tl.request_session_id AS [SPID],
    DB_NAME(tl.resource_database_id) AS [Database_Name],
    tl.resource_type AS [Resource_Type],
    CASE
        WHEN tl.resource_type = N'OBJECT' THEN OBJECT_NAME(tl.resource_associated_entity_id, tl.resource_database_id)
        ELSE CAST(tl.resource_associated_entity_id AS NVARCHAR(50))
    END AS [Resource_Entity],
    tl.request_mode AS [Lock_Mode],
    tl.request_status AS [Lock_Status],
    COUNT(*) AS [Lock_Count]
FROM sys.dm_tran_locks tl
WHERE tl.request_session_id <> @@SPID
  AND tl.resource_type <> N'DATABASE'
GROUP BY tl.request_session_id, tl.resource_database_id, tl.resource_type,
         tl.resource_associated_entity_id, tl.request_mode, tl.request_status
ORDER BY CASE WHEN tl.request_status = N'WAIT' THEN 0 ELSE 1 END, [Lock_Count] DESC;
