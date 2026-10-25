/*========================================================================================================================
SCRIPT:       25_AlwaysOn_AG_Redo_And_Send_Queue_Lag.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Checks Always On Availability Group Send Queue, Redo Queue, and Synchronization Health across replicas.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    ag.name AS [AG_Name],
    ar.replica_server_name AS [Replica_Server],
    DB_NAME(drs.database_id) AS [Database_Name],
    ars.role_desc AS [Role],
    drs.synchronization_state_desc AS [Sync_State],
    drs.synchronization_health_desc AS [Sync_Health],
    drs.log_send_queue_size AS [Send_Queue_KB],
    drs.redo_queue_size AS [Redo_Queue_KB],
    drs.redo_rate AS [Redo_Rate_KB_Sec],
    CAST(drs.redo_queue_size * 1.0 / NULLIF(drs.redo_rate, 0) AS DECIMAL(12,1)) AS [Est_Redo_Catchup_Sec],
    drs.last_commit_time AS [Last_Commit_Time]
FROM sys.dm_hadr_database_replica_states drs
INNER JOIN sys.availability_replicas ar ON drs.replica_id = ar.replica_id
INNER JOIN sys.availability_groups ag ON drs.group_id = ag.group_id
LEFT JOIN sys.dm_hadr_availability_replica_states ars ON drs.replica_id = ars.replica_id
ORDER BY drs.redo_queue_size DESC, drs.log_send_queue_size DESC;
