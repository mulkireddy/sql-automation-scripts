/*========================================================================================================================
SCRIPT:       22_Transaction_Log_Full_Why_Wont_It_Truncate.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Diagnoses Error 9002 (Transaction Log Full) and tells you exactly what is preventing truncation.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    d.name AS [Database_Name],
    d.recovery_model_desc AS [Recovery_Model],
    d.log_reuse_wait_desc AS [Why_Log_Wont_Truncate],
    CASE d.log_reuse_wait_desc
        WHEN N'NOTHING' THEN N'Log is free to reuse space normally.'
        WHEN N'LOG_BACKUP' THEN N'ACTION: Take a Transaction Log Backup immediately!'
        WHEN N'ACTIVE_TRANSACTION' THEN N'ACTION: Run Script #24 to find and kill/commit the open transaction!'
        WHEN N'AVAILABILITY_REPLICA' THEN N'ACTION: Secondary AG replica is lagging -> Run Script #25!'
        WHEN N'REPLICATION' THEN N'ACTION: CDC or Transactional Replication Log Reader Agent is failing/stopped!'
        WHEN N'ACTIVE_BACKUP_OR_RESTORE' THEN N'ACTION: Wait for active Full/Diff backup to complete -> Check Script #27.'
        ELSE N'Consult Field Manual for ' + d.log_reuse_wait_desc
    END AS [Remediation_Prescription]
FROM sys.databases d
ORDER BY CASE WHEN d.log_reuse_wait_desc NOT IN (N'NOTHING') THEN 0 ELSE 1 END, d.name;
