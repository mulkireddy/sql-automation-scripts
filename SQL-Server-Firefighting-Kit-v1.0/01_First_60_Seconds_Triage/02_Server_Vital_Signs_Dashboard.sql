/*========================================================================================================================
SCRIPT:       02_Server_Vital_Signs_Dashboard.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Instance Config Sanity Check, OS Memory State, 30-Min CPU Timeline, and Database State Check.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 2000;

DECLARE @StartTime DATETIME, @UptimeHours DECIMAL(10,1);
DECLARE @TotalOSRamMB DECIMAL(18,1), @AvailOSRamMB DECIMAL(18,1), @OSMemState NVARCHAR(120);
DECLARE @SqlCommittedMB DECIMAL(18,1), @SqlTargetMB DECIMAL(18,1);

SELECT @StartTime = sqlserver_start_time,
       @UptimeHours = DATEDIFF(MINUTE, sqlserver_start_time, GETDATE()) / 60.0,
       @SqlCommittedMB = committed_kb / 1024.0,
       @SqlTargetMB = committed_target_kb / 1024.0
FROM sys.dm_os_sys_info;

SELECT @TotalOSRamMB = total_physical_memory_kb / 1024.0,
       @AvailOSRamMB = available_physical_memory_kb / 1024.0,
       @OSMemState   = system_memory_state_desc
FROM sys.dm_os_sys_memory;

SELECT
    @@SERVERNAME AS [Server_Name],
    CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(50)) AS [Product_Version],
    CAST(SERVERPROPERTY('Edition') AS NVARCHAR(100)) AS [Edition],
    @StartTime AS [Last_Startup_Time],
    @UptimeHours AS [Uptime_Hours],
    osi.cpu_count AS [Logical_CPUs],
    @TotalOSRamMB AS [Total_OS_RAM_MB],
    @AvailOSRamMB AS [Available_OS_RAM_MB],
    @OSMemState AS [OS_Memory_State],
    @SqlCommittedMB AS [SQL_Committed_RAM_MB],
    CAST(cfg_maxmem.value_in_use AS INT) AS [Configured_Max_Server_Mem_MB],
    CAST(cfg_maxdop.value_in_use AS INT) AS [Configured_MAXDOP],
    CAST(cfg_ctp.value_in_use AS INT) AS [Cost_Threshold_Parallelism]
FROM sys.dm_os_sys_info osi
CROSS JOIN (SELECT value_in_use FROM sys.configurations WHERE name = N'max server memory (MB)') cfg_maxmem
CROSS JOIN (SELECT value_in_use FROM sys.configurations WHERE name = N'max degree of parallelism') cfg_maxdop
CROSS JOIN (SELECT value_in_use FROM sys.configurations WHERE name = N'cost threshold for parallelism') cfg_ctp;

-- RESULT SET 2: 30-MINUTE CPU TIMELINE
DECLARE @ts_now BIGINT;
SELECT @ts_now = cpu_ticks / (cpu_ticks / ms_ticks) FROM sys.dm_os_sys_info;

SELECT TOP (30)
    CONVERT(VARCHAR(19), DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()), 120) AS [Event_Time],
    SQLProcessUtilization AS [SQL_Server_CPU_Pct],
    100 - SystemIdle - SQLProcessUtilization AS [Non_SQL_OS_CPU_Pct],
    SystemIdle AS [Idle_CPU_Pct],
    REPLICATE(N'#', SQLProcessUtilization / 5) AS [SQL_CPU_Visual_Bar]
FROM (
    SELECT
        record.value('(./Record/@id)[1]', 'int') AS record_id,
        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS [SystemIdle],
        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS [SQLProcessUtilization],
        [timestamp]
    FROM (
        SELECT [timestamp], CONVERT(XML, record) AS [record]
        FROM sys.dm_os_ring_buffers
        WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' AND record LIKE N'%<Record%'
    ) AS x
) AS y
ORDER BY record_id DESC;

-- RESULT SET 3: DATABASE HEALTH
SELECT
    d.database_id, d.name AS [Database_Name], d.state_desc AS [DB_State],
    d.user_access_desc AS [Access_Mode], d.recovery_model_desc AS [Recovery_Model],
    d.log_reuse_wait_desc AS [Log_Reuse_Wait], d.is_auto_close_on, d.is_auto_shrink_on
FROM sys.databases d
ORDER BY CASE WHEN d.state_desc <> N'ONLINE' THEN 0 ELSE 1 END, d.name;
