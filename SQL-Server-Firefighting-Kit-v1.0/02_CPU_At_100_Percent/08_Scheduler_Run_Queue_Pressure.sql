/*========================================================================================================================
SCRIPT:       08_Scheduler_Run_Queue_Pressure.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Checks sys.dm_os_schedulers for runnable tasks waiting for a CPU core and worker thread exhaustion.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    parent_node_id AS [NUMA_Node],
    scheduler_id AS [Scheduler_ID],
    cpu_id AS [CPU_ID],
    status AS [Status],
    current_tasks_count AS [Current_Tasks],
    runnable_tasks_count AS [Runnable_Tasks_Waiting_For_CPU],
    current_workers_count AS [Current_Workers],
    active_workers_count AS [Active_Workers],
    work_queue_count AS [Thread_Starvation_Work_Queue],
    pending_disk_io_count AS [Pending_Disk_IO]
FROM sys.dm_os_schedulers
WHERE scheduler_id < 255
ORDER BY runnable_tasks_count DESC, current_tasks_count DESC;
