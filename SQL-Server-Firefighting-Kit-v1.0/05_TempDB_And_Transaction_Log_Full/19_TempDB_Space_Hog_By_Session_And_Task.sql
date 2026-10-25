/*========================================================================================================================
SCRIPT:       19_TempDB_Space_Hog_By_Session_And_Task.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Shows global TempDB consumption (User Objects, Internal Spills, Version Store) and top hogging SPIDs.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    CAST(SUM(total_page_count) * 8.0 / 1024.0 AS DECIMAL(12,2)) AS [Total_TempDB_MB],
    CAST(SUM(unallocated_extent_page_count) * 8.0 / 1024.0 AS DECIMAL(12,2)) AS [Free_TempDB_MB],
    CAST(SUM(user_object_reserved_page_count) * 8.0 / 1024.0 AS DECIMAL(12,2)) AS [User_Objects_TempTables_MB],
    CAST(SUM(internal_object_reserved_page_count) * 8.0 / 1024.0 AS DECIMAL(12,2)) AS [Internal_Sort_Hash_Spills_MB],
    CAST(SUM(version_store_reserved_page_count) * 8.0 / 1024.0 AS DECIMAL(12,2)) AS [Version_Store_MB]
FROM tempdb.sys.dm_db_file_space_usage;

;WITH SessionTempDB AS (
    SELECT
        session_id,
        SUM(user_objects_alloc_page_count - user_objects_dealloc_page_count) * 8.0 / 1024.0 AS User_Obj_MB,
        SUM(internal_objects_alloc_page_count - internal_objects_dealloc_page_count) * 8.0 / 1024.0 AS Internal_Obj_MB
    FROM sys.dm_db_session_space_usage
    GROUP BY session_id
),
TaskTempDB AS (
    SELECT
        session_id,
        SUM(user_objects_alloc_page_count - user_objects_dealloc_page_count) * 8.0 / 1024.0 AS Active_Task_User_MB,
        SUM(internal_objects_alloc_page_count - internal_objects_dealloc_page_count) * 8.0 / 1024.0 AS Active_Task_Internal_MB
    FROM sys.dm_db_task_space_usage
    GROUP BY session_id
)
SELECT TOP (20)
    s.session_id AS [SPID],
    s.status AS [Status],
    s.login_name,
    s.host_name,
    s.program_name,
    CAST(ISNULL(st.User_Obj_MB, 0) + ISNULL(tt.Active_Task_User_MB, 0) AS DECIMAL(12,2)) AS [Net_User_TempTables_MB],
    CAST(ISNULL(st.Internal_Obj_MB, 0) + ISNULL(tt.Active_Task_Internal_MB, 0) AS DECIMAL(12,2)) AS [Net_Internal_Spills_MB],
    SUBSTRING(txt.text, 1, 400) AS [SQL_Text],
    N'KILL ' + CAST(s.session_id AS NVARCHAR(10)) + N';' AS [Kill_TempDB_Hog_Cmd]
FROM sys.dm_exec_sessions s
LEFT JOIN SessionTempDB st ON s.session_id = st.session_id
LEFT JOIN TaskTempDB tt ON s.session_id = tt.session_id
LEFT JOIN sys.dm_exec_connections c ON s.session_id = c.session_id
OUTER APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) txt
WHERE s.is_user_process = 1
ORDER BY ([Net_User_TempTables_MB] + [Net_Internal_Spills_MB]) DESC;
