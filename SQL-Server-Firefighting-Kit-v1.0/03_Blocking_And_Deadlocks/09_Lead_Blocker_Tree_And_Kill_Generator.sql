/*========================================================================================================================
SCRIPT:       09_Lead_Blocker_Tree_And_Kill_Generator.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Builds a visual recursive blocking tree showing Root Blockers and all downstream victims.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH ActiveChain AS (
    SELECT
        s.session_id,
        ISNULL(r.blocking_session_id, 0) AS blocking_session_id,
        s.status,
        s.open_transaction_count,
        r.wait_type,
        r.wait_time / 1000.0 AS wait_sec,
        DB_NAME(COALESCE(r.database_id, s.database_id)) AS db_name,
        s.login_name,
        s.host_name,
        s.program_name,
        COALESCE(r.sql_handle, c.most_recent_sql_handle) AS sql_handle
    FROM sys.dm_exec_sessions s
    LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
    LEFT JOIN sys.dm_exec_connections c ON s.session_id = c.session_id
    WHERE s.is_user_process = 1
),
BlockingTree AS (
    SELECT
        session_id, blocking_session_id, status, open_transaction_count,
        wait_type, wait_sec, db_name, login_name, host_name, program_name, sql_handle,
        0 AS [Level],
        CAST(RIGHT('0000' + CAST(session_id AS VARCHAR(10)), 4) AS VARCHAR(MAX)) AS [Sort_Path]
    FROM ActiveChain
    WHERE blocking_session_id = 0
      AND session_id IN (SELECT blocking_session_id FROM ActiveChain WHERE blocking_session_id > 0)
    UNION ALL
    SELECT
        c.session_id, c.blocking_session_id, c.status, c.open_transaction_count,
        c.wait_type, c.wait_sec, c.db_name, c.login_name, c.host_name, c.program_name, c.sql_handle,
        bt.[Level] + 1,
        bt.[Sort_Path] + ' -> ' + RIGHT('0000' + CAST(c.session_id AS VARCHAR(10)), 4)
    FROM ActiveChain c
    INNER JOIN BlockingTree bt ON c.blocking_session_id = bt.session_id
    WHERE bt.[Level] < 20
)
SELECT
    REPLICATE(N'    |--> ', bt.[Level]) + N'SPID ' + CAST(bt.session_id AS NVARCHAR(10)) AS [Blocking_Hierarchy_Tree],
    bt.[Level] AS [Depth],
    bt.status AS [Status],
    bt.open_transaction_count AS [Open_Trans],
    ISNULL(bt.wait_type, N'ROOT BLOCKER (IDLE/RUNNING)') AS [Wait_Type],
    CAST(ISNULL(bt.wait_sec, 0) AS DECIMAL(12,1)) AS [Wait_Sec],
    bt.db_name AS [DB_Name],
    bt.login_name,
    bt.host_name,
    bt.program_name,
    SUBSTRING(st.text, 1, 400) AS [SQL_Text],
    CASE WHEN bt.[Level] = 0 THEN N'KILL ' + CAST(bt.session_id AS NVARCHAR(10)) + N';' ELSE N'-- Victim --' END AS [Root_Kill_Command]
FROM BlockingTree bt
OUTER APPLY sys.dm_exec_sql_text(bt.sql_handle) st
ORDER BY bt.[Sort_Path];
