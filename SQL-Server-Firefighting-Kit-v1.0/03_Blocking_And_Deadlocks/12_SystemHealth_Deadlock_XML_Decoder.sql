/*========================================================================================================================
SCRIPT:       12_SystemHealth_Deadlock_XML_Decoder.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Extracts the latest 25 Deadlock XML Graphs directly from the built-in system_health Extended Event ring buffer.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH SystemHealthXML AS (
    SELECT CAST(target_data AS XML) AS TargetData
    FROM sys.dm_xe_session_targets st
    INNER JOIN sys.dm_xe_sessions s ON s.address = st.event_session_address
    WHERE s.name = N'system_health'
      AND st.target_name = N'ring_buffer'
)
SELECT TOP (25)
    XEvent.value('(@timestamp)[1]', 'datetime2') AS [UTC_Deadlock_Time],
    XEvent.query('(data/value/deadlock)[1]') AS [Click_To_Open_Deadlock_Graph_XML]
FROM SystemHealthXML
CROSS APPLY TargetData.nodes('//RingBufferTarget/event[@name="xml_deadlock_report"]') AS XEventData(XEvent)
ORDER BY [UTC_Deadlock_Time] DESC;
