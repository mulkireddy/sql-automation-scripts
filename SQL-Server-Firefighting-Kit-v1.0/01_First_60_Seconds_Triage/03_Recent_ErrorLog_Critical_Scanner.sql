/*========================================================================================================================
SCRIPT:       03_Recent_ErrorLog_Critical_Scanner.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Fast scan of SQL Server Error Log (Last 24 Hours) for critical outages without freezing SSMS.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @StartTime DATETIME = DATEADD(HOUR, -24, GETDATE());
DECLARE @EndTime   DATETIME = GETDATE();

IF OBJECT_ID('tempdb..#RawErrorLog') IS NOT NULL DROP TABLE #RawErrorLog;
CREATE TABLE #RawErrorLog (LogDate DATETIME, ProcessInfo NVARCHAR(100), ErrorText NVARCHAR(MAX));

INSERT INTO #RawErrorLog (LogDate, ProcessInfo, ErrorText)
EXEC sys.xp_readerrorlog 0, 1, NULL, NULL, @StartTime, @EndTime, N'desc';

DELETE FROM #RawErrorLog
WHERE ErrorText LIKE N'Log was backed up.%'
   OR ErrorText LIKE N'Database backed up.%'
   OR ErrorText LIKE N'BACKUP DATABASE successfully%'
   OR ErrorText LIKE N'DBCC CHECKDB%found 0 errors%';

SELECT TOP (100)
    CONVERT(VARCHAR(19), LogDate, 120) AS [Log_Timestamp],
    ProcessInfo AS [Source],
    ErrorText AS [Raw_Error_Message]
FROM #RawErrorLog
WHERE ErrorText LIKE N'%Error:%' OR ErrorText LIKE N'%corrupt%' OR ErrorText LIKE N'%longer than 15 seconds%'
   OR ErrorText LIKE N'%full%' OR ErrorText LIKE N'%Stack Dump%' OR ErrorText LIKE N'%deadlock%'
   OR ErrorText LIKE N'%Availability Group%' OR ErrorText LIKE N'%failover%' OR ErrorText LIKE N'%Login failed%'
ORDER BY LogDate DESC;

SELECT TOP (25)
    COUNT(*) AS [Occurrence_Count],
    CONVERT(VARCHAR(19), MIN(LogDate), 120) AS [First_Seen],
    CONVERT(VARCHAR(19), MAX(LogDate), 120) AS [Last_Seen],
    LEFT(ErrorText, 200) AS [Error_Pattern_Summary]
FROM #RawErrorLog
GROUP BY LEFT(ErrorText, 200)
HAVING COUNT(*) >= 2
ORDER BY [Occurrence_Count] DESC;

DROP TABLE #RawErrorLog;
