/*========================================================================================================================
SCRIPT:       26_Suspect_Pages_And_I_O_Error_Detector.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Inspects msdb.dbo.suspect_pages for 823/824/825 corruption errors and decodes the Page Status.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    DB_NAME(sp.database_id) AS [Database_Name],
    sp.database_id,
    sp.file_id,
    mf.name AS [Logical_File_Name],
    mf.physical_name AS [Physical_File_Path],
    sp.page_id AS [Corrupted_Page_ID],
    CASE sp.event_type
        WHEN 1 THEN N'823 or 824 I/O Error'
        WHEN 2 THEN N'Bad Checksum'
        WHEN 3 THEN N'Torn Page'
        WHEN 4 THEN N'Restored (Fixed)'
        WHEN 5 THEN N'Repaired by DBCC'
        WHEN 7 THEN N'Deallocated by DBCC'
        ELSE N'Unknown'
    END AS [Corruption_Type],
    sp.error_count,
    sp.last_update_date
FROM msdb.dbo.suspect_pages sp
LEFT JOIN sys.master_files mf ON sp.database_id = mf.database_id AND sp.file_id = mf.file_id
ORDER BY sp.last_update_date DESC;
