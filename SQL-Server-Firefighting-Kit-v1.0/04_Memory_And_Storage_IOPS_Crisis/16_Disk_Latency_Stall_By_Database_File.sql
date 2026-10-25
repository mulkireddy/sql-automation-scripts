/*========================================================================================================================
SCRIPT:       16_Disk_Latency_Stall_By_Database_File.sql
TOOLKIT:      The SQL Server Firefighting & Triage Kit (v1.0)
PURPOSE:      Calculates Read & Write Disk Latency (ms) per database file to pinpoint slow SAN/Cloud storage volumes.
========================================================================================================================*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    DB_NAME(vfs.database_id) AS [Database_Name],
    mf.name AS [Logical_File_Name],
    mf.type_desc AS [File_Type],
    LEFT(mf.physical_name, 3) AS [Drive],
    mf.physical_name AS [Physical_Path],
    CAST(vfs.size_on_disk_bytes / 1048576.0 AS DECIMAL(14,2)) AS [File_Size_MB],
    CAST(vfs.io_stall_read_ms / NULLIF(vfs.num_of_reads, 0) AS DECIMAL(12,2)) AS [Avg_Read_Latency_ms],
    CAST(vfs.io_stall_write_ms / NULLIF(vfs.num_of_writes, 0) AS DECIMAL(12,2)) AS [Avg_Write_Latency_ms],
    CASE
        WHEN (vfs.io_stall_write_ms / NULLIF(vfs.num_of_writes, 0)) > 20 THEN N'CRITICAL: High Write Latency (>20ms)'
        WHEN (vfs.io_stall_read_ms / NULLIF(vfs.num_of_reads, 0)) > 20 THEN N'WARNING: High Read Latency (>20ms)'
        ELSE N'Healthy (<20ms)'
    END AS [Storage_Verdict]
FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
INNER JOIN sys.master_files mf ON vfs.database_id = mf.database_id AND vfs.file_id = mf.file_id
ORDER BY (vfs.io_stall_read_ms + vfs.io_stall_write_ms) DESC;
