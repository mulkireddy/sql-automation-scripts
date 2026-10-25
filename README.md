# SQL Server Automation & Troubleshooting Scripts

A comprehensive collection of production-ready Microsoft SQL Server automation, performance diagnostic, and emergency firefighting scripts.

---

## 🚀 The SQL Server Firefighting & Triage Kit (v1.0)

A standardized toolkit of **30 read-only emergency scripts** and Standard Operating Procedures (SOPs) designed to triage live production database incidents in 60 seconds or less.

### Directory Structure & Script Matrix

```text
SQL-Server-Firefighting-Kit-v1.0/
├── 00_START_HERE/
│   ├── 00_Emergency_Decision_Tree_Flowchart.md    # 60-second triage decision tree & symptom matrix
│   ├── 01_The_SQL_Firefighting_Field_Manual.md     # Production DBA SOP field manual
│   └── 02_Corporate_Expense_Reimbursement_Letter.txt # Team expense justification template
├── 01_First_60_Seconds_Triage/
│   ├── 01_First_60_Seconds_Triage.sql            # Instant vital signs, blockers, CPU, & wait summary
│   ├── 02_Server_Vital_Signs_Dashboard.sql       # OS memory, 30-min ring buffer CPU history, DB states
│   └── 03_Recent_ErrorLog_Critical_Scanner.sql   # Fast 24-hr Error Log scanner (823/824/825, 9002, I/O)
├── 02_CPU_At_100_Percent/
│   ├── 04_Top_Active_CPU_Burners_Live.sql        # Live CPU-burning sessions & single-plan eviction
│   ├── 05_Implicit_Conversion_And_Scan_Hunter.sql# Identifies CONVERT_IMPLICIT & costly scans
│   ├── 06_Parameter_Sniffing_Detector_And_Fix.sql# High variance plan hunter with eviction hints
│   ├── 07_High_Compilation_And_AdHoc_Flood.sql   # Measures single-use ad-hoc cache bloat
│   └── 08_Scheduler_Run_Queue_Pressure.sql       # CPU runnable task depth & thread exhaustion
├── 03_Blocking_And_Deadlocks/
│   ├── 09_Lead_Blocker_Tree_And_Kill_Generator.sql # Recursive blocking tree with auto-generated KILLs
│   ├── 10_Open_Sleeping_Transactions_With_Locks.sql# Catches idle sessions with uncommitted transactions
│   ├── 11_Live_Lock_Escalation_And_Waiters.sql   # Object/Page lock escalation analyzer
│   ├── 12_SystemHealth_Deadlock_XML_Decoder.sql  # Extracts deadlock graphs from system_health XE
│   └── 13_Application_Connection_Pool_Storm_Check.sql # Connection pool leak & storm detection
├── 04_Memory_And_Storage_IOPS_Crisis/
│   ├── 14_PLE_And_Memory_Clerk_Hog_Analyzer.sql  # Page Life Expectancy (PLE) & memory clerk breakdown
│   ├── 15_Memory_Grant_Pending_Queue_Rescue.sql  # RESOURCE_SEMAPHORE wait queue debugger
│   ├── 16_Disk_Latency_Stall_By_Database_File.sql# Virtual file stats read/write latency metrics
│   ├── 17_Top_Physical_IO_Thrashing_Queries.sql  # Plan cache physical read/write hogs
│   └── 18_Buffer_Pool_Dirty_Page_Usage_By_DB.sql # Buffer pool memory & dirty pages per database
├── 05_TempDB_And_Transaction_Log_Full/
│   ├── 19_TempDB_Space_Hog_By_Session_And_Task.sql # User objects, internal spills, & version store hogs
│   ├── 20_Version_Store_Runaway_Transaction_Finder.sql # Oldest RCSI/Snapshot transactions
│   ├── 21_TempDB_PAGELATCH_Contention_Check.sql  # Allocation page contention (PFS/GAM/SGAM)
│   ├── 22_Transaction_Log_Full_Why_Wont_It_Truncate.sql # Diagnoses log_reuse_wait_desc & remediation
│   ├── 23_Active_VLF_Count_And_Safe_Shrink_Generator.sql # Virtual Log File (VLF) count & health
│   └── 24_Long_Running_Uncommitted_Transaction_Sniper.sql # Log bytes consumed by open transactions
├── 06_HADR_Replication_And_Corruption_Rescue/
│   ├── 25_AlwaysOn_AG_Redo_And_Send_Queue_Lag.sql# AG Send/Redo queues & estimated catchup time
│   ├── 26_Suspect_Pages_And_I_O_Error_Detector.sql# msdb suspect_pages corruption resolver
│   ├── 27_Backup_And_Restore_ETA_Progress_Tracker.sql # Live ETA & % complete for backup/restore/DBCC
│   └── 28_Ghost_Cleanup_And_Rollback_Progress_Tracker.sql # Rollback ETA for killed transactions
└── 07_Post_Mortem_And_RCA_Templates/
    ├── 29_Query_Store_Regression_Time_Machine.sql# Identifies plan regressions & force plan commands
    ├── 30_Wait_Stats_Delta_60_Second_Sampler.sql # 10-second live wait stats delta sampler
    └── Incident_Post_Mortem_RCA_Template.md      # Executive Post-Mortem & 5 Whys RCA template
```

### Automation & Builders
* **`Build-SQLFirefightingKit.ps1`**: PowerShell script to generate all folders, scripts, and build `SQL-Server-Firefighting-Kit-v1.0.zip`.
* **`build_kit.py`**: Python-based builder for cross-platform archive creation.
* **`SQL-Server-Firefighting-Kit-v1.0.zip`**: Ready-to-deploy archive containing all 34 files.

---

## 🛠️ General SQL Automation & Diagnostic Tools

* **`SQL_Performance_Diagnostic_Scripts.sql`**: Consolidated collection of performance tuning and diagnostic queries.
* **`sql_health_monitor.py`**: Python script to monitor SQL Server health and connectivity.
* **`test_connection.py`**: Python script to validate database connection and driver configurations.
* **`SQL Server — High CPU Troubleshooting Guide`**: In-depth documentation on investigating CPU bottlenecks.
* **`SQL Server — IO_COMPLETION Wait Type`**: Diagnostic notes on `IO_COMPLETION` wait events.
* **`DB_Refresh_repl_automation.txt`**: Database refresh and replication automation notes.
* **`Check database file & its Table sizes.txt`**: File and table storage utilization checks.
* **`Query to check and add the missing rows between two tables`**: Data comparison and sync utility.
* **`Query to find queries that would benefit from indexes.txt`**: Missing index discovery query.
* **`Top 10 CPU queries with execution count.txt`**: High-impact CPU query extraction.
* **`Find top CPU queries from the last hour in Query store`**: Query Store hourly CPU regression analysis.

---

## 🛡️ Safety & Execution Standards

All scripts in the Firefighting Kit follow strict production safety principles:
- **`READ UNCOMMITTED`** isolation to avoid acquiring shared locks.
- **`LOCK_TIMEOUT 2000`** (2 seconds) to avoid contributing to existing blocking chains.
- **`DEADLOCK_PRIORITY LOW`** so diagnostic sessions are sacrificed if a deadlock occurs.
- Fully compatible with SQL Server 2016 through 2025 and Azure SQL Managed Instance.
