# THE 60-SECOND SQL SERVER EMERGENCY DECISION TREE (v1.0)

**GOLDEN RULE #1:** DO NOT RESTART THE SQL SERVER SERVICE BEFORE RUNNING SCRIPT #01. Restarting clears DMVs, destroys forensic evidence, and triggers long crash recovery/rollback loops.  
**GOLDEN RULE #2:** Always start by running `01_First_60_Seconds_Triage.sql`, inspect Result Set 1 (`[Triage_Verdict]`) and Result Set 4 (`[Wait_Category]`), then follow the matching branch below:

```text
                        +----------------------------------------------+
                        |     ALERT FIRES / APP TIMEOUTS REPORTED      |
                        |      Run: 01_First_60_Seconds_Triage.sql     |
                        +----------------------+-----------------------+
                                               |
        +------------------+-------------------+-------------------+------------------+
        |                  |                   |                   |                  |
        v                  v                   v                   v                  v
  [Blocked >= 5]     [SQL CPU > 85%]   [Mem Grants > 0]   [TempDB Used > 85%]  [Disk/Log Waits]
  Wait: LCK_M_*      Wait: SOS_SCHED   Wait: RESOURCE_    Error 1101 / 1105    Wait: WRITELOG /
                           CXPACKET          SEMAPHORE     or Error 9002        PAGEIOLATCH_*
        |                  |                   |                   |                  |
        v                  v                   v                   v                  v
  BRANCH A:          BRANCH B:           BRANCH C:           BRANCH D:          BRANCH E:
  Folder 03          Folder 02           Script #14 & #15    Folder 05          Script #16 & #17
```

---

## Quick Symptom-to-Script Lookup Matrix

| Emergency Symptom | Primary Script to Run Immediately | Secondary Follow-Up Script |
| :--- | :--- | :--- |
| **"Everything is hung / timing out!"** | `01_First_60_Seconds_Triage.sql` | `02_Server_Vital_Signs_Dashboard.sql` |
| **CPU Pinned at 90% - 100%** | `04_Top_Active_CPU_Burners_Live.sql` | `06_Parameter_Sniffing_Detector_And_Fix.sql` |
| **Severe Blocking Pile-Up** | `09_Lead_Blocker_Tree_And_Kill_Generator.sql` | `10_Open_Sleeping_Transactions_With_Locks.sql` |
| **Deadlock Errors (Error 1205)** | `12_SystemHealth_Deadlock_XML_Decoder.sql` | `11_Live_Lock_Escalation_And_Waiters.sql` |
| **Queries Waiting on Memory (RESOURCE_SEMAPHORE)** | `15_Memory_Grant_Pending_Queue_Rescue.sql` | `14_PLE_And_Memory_Clerk_Hog_Analyzer.sql` |
| **Disk Latency / Storage Thrashing** | `16_Disk_Latency_Stall_By_Database_File.sql` | `17_Top_Physical_IO_Thrashing_Queries.sql` |
| **TempDB Full (Error 1101 / 1105)** | `19_TempDB_Space_Hog_By_Session_And_Task.sql` | `20_Version_Store_Runaway_Transaction_Finder.sql` |
| **Transaction Log Full (Error 9002)** | `22_Transaction_Log_Full_Why_Wont_It_Truncate.sql` | `24_Long_Running_Uncommitted_Transaction_Sniper.sql` |
| **Always On AG Synchronization Lag** | `25_AlwaysOn_AG_Redo_And_Send_Queue_Lag.sql` | `16_Disk_Latency_Stall_By_Database_File.sql` |
| **Corruption Alert (Error 823 / 824 / 825)** | `26_Suspect_Pages_And_I_O_Error_Detector.sql` | `03_Recent_ErrorLog_Critical_Scanner.sql` |
