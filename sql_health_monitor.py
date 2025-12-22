"""
SQL Server Health Monitor
Author: mulkireddy
Date: December 23, 2024
Purpose: Monitor SQL Server performance metrics
"""

import pyodbc
from datetime import datetime
import time

def get_connection():
    """Create database connection"""
    server = 'localhost'
    database = 'master'
    conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes'
    return pyodbc.connect(conn_str)


def get_cpu_usage(cursor):
    """Get SQL Server CPU usage"""
    query = """
    SELECT TOP 1
        SQLProcessUtilization AS SQL_CPU_Usage,
        SystemIdle,
        100 - SystemIdle - SQLProcessUtilization AS Other_Process_CPU
    FROM (
        SELECT 
            record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS SystemIdle,
            record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS SQLProcessUtilization,
            record.value('(./Record/@id)[1]', 'int') AS record_id
        FROM (
            SELECT CAST(record AS XML) AS record
            FROM sys.dm_os_ring_buffers
            WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
        ) AS x
    ) AS y
    ORDER BY record_id DESC
    """
    cursor.execute(query)
    return cursor.fetchone()

def get_active_queries(cursor):
    """Get currently running queries"""
    query = """
    SELECT 
        session_id,
        status,
        command,
        cpu_time,
        total_elapsed_time / 1000 AS elapsed_sec,
        reads,
        writes,
        blocking_session_id,
        wait_type
    FROM sys.dm_exec_requests
    WHERE session_id > 50
    ORDER BY cpu_time DESC
    """
    cursor.execute(query)
    return cursor.fetchall()

def get_blocking(cursor):
    """Get blocking sessions"""
    query = """
    SELECT 
        blocking_session_id,
        session_id,
        wait_type,
        wait_time / 1000 AS wait_sec,
        wait_resource
    FROM sys.dm_exec_requests
    WHERE blocking_session_id <> 0
    """
    cursor.execute(query)
    return cursor.fetchall()

def display_header():
    """Display monitoring header"""
    print("=" * 80)
    print("SQL SERVER HEALTH MONITOR")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)


def display_cpu(cpu):
    """Display CPU stats"""
    print("\n[CPU]")
    print(f"SQL Server:  {cpu.SQL_CPU_Usage}%")
    print(f"Other:       {cpu.Other_Process_CPU}%")
    print(f"Idle:        {cpu.SystemIdle}%")

def display_queries(queries):
    """Display active queries"""
    print("\n[ACTIVE QUERIES]")
    if not queries:
        print("No active queries")
        return
    
    print(f"{'Session':<10} {'Status':<12} {'Command':<20} {'CPU(ms)':<10} {'Time(s)':<10} {'Blocking':<10}")
    print("-" * 80)
    for q in queries:
        print(f"{q.session_id:<10} {q.status:<12} {q.command:<20} {q.cpu_time:<10} {q.elapsed_sec:<10} {q.blocking_session_id:<10}")

def display_blocking(blocking):
    """Display blocking sessions"""
    print("\n[BLOCKING]")
    if not blocking:
        print("No blocking detected")
        return
    
    print(f"{'Blocker':<10} {'Blocked':<10} {'Wait Type':<20} {'Wait(s)':<10} {'Resource':<30}")
    print("-" * 80)
    for b in blocking:
        print(f"{b.blocking_session_id:<10} {b.session_id:<10} {b.wait_type:<20} {b.wait_sec:<10} {b.wait_resource:<30}")

def monitor_once():
    """Run one monitoring cycle"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        display_header()
        
      
        
        cpu = get_cpu_usage(cursor)
        display_cpu(cpu)
        
        queries = get_active_queries(cursor)
        display_queries(queries)
        
        blocking = get_blocking(cursor)
        display_blocking(blocking)
        
        conn.close()
        
    except Exception as e:
        print(f"\n[ERROR] {e}")

def monitor_continuous(interval=5, duration=60):
    """Monitor continuously"""
    print(f"Starting continuous monitoring (every {interval}s for {duration}s)")
    print("Press Ctrl+C to stop\n")
    
    end_time = time.time() + duration
    
    try:
        while time.time() < end_time:
            monitor_once()
            print(f"\nRefreshing in {interval} seconds...")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user")

def main():
    """Main function"""
    print("SQL Server Health Monitor")
    print("1. Single snapshot")
    print("2. Continuous monitoring (60 seconds)")
    
    choice = input("\nSelect option (1 or 2): ").strip()
    
    if choice == "1":
        monitor_once()
    elif choice == "2":
        monitor_continuous(interval=5, duration=60)
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()