"""
SQL Server Connection Test Script
Author: mulkireddy
Date: December 22, 2024
Purpose: Verify Python can connect to SQL Server using pyodbc
"""

import pyodbc

def test_connection():
    """Test basic SQL Server connectivity"""
    
    # Configuration - CHANGE THESE to match your SQL Server
    server = 'localhost'  # or (local), .\SQLEXPRESS, YOUR-COMPUTER-NAME
    database = 'master'
    
    print("=" * 70)
    print("SQL Server Connection Test")
    print("=" * 70)
    print(f"Server: {server}")
    print(f"Database: {database}")
    print(f"Authentication: Windows (Trusted Connection)")
    print("-" * 70)
    
    try:
        # Build connection string for Windows Authentication
        connection_string = (
            f'DRIVER={{SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'Trusted_Connection=yes;'
        )
        
        print("Attempting connection...")
        
        # Connect
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Test query - Get SQL Server version
        cursor.execute("SELECT @@VERSION AS Version")
        version = cursor.fetchone()
        
        print("\n✓ CONNECTION SUCCESSFUL!")
        print("-" * 70)
        print("SQL Server Version:")
        print(version[0])
        print("-" * 70)
        
        # Get list of databases
        cursor.execute("""
            SELECT 
                name AS DatabaseName,
                create_date AS Created,
                state_desc AS Status
            FROM sys.databases
            ORDER BY name
        """)
        
        print("\nDatabases on this server:")
        print(f"{'Database Name':<30} | {'Created':<20} | {'Status':<10}")
        print("-" * 70)
        
        for row in cursor:
            print(f"{row.DatabaseName:<30} | {str(row.Created):<20} | {row.Status:<10}")
        
        # Close connection
        conn.close()
        print("\n✓ Connection closed successfully")
        print("=" * 70)
        
    except pyodbc.Error as e:
        print("\n✗ CONNECTION FAILED!")
        print(f"Error: {e}")
        print("\nTroubleshooting tips:")
        print("1. Is SQL Server running? Check services.msc")
        print("2. Try different server names: 'localhost', '(local)', '.\\SQLEXPRESS'")
        print("3. Is Windows Authentication enabled on SQL Server?")
        print("4. Do you have permission to connect?")
        print("5. Is TCP/IP enabled in SQL Server Configuration Manager?")
        
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")

if __name__ == "__main__":
    test_connection()
    input("\nPress Enter to exit...")