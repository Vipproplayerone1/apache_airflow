#!/usr/bin/env python3
"""
Script để xóa dữ liệu trong ClickHouse để chuẩn bị demo ETL
"""

import requests
import subprocess

def run_clickhouse_query(query):
    """Chạy query trong ClickHouse"""
    try:
        url = "http://localhost:8123"
        # Sử dụng authentication với myuser/mypassword
        params = {
            'user': 'myuser',
            'password': 'mypassword',
            'database': 'mydatabase'
        }
        response = requests.post(url, data=query, params=params)
        
        if response.status_code == 200:
            return response.text.strip()
        else:
            print(f"❌ ClickHouse error: {response.text}")
            return None
    except Exception as e:
        print(f"❌ Exception: {e}")
        return None

def clear_clickhouse_data():
    """Xóa tất cả dữ liệu trong ClickHouse"""
    print("🗑️  Clearing all data in ClickHouse...")
    
    # Thứ tự xóa theo foreign key dependencies
    tables = ['transaction', 'order_product', 'topup', 'orders', 'product', 'users', 'category']
    
    for table in tables:
        query = f"TRUNCATE TABLE {table}"
        result = run_clickhouse_query(query)
        if result is not None:
            print(f"   ✅ Cleared table: {table}")
        else:
            print(f"   ❌ Failed to clear table: {table}")
    
    print("✅ All ClickHouse data cleared!")

def verify_empty_tables():
    """Verify rằng tất cả bảng đã trống"""
    print("🔍 Verifying tables are empty...")
    
    tables = ['category', 'users', 'product', 'orders', 'topup', 'order_product', 'transaction']
    
    for table in tables:
        query = f"SELECT COUNT(*) FROM {table}"
        result = run_clickhouse_query(query)
        if result == "0":
            print(f"   ✅ {table}: 0 records (empty)")
        else:
            print(f"   ❌ {table}: {result} records (not empty)")

def check_postgresql_data():
    """Kiểm tra dữ liệu trong PostgreSQL"""
    print("📊 Checking PostgreSQL data (source)...")
    
    tables = ['category', 'users', 'product', 'orders', 'topup', 'order_product', 'transaction']
    
    for table in tables:
        if table == 'transaction':
            command = f'docker exec apache_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT COUNT(*) FROM \\"{table}\\";"'
        else:
            command = f'docker exec apache_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT COUNT(*) FROM {table};"'
        
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            count = result.stdout.strip().split('\n')[-2].strip()
            print(f"   📊 {table}: {count} records")

def main():
    """Main function"""
    print("🎯 ClickHouse Data Clearing for ETL Demo")
    print("=" * 50)
    
    print("\n1️⃣  Checking PostgreSQL data (source)...")
    check_postgresql_data()
    
    print("\n2️⃣  Clearing ClickHouse data...")
    clear_clickhouse_data()
    
    print("\n3️⃣  Verifying tables are empty...")
    verify_empty_tables()
    
    print("\n✅ ClickHouse cleared! Ready for ETL demo!")
    print("\n📋 Next steps:")
    print("   1. Go to Airflow UI: http://localhost:8080")
    print("   2. Login: airflow/airflow")
    print("   3. Find DAG: upsert_all_tables_daily")
    print("   4. Trigger with date range: 2024-01-01 to 2025-12-31")
    print("   5. Watch ETL process extract from PostgreSQL → ClickHouse")

if __name__ == "__main__":
    main() 