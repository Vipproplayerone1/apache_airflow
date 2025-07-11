#!/usr/bin/env python3
"""
Script để thêm dữ liệu cho demo TIME-BASED UPSERT
Tạo dữ liệu với timestamps khác nhau để chứng minh upsert chỉ xử lý 
dữ liệu trong khoảng thời gian chỉ định.

Demo data includes:
- OLD data (ID=200): timestamp 2024-01-01 → Will NOT be upserted
- NEW data (ID=100): current timestamp → Will be upserted  
- UPDATED data (ID=1): current timestamp → Will be upserted

Chạy thông qua container Airflow
"""

import subprocess
import sys

def run_sql_in_postgres(sql_commands):
    """Chạy SQL commands trong PostgreSQL container"""
    try:
        # Tạo file SQL tạm thời
        sql_content = "\n".join(sql_commands)
        
        # Ghi SQL vào file tạm thời
        with open("temp_upsert_demo.sql", "w") as f:
            f.write(sql_content)
        
        # Copy file vào container và thực thi
        copy_cmd = ["docker", "cp", "temp_upsert_demo.sql", "apache_airflow-postgres-1:/tmp/"]
        result = subprocess.run(copy_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Error copying SQL file: {result.stderr}")
            return False
        
        # Thực thi SQL
        exec_cmd = [
            "docker", "exec", "-i", "apache_airflow-postgres-1",
            "psql", "-U", "airflow", "-d", "airflow", "-f", "/tmp/temp_upsert_demo.sql"
        ]
        result = subprocess.run(exec_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Error executing SQL: {result.stderr}")
            return False
        
        print("✅ SQL executed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error running SQL: {e}")
        return False
    finally:
        # Cleanup temp file
        try:
            import os
            os.remove("temp_upsert_demo.sql")
        except:
            pass

def add_old_data():
    """Thêm dữ liệu cũ vào PostgreSQL (quá khứ - không được upsert)"""
    print("📅 Adding OLD data to PostgreSQL (past timestamps)...")
    
    sql_commands = [
        "-- Adding OLD data for time-based upsert demo (2024-01-01)",
        """INSERT INTO category (id, name, created_at, updated_at)
           VALUES (200, 'Category OLD Data', '2024-01-01 09:55:00', '2024-01-01 09:55:00')
           ON CONFLICT (id) DO UPDATE SET
               name = EXCLUDED.name,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;""",
               
        """INSERT INTO users (id, name, email, created_at, updated_at) 
           VALUES (200, 'User OLD Data', 'old_data@example.com', '2024-01-01 10:00:00', '2024-01-01 10:00:00')
           ON CONFLICT (id) DO UPDATE SET
               name = EXCLUDED.name,
               email = EXCLUDED.email,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;""",
        
        """INSERT INTO orders (id, user_id, status, total_amount, created_at, updated_at)
           VALUES (200, 200, 'old_status', 99.99, '2024-01-01 10:05:00', '2024-01-01 10:05:00')
           ON CONFLICT (id) DO UPDATE SET
               user_id = EXCLUDED.user_id,
               status = EXCLUDED.status,
               total_amount = EXCLUDED.total_amount,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;""",
        
        """INSERT INTO product (id, name, category_id, price, created_at, updated_at)
           VALUES (200, 'Product OLD Data', 1, 49.99, '2024-01-01 10:10:00', '2024-01-01 10:10:00')
           ON CONFLICT (id) DO UPDATE SET
               name = EXCLUDED.name,
               category_id = EXCLUDED.category_id,
               price = EXCLUDED.price,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;""",
        
        """INSERT INTO topup (id, user_id, amount, created_at, updated_at)
           VALUES (200, 200, 100.00, '2024-01-01 10:15:00', '2024-01-01 10:15:00')
           ON CONFLICT (id) DO UPDATE SET
               user_id = EXCLUDED.user_id,
               amount = EXCLUDED.amount,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;""",
        
        """INSERT INTO order_product (id, order_id, product_id, quantity, price, created_at, updated_at)
           VALUES (200, 200, 200, 1, 49.99, '2024-01-01 10:20:00', '2024-01-01 10:20:00')
           ON CONFLICT (id) DO UPDATE SET
               order_id = EXCLUDED.order_id,
               product_id = EXCLUDED.product_id,
               quantity = EXCLUDED.quantity,
               price = EXCLUDED.price,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;""",
        
        """INSERT INTO transaction (id, user_id, order_id, topup_id, amount, type, created_at, updated_at)
           VALUES (200, 200, 200, 200, 99.99, 'old_purchase', '2024-01-01 10:25:00', '2024-01-01 10:25:00')
           ON CONFLICT (id) DO UPDATE SET
               user_id = EXCLUDED.user_id,
               order_id = EXCLUDED.order_id,
               topup_id = EXCLUDED.topup_id,
               amount = EXCLUDED.amount,
               type = EXCLUDED.type,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;"""
    ]
    
    if run_sql_in_postgres(sql_commands):
        print("   ✅ Added OLD category (ID=200) - timestamp: 2024-01-01")
        print("   ✅ Added OLD user (ID=200) - timestamp: 2024-01-01")
        print("   ✅ Added OLD order (ID=200) - timestamp: 2024-01-01")
        print("   ✅ Added OLD product (ID=200) - timestamp: 2024-01-01")
        print("   ✅ Added OLD topup (ID=200) - timestamp: 2024-01-01")
        print("   ✅ Added OLD order_product (ID=200) - timestamp: 2024-01-01")
        print("   ✅ Added OLD transaction (ID=200) - timestamp: 2024-01-01")
        print("   ⚠️  These records will NOT be upserted when using current date range!")
        print("   📊 All OLD records have both created_at AND updated_at = 2024-01-01")
        return True
    return False

def add_new_data():
    """Thêm dữ liệu mới vào PostgreSQL (hiện tại - sẽ được upsert)"""
    print("📝 Adding NEW data to PostgreSQL (current timestamps)...")
    
    sql_commands = [
        "-- Adding NEW data for upsert demo (current time)",
        """INSERT INTO category (id, name, created_at, updated_at)
           VALUES (100, 'Category NEW Demo', NOW(), NOW())
           ON CONFLICT (id) DO UPDATE SET
               name = EXCLUDED.name,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;""",
               
        """INSERT INTO users (id, name, email, created_at, updated_at) 
           VALUES (100, 'User Upsert Demo', 'upsert_demo@example.com', NOW(), NOW())
           ON CONFLICT (id) DO UPDATE SET
               name = EXCLUDED.name,
               email = EXCLUDED.email,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;""",
        
        """INSERT INTO orders (id, user_id, status, total_amount, created_at, updated_at)
           VALUES (100, 100, 'pending', 199.99, NOW(), NOW())
           ON CONFLICT (id) DO UPDATE SET
               user_id = EXCLUDED.user_id,
               status = EXCLUDED.status,
               total_amount = EXCLUDED.total_amount,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;""",
        
        """INSERT INTO product (id, name, category_id, price, created_at, updated_at)
           VALUES (100, 'Product Upsert Demo', 1, 299.99, NOW(), NOW())
           ON CONFLICT (id) DO UPDATE SET
               name = EXCLUDED.name,
               category_id = EXCLUDED.category_id,
               price = EXCLUDED.price,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;""",
        
        """INSERT INTO topup (id, user_id, amount, created_at, updated_at)
           VALUES (100, 100, 500.00, NOW(), NOW())
           ON CONFLICT (id) DO UPDATE SET
               user_id = EXCLUDED.user_id,
               amount = EXCLUDED.amount,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;""",
        
        """INSERT INTO order_product (id, order_id, product_id, quantity, price, created_at, updated_at)
           VALUES (100, 100, 100, 2, 299.99, NOW(), NOW())
           ON CONFLICT (id) DO UPDATE SET
               order_id = EXCLUDED.order_id,
               product_id = EXCLUDED.product_id,
               quantity = EXCLUDED.quantity,
               price = EXCLUDED.price,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;""",
        
        """INSERT INTO transaction (id, user_id, order_id, topup_id, amount, type, created_at, updated_at)
           VALUES (100, 100, 100, 100, 199.99, 'purchase', NOW(), NOW())
           ON CONFLICT (id) DO UPDATE SET
               user_id = EXCLUDED.user_id,
               order_id = EXCLUDED.order_id,
               topup_id = EXCLUDED.topup_id,
               amount = EXCLUDED.amount,
               type = EXCLUDED.type,
               created_at = EXCLUDED.created_at,
               updated_at = EXCLUDED.updated_at;"""
    ]
    
    if run_sql_in_postgres(sql_commands):
        print("   ✅ Added NEW category (ID=100) - timestamp: NOW()")
        print("   ✅ Added NEW user (ID=100) - timestamp: NOW()")
        print("   ✅ Added NEW order (ID=100) - timestamp: NOW()")
        print("   ✅ Added NEW product (ID=100) - timestamp: NOW()")
        print("   ✅ Added NEW topup (ID=100) - timestamp: NOW()")
        print("   ✅ Added NEW order_product (ID=100) - timestamp: NOW()")
        print("   ✅ Added NEW transaction (ID=100) - timestamp: NOW()")
        print("   ✅ These records WILL be upserted when using current date range!")
        print("   📊 All NEW records have both created_at AND updated_at = NOW()")
        return True
    return False

def update_existing_data():
    """Cập nhật dữ liệu hiện có trong PostgreSQL (sẽ được upsert)"""
    print("🔄 Updating existing data in PostgreSQL (will be upserted)...")
    
    sql_commands = [
        "-- Updating existing data for time-based upsert demo",
        """UPDATE category 
           SET name = 'Category Updated for Upsert', updated_at = NOW()
           WHERE id = 1;""",
           
        """UPDATE users 
           SET name = 'User Updated for Upsert', email = 'updated_upsert@example.com', updated_at = NOW()
           WHERE id = 1;""",
        
        """UPDATE orders 
           SET status = 'completed', total_amount = 1299.99, updated_at = NOW()
           WHERE id = 1;""",
        
        """UPDATE product 
           SET name = 'Product Updated for Upsert', price = 599.99, updated_at = NOW()
           WHERE id = 1;""",
        
        """UPDATE topup 
           SET amount = 1000.00, updated_at = NOW()
           WHERE id = 1;""",
        
        """UPDATE order_product 
           SET quantity = 5, price = 999.99, updated_at = NOW()
           WHERE id = 1;""",
        
        """UPDATE "transaction" 
           SET amount = 1500.00, updated_at = NOW()
           WHERE id = 1;"""
    ]
    
    if run_sql_in_postgres(sql_commands):
        print("   ✅ Updated category (ID=1) - updated_at: NOW()")
        print("   ✅ Updated user (ID=1) - updated_at: NOW()")
        print("   ✅ Updated order (ID=1) - updated_at: NOW()")
        print("   ✅ Updated product (ID=1) - updated_at: NOW()")
        print("   ✅ Updated topup (ID=1) - updated_at: NOW()")
        print("   ✅ Updated order_product (ID=1) - updated_at: NOW()")
        print("   ✅ Updated transaction (ID=1) - updated_at: NOW()")
        print("   ✅ These updates WILL be detected and upserted!")
        print("   📊 All UPDATED records now have updated_at = NOW()")
        return True
    return False

def verify_changes():
    """Verify các thay đổi trong PostgreSQL"""
    print("🔍 Verifying changes in PostgreSQL...")
    
    try:
        # Check OLD data (should NOT be upserted)
        print("\n   📅 OLD Data (2024-01-01) - Will NOT be upserted:")
        cmd = [
            "docker", "exec", "-i", "apache_airflow-postgres-1",
            "psql", "-U", "airflow", "-d", "airflow", "-t", "-c",
            "SELECT COUNT(*) FROM users WHERE id = 200;"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            count = result.stdout.strip()
            print(f"      📊 OLD user (ID=200): {count} record")
        
        cmd = [
            "docker", "exec", "-i", "apache_airflow-postgres-1",
            "psql", "-U", "airflow", "-d", "airflow", "-t", "-c",
            "SELECT created_at FROM users WHERE id = 200;"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            timestamp = result.stdout.strip()
            print(f"      🕒 OLD user timestamp: {timestamp}")
        
        # Check NEW data (should be upserted)
        print("\n   📝 NEW Data (current) - Will be upserted:")
        cmd = [
            "docker", "exec", "-i", "apache_airflow-postgres-1",
            "psql", "-U", "airflow", "-d", "airflow", "-t", "-c",
            "SELECT COUNT(*) FROM users WHERE id = 100;"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            count = result.stdout.strip()
            print(f"      📊 NEW user (ID=100): {count} record")
        
        cmd = [
            "docker", "exec", "-i", "apache_airflow-postgres-1",
            "psql", "-U", "airflow", "-d", "airflow", "-t", "-c",
            "SELECT COUNT(*) FROM orders WHERE id = 100;"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            count = result.stdout.strip()
            print(f"      📊 NEW order (ID=100): {count} record")
        
        # Check updated data
        print("\n   🔄 UPDATED Data - Will be upserted:")
        cmd = [
            "docker", "exec", "-i", "apache_airflow-postgres-1",
            "psql", "-U", "airflow", "-d", "airflow", "-t", "-c",
            "SELECT name, email FROM users WHERE id = 1;"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            data = result.stdout.strip()
            print(f"      🔄 Updated user (ID=1): {data}")
        
        cmd = [
            "docker", "exec", "-i", "apache_airflow-postgres-1",
            "psql", "-U", "airflow", "-d", "airflow", "-t", "-c",
            "SELECT status, total_amount FROM orders WHERE id = 1;"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            data = result.stdout.strip()
            print(f"      🔄 Updated order (ID=1): {data}")
        
        print("\n✅ Verification completed!")
        print("\n🎯 TIME-BASED UPSERT DEMO:")
        print("   - Records with ID=200 (timestamp: 2024-01-01) will NOT be processed")
        print("   - Records with ID=100 (timestamp: current) WILL be processed")
        print("   - Updated records (ID=1) WILL be processed if in date range")
        return True
        
    except Exception as e:
        print(f"❌ Error verifying changes: {e}")
        return False

def main():
    """Main function"""
    print("🎯 Time-Based Upsert Demo Data Preparation")
    print("=" * 60)
    print("This demo will create data with different timestamps to demonstrate")
    print("that upsert only processes data within specified date ranges.")
    
    # Bước 1: Thêm dữ liệu cũ (sẽ KHÔNG được upsert)
    print("\n1️⃣ Adding OLD data (past timestamps)...")
    success1 = add_old_data()
    
    # Bước 2: Thêm dữ liệu mới (sẽ được upsert)
    print("\n2️⃣ Adding NEW data (current timestamps)...")
    success2 = add_new_data()
    
    # Bước 3: Cập nhật dữ liệu hiện có (sẽ được upsert)
    print("\n3️⃣ Updating existing data...")
    success3 = update_existing_data()
    
    # Bước 4: Verify changes
    print("\n4️⃣ Verifying changes...")
    success4 = verify_changes()
    
    print("\n" + "=" * 60)
    if success1 and success2 and success3 and success4:
        print("✅ Time-based upsert demo data prepared successfully!")
    else:
        print("⚠️  Some operations may have failed. Check logs above.")
    
    print("\n📋 Next steps for TIME-BASED UPSERT DEMO:")
    print("1. Go to Airflow UI: http://localhost:8080")
    print("2. Find DAG: upsert_demo_manual_sequential")
    print("3. Trigger with date range parameters:")
    print("   {")
    print('     "start_date": "2025-07-04 00:00:00",')
    print('     "end_date": "2025-07-04 23:59:59",')
    print('     "batch_size": 5000')
    print("   }")
    print("4. Monitor execution - only current data will be processed!")
    print("5. Verify in ClickHouse:")
    print("   - Records ID=100 (current) should be upserted ✅")
    print("   - Records ID=200 (2024-01-01) should NOT be upserted ❌")
    print("   - Updated records (ID=1) should be upserted ✅")
    print("6. Run comparison script to verify:")
    print("   python compare_psql_clickhouse.py")
    print("7. Expected results:")
    print("   - PostgreSQL will show ID=1 (updated), ID=100 (new), ID=200 (old)")
    print("   - ClickHouse will only show ID=1 & ID=100 (time-filtered!)")
    print("   - ID=200 (old data with 2024-01-01 timestamps) will NOT be upserted")
    print("\n🎯 This proves upsert is TIME-BASED, not full table sync!")
    print("📊 Summary of demo data:")
    print("   • OLD data (ID=200): created_at & updated_at = 2024-01-01 → NOT upserted")
    print("   • NEW data (ID=100): created_at & updated_at = NOW() → WILL be upserted")  
    print("   • UPDATED data (ID=1): updated_at = NOW() → WILL be upserted")

if __name__ == "__main__":
    main() 