#!/usr/bin/env python3
"""
Script để reset PostgreSQL database về trạng thái ban đầu
Xóa toàn bộ dữ liệu và tạo lại schema + dữ liệu mặc định
"""

import subprocess
import sys
import os

def run_sql_file_in_postgres(file_path, description=""):
    """Thực thi file SQL trong PostgreSQL"""
    print(f"📁 Executing {description}: {file_path}")
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        cmd = [
            "docker", "exec", "-i", "apache_airflow-postgres-1",
            "psql", "-U", "airflow", "-d", "airflow", "-c", sql_content
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Error executing {file_path}: {result.stderr}")
            return False
        
        print(f"✅ Successfully executed {description}")
        return True
        
    except Exception as e:
        print(f"❌ Exception executing {file_path}: {e}")
        return False

def run_sql_command(sql_command, description=""):
    """Thực thi SQL command trong PostgreSQL"""
    print(f"🔧 Executing {description}")
    
    try:
        cmd = [
            "docker", "exec", "-i", "apache_airflow-postgres-1",
            "psql", "-U", "airflow", "-d", "airflow", "-c", sql_command
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Error: {result.stderr}")
            return False
        
        print(f"✅ {description} completed")
        return True
        
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def reset_postgres_database():
    """Reset toàn bộ PostgreSQL database về trạng thái ban đầu"""
    print("🔄 RESET POSTGRESQL DATABASE")
    print("=" * 60)
    
    # Bước 1: Drop tất cả bảng (theo thứ tự để tránh foreign key conflicts)
    print("\n1️⃣ Dropping existing tables...")
    
    drop_commands = [
        "DROP TABLE IF EXISTS transaction CASCADE;",
        "DROP TABLE IF EXISTS order_product CASCADE;", 
        "DROP TABLE IF EXISTS orders CASCADE;",
        "DROP TABLE IF EXISTS topup CASCADE;",
        "DROP TABLE IF EXISTS product CASCADE;",
        "DROP TABLE IF EXISTS users CASCADE;",
        "DROP TABLE IF EXISTS category CASCADE;"
    ]
    
    for cmd in drop_commands:
        run_sql_command(cmd, f"Drop table")
    
    # Bước 2: Tạo lại tất cả bảng theo schema hiện tại
    print("\n2️⃣ Creating tables...")
    
    ddl_files = [
        ("sql/postgres/ddl/category.sql", "Category table"),
        ("sql/postgres/ddl/users.sql", "Users table"),
        ("sql/postgres/ddl/product.sql", "Product table"),
        ("sql/postgres/ddl/orders.sql", "Orders table"),
        ("sql/postgres/ddl/topup.sql", "Topup table"),
        ("sql/postgres/ddl/order_product.sql", "Order_product table"),
        ("sql/postgres/ddl/transaction.sql", "Transaction table")
    ]
    
    success_count = 0
    for file_path, description in ddl_files:
        if run_sql_file_in_postgres(file_path, description):
            success_count += 1
    
    if success_count != len(ddl_files):
        print(f"❌ Only {success_count}/{len(ddl_files)} tables created successfully")
        return False
    
    # Bước 3: Insert dữ liệu mặc định
    print("\n3️⃣ Inserting default data...")
    
    insert_files = [
        ("sql/postgres/insert/category.sql", "Category data"),
        ("sql/postgres/insert/users.sql", "Users data"),
        ("sql/postgres/insert/product.sql", "Product data"),
        ("sql/postgres/insert/orders.sql", "Orders data"),
        ("sql/postgres/insert/topup.sql", "Topup data"),
        ("sql/postgres/insert/order_product.sql", "Order_product data"),
        ("sql/postgres/insert/transaction.sql", "Transaction data")
    ]
    
    success_count = 0
    for file_path, description in insert_files:
        if run_sql_file_in_postgres(file_path, description):
            success_count += 1
    
    if success_count != len(insert_files):
        print(f"❌ Only {success_count}/{len(insert_files)} data inserts completed successfully")
        return False
    
    return True

def verify_reset():
    """Verify database reset thành công"""
    print("\n4️⃣ Verifying database reset...")
    
    verification_queries = [
        ("SELECT COUNT(*) FROM category;", "Category count"),
        ("SELECT COUNT(*) FROM users;", "Users count"),
        ("SELECT COUNT(*) FROM product;", "Product count"),
        ("SELECT COUNT(*) FROM orders;", "Orders count"),
        ("SELECT COUNT(*) FROM topup;", "Topup count"),
        ("SELECT COUNT(*) FROM order_product;", "Order_product count"),
        ("SELECT COUNT(*) FROM transaction;", "Transaction count")
    ]
    
    print("📊 Record counts:")
    for query, description in verification_queries:
        try:
            cmd = [
                "docker", "exec", "-i", "apache_airflow-postgres-1",
                "psql", "-U", "airflow", "-d", "airflow", "-t", "-c", query
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                count = result.stdout.strip()
                print(f"   ✅ {description}: {count}")
            else:
                print(f"   ❌ {description}: Error")
                
        except Exception as e:
            print(f"   ❌ {description}: Exception {e}")
    
    # Verify table structure
    print("\n🔍 Verifying table structure:")
    
    tables_to_check = ['category', 'users', 'product', 'orders', 'topup', 'order_product', 'transaction']
    
    for table in tables_to_check:
        try:
            query = f"""
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_name = '{table}';
            """
            
            cmd = [
                "docker", "exec", "-i", "apache_airflow-postgres-1",
                "psql", "-U", "airflow", "-d", "airflow", "-t", "-c", query
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                column_count = result.stdout.strip()
                print(f"   ✅ {table}: {column_count} columns")
            else:
                print(f"   ❌ {table}: Error getting structure")
                
        except Exception as e:
            print(f"   ❌ {table}: Error checking structure - {e}")

def main():
    """Main function"""
    try:
        # Reset database
        if reset_postgres_database():
            print("\n✅ Database reset completed successfully!")
            
            # Verify reset
            verify_reset()
            
            print("\n" + "=" * 60)
            print("🎉 POSTGRESQL DATABASE RESET COMPLETED")
            print("=" * 60)
            print("✅ All tables recreated successfully")
            print("✅ Default data inserted successfully")
            print("✅ Database is ready for ETL operations")
            
            print("\n📋 Next steps:")
            print("1. Clear ClickHouse data: python clear_clickhouse_data.py")
            print("2. Run ETL DAG to sync initial data")
            print("3. Add demo data: python add_upsert_demo_data.py")
            print("4. Test upsert operations")
            print("5. Compare databases: python compare_psql_clickhouse.py")
            
        else:
            print("\n❌ Database reset failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 