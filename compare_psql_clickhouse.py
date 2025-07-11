#!/usr/bin/env python3
"""
Script để so sánh số lượng records giữa PostgreSQL và ClickHouse
In dữ liệu của TẤT CẢ bảng (bất kể đã đồng bộ hay chưa)
"""

import subprocess
import sys
from datetime import datetime
import json

class DatabaseComparator:
    def __init__(self):
        self.tables = [
            'category', 'users', 'product', 'orders', 
            'topup', 'order_product', 'transaction'
        ]
        self.postgres_container = "apache_airflow-postgres-1"
        self.clickhouse_container = "clickhouse-server"
        
        # Database credentials
        self.postgres_creds = {
            'host': 'postgres',
            'user': 'airflow',
            'database': 'airflow'
        }
        
        self.clickhouse_creds = {
            'user': 'myuser',
            'password': 'mypassword',
            'database': 'mydatabase'
        }
        
        self.results = {}
        self.unsynced_tables = []
    
    def execute_postgres_query(self, query):
        """Thực thi query trong PostgreSQL"""
        try:
            cmd = [
                "docker", "exec", "-i", self.postgres_container,
                "psql", "-U", self.postgres_creds['user'], 
                "-d", self.postgres_creds['database'], 
                "-t", "-c", query
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"❌ PostgreSQL error: {result.stderr}")
                return None
            
            return result.stdout.strip()
            
        except Exception as e:
            print(f"❌ Error executing PostgreSQL query: {e}")
            return None
    
    def execute_clickhouse_query(self, query):
        """Thực thi query trong ClickHouse"""
        try:
            cmd = [
                "docker", "exec", "-i", self.clickhouse_container,
                "clickhouse-client", 
                "-u", self.clickhouse_creds['user'],
                "--password", self.clickhouse_creds['password'],
                "-d", self.clickhouse_creds['database'],
                "-q", query
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"❌ ClickHouse error: {result.stderr}")
                return None
            
            return result.stdout.strip()
            
        except Exception as e:
            print(f"❌ Error executing ClickHouse query: {e}")
            return None
    
    def compare_record_counts(self):
        """So sánh số lượng records giữa PostgreSQL và ClickHouse"""
        print("📊 So sánh số lượng records...")
        print("-" * 60)
        
        for table in self.tables:
            postgres_count = self.execute_postgres_query(f"SELECT COUNT(*) FROM {table};")
            clickhouse_count = self.execute_clickhouse_query(f"SELECT COUNT(*) FROM {table};")
            
            if postgres_count is not None and clickhouse_count is not None:
                postgres_count = int(postgres_count.strip())
                clickhouse_count = int(clickhouse_count.strip())
                
                status = "✅" if postgres_count == clickhouse_count else "❌"
                print(f"{status} {table:15} | PostgreSQL: {postgres_count:4} | ClickHouse: {clickhouse_count:4}")
                
                self.results[table] = {
                    'postgres_count': postgres_count,
                    'clickhouse_count': clickhouse_count,
                    'match': postgres_count == clickhouse_count
                }
                
                # Lưu lại bảng chưa đồng bộ
                if postgres_count != clickhouse_count:
                    self.unsynced_tables.append(table)
                    
            else:
                print(f"❌ {table:15} | Không thể truy vấn dữ liệu")
                self.results[table] = {
                    'postgres_count': 0,
                    'clickhouse_count': 0,
                    'match': False,
                    'error': True
                }
    
    def get_postgres_columns(self, table):
        """Lấy tên cột từ PostgreSQL"""
        query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table}' 
        ORDER BY ordinal_position;
        """
        result = self.execute_postgres_query(query)
        if result:
            return [col.strip() for col in result.split('\n') if col.strip()]
        return []

    def get_clickhouse_columns(self, table):
        """Lấy tên cột từ ClickHouse"""
        query = f"DESCRIBE {table};"
        result = self.execute_clickhouse_query(query)
        if result:
            # ClickHouse DESCRIBE trả về: name, type, default_type, default_expression, comment, codec_expression, ttl_expression
            columns = []
            for line in result.split('\n'):
                if line.strip():
                    # Lấy tên cột đầu tiên (phần trước tab đầu tiên)
                    col_name = line.split('\t')[0].strip()
                    if col_name:
                        columns.append(col_name)
            return columns
        return []

    def print_table_data(self, table):
        """In toàn bộ dữ liệu của một bảng từ cả 2 database với tên cột"""
        print(f"\n🔍 Dữ liệu bảng {table.upper()}:")
        print("=" * 80)
        
        # Lấy dữ liệu từ PostgreSQL
        print(f"\n📋 PostgreSQL - {table}:")
        print("-" * 40)
        
        # Lấy tên cột PostgreSQL
        postgres_columns = self.get_postgres_columns(table)
        if postgres_columns:
            print(f"  📋 Cột: {' | '.join(postgres_columns)}")
            print(f"  {'-' * (len(' | '.join(postgres_columns)) + 4)}")
        
        postgres_data = self.execute_postgres_query(f"SELECT * FROM {table} ORDER BY id;")
        if postgres_data:
            for line in postgres_data.split('\n'):
                if line.strip():
                    print(f"  {line.strip()}")
        else:
            print("  Không có dữ liệu hoặc lỗi truy vấn")
        
        # Lấy dữ liệu từ ClickHouse
        print(f"\n📋 ClickHouse - {table}:")
        print("-" * 40)
        
        # Lấy tên cột ClickHouse
        clickhouse_columns = self.get_clickhouse_columns(table)
        if clickhouse_columns:
            print(f"  📋 Cột: {' | '.join(clickhouse_columns)}")
            print(f"  {'-' * (len(' | '.join(clickhouse_columns)) + 4)}")
        
        clickhouse_data = self.execute_clickhouse_query(f"SELECT * FROM {table} ORDER BY id FORMAT TabSeparated;")
        if clickhouse_data:
            for line in clickhouse_data.split('\n'):
                if line.strip():
                    print(f"  {line.strip()}")
        else:
            print("  Không có dữ liệu hoặc lỗi truy vấn")
    
    def print_all_tables_data(self):
        """In dữ liệu của tất cả bảng từ cả 2 database"""
        print(f"\n📋 DỮ LIỆU TẤT CẢ BẢNG:")
        print("=" * 80)
        
        for table in self.tables:
            self.print_table_data(table)
    
    def generate_summary(self):
        """Tạo tóm tắt kết quả"""
        print("\n" + "="*60)
        print("📈 TÓM TẮT KẾT QUẢ")
        print("="*60)
        
        total_tables = len(self.tables)
        synced_tables = len([t for t in self.results.values() if t.get('match', False)])
        unsynced_tables = total_tables - synced_tables
        
        print(f"📊 Tổng số bảng: {total_tables}")
        print(f"✅ Đã đồng bộ: {synced_tables}")
        print(f"❌ Chưa đồng bộ: {unsynced_tables}")
        
        if unsynced_tables > 0:
            print(f"\n⚠️  Bảng chưa đồng bộ:")
            for table, result in self.results.items():
                if not result.get('match', False):
                    postgres_count = result.get('postgres_count', 0)
                    clickhouse_count = result.get('clickhouse_count', 0)
                    print(f"   • {table}: PostgreSQL={postgres_count}, ClickHouse={clickhouse_count}")
        
        print(f"\n⏰ Thời gian hoàn thành: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def run_comparison(self):
        """Chạy toàn bộ quá trình so sánh"""
        print("🔍 BẮT ĐẦU SO SÁNH DỮ LIỆU POSTGRESQL VÀ CLICKHOUSE")
        print("="*60)
        print(f"⏰ Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Bước 1: So sánh số lượng records
        self.compare_record_counts()
        
        # Bước 2: In dữ liệu tất cả bảng
        self.print_all_tables_data()
        
        # Bước 3: Tạo tóm tắt
        self.generate_summary()

def main():
    """Main function"""
    try:
        comparator = DatabaseComparator()
        comparator.run_comparison()
        
    except KeyboardInterrupt:
        print("\n⚠️  So sánh bị hủy bởi người dùng")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Lỗi không mong muốn: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
