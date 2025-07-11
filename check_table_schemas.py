#!/usr/bin/env python3
"""
Script để kiểm tra schema từng bảng của PostgreSQL và ClickHouse
So sánh số lượng cột và hiển thị thông tin chi tiết
"""

import subprocess
import sys
from typing import Dict, List, Tuple

class SchemaChecker:
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
        
        self.schema_results = {}
    
    def execute_postgres_query(self, query: str) -> str:
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
    
    def execute_clickhouse_query(self, query: str) -> str:
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
    
    def get_postgres_schema(self, table: str) -> List[Dict]:
        """Lấy schema từ PostgreSQL"""
        query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_name = '{table}' 
        ORDER BY ordinal_position;
        """
        
        result = self.execute_postgres_query(query)
        if not result:
            return []
        
        columns = []
        for line in result.split('\n'):
            if line.strip():
                parts = [part.strip() for part in line.split('|')]
                if len(parts) >= 4:
                    columns.append({
                        'name': parts[0],
                        'type': parts[1],
                        'nullable': parts[2],
                        'default': parts[3] if parts[3] else None
                    })
        
        return columns
    
    def get_clickhouse_schema(self, table: str) -> List[Dict]:
        """Lấy schema từ ClickHouse"""
        query = f"DESCRIBE {table};"
        
        result = self.execute_clickhouse_query(query)
        if not result:
            return []
        
        columns = []
        for line in result.split('\n'):
            if line.strip():
                parts = line.split('\t')
                if len(parts) >= 2:
                    columns.append({
                        'name': parts[0].strip(),
                        'type': parts[1].strip(),
                        'nullable': 'Nullable' in parts[1] if len(parts) > 1 else False,
                        'default': parts[2] if len(parts) > 2 and parts[2].strip() else None
                    })
        
        return columns
    
    def compare_schemas(self):
        """So sánh schema giữa PostgreSQL và ClickHouse"""
        print("KIEM TRA SCHEMA CUA TUNG BANG")
        print("=" * 80)
        
        for table in self.tables:
            print(f"\nBang: {table.upper()}")
            print("-" * 60)
            
            # Lấy schema từ PostgreSQL
            postgres_schema = self.get_postgres_schema(table)
            postgres_count = len(postgres_schema)
            
            # Lấy schema từ ClickHouse
            clickhouse_schema = self.get_clickhouse_schema(table)
            clickhouse_count = len(clickhouse_schema)
            
            # So sánh số lượng cột
            status = "OK" if postgres_count == clickhouse_count else "ERROR"
            print(f"{status} So luong cot: PostgreSQL ({postgres_count}) vs ClickHouse ({clickhouse_count})")
            
            # Hiển thị chi tiết cột
            print(f"\n   PostgreSQL - {table} ({postgres_count} cot):")
            for i, col in enumerate(postgres_schema, 1):
                print(f"      {i:2d}. {col['name']:15} | {col['type']:20} | Nullable: {col['nullable']}")
            
            print(f"\n   ClickHouse - {table} ({clickhouse_count} cot):")
            for i, col in enumerate(clickhouse_schema, 1):
                print(f"      {i:2d}. {col['name']:15} | {col['type']:20} | Nullable: {col['nullable']}")
            
            # Tìm cột bị thiếu
            postgres_columns = {col['name'] for col in postgres_schema}
            clickhouse_columns = {col['name'] for col in clickhouse_schema}
            
            missing_in_clickhouse = postgres_columns - clickhouse_columns
            missing_in_postgres = clickhouse_columns - postgres_columns
            
            if missing_in_clickhouse:
                print(f"\n   WARNING Cot thieu trong ClickHouse: {', '.join(missing_in_clickhouse)}")
            
            if missing_in_postgres:
                print(f"\n   WARNING Cot thieu trong PostgreSQL: {', '.join(missing_in_postgres)}")
            
            # Lưu kết quả
            self.schema_results[table] = {
                'postgres_count': postgres_count,
                'clickhouse_count': clickhouse_count,
                'postgres_schema': postgres_schema,
                'clickhouse_schema': clickhouse_schema,
                'match': postgres_count == clickhouse_count,
                'missing_in_clickhouse': list(missing_in_clickhouse),
                'missing_in_postgres': list(missing_in_postgres)
            }
        
        # Tóm tắt kết quả
        self.print_summary()
    
    def print_summary(self):
        """In tóm tắt kết quả"""
        print("\n" + "=" * 80)
        print("TOM TAT KET QUA SCHEMA")
        print("=" * 80)
        
        total_tables = len(self.tables)
        matching_tables = sum(1 for result in self.schema_results.values() if result['match'])
        mismatched_tables = total_tables - matching_tables
        
        print(f"Tong so bang: {total_tables}")
        print(f"Bang co schema khop: {matching_tables}")
        print(f"Bang co schema khong khop: {mismatched_tables}")
        
        if mismatched_tables > 0:
            print(f"\nChi tiet bang co van de:")
            for table, result in self.schema_results.items():
                if not result['match']:
                    postgres_count = result['postgres_count']
                    clickhouse_count = result['clickhouse_count']
                    print(f"   + {table}: PostgreSQL={postgres_count}, ClickHouse={clickhouse_count}")
                    
                    if result['missing_in_clickhouse']:
                        print(f"     - Thieu trong ClickHouse: {', '.join(result['missing_in_clickhouse'])}")
                    
                    if result['missing_in_postgres']:
                        print(f"     - Thieu trong PostgreSQL: {', '.join(result['missing_in_postgres'])}")
        
        print(f"\nKhuyen nghi:")
        if mismatched_tables > 0:
            print("   - Kiem tra cac file DDL trong sql/postgres/ddl/ va sql/clickhouse/ddl/")
            print("   - Dam bao ca 2 database co cung cau truc bang")
            print("   - Chay lai script tao bang neu can thiet")
        else:
            print("   - Tat ca bang da co schema khop nhau")
            print("   - San sang cho viec ETL va upsert")

def main():
    """Main function"""
    try:
        checker = SchemaChecker()
        checker.compare_schemas()
        
    except KeyboardInterrupt:
        print("\nKiem tra bi huy boi nguoi dung")
        sys.exit(1)
    except Exception as e:
        print(f"\nLoi khong mong muon: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 