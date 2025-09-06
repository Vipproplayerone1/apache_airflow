from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import pandas as pd
import os
from datetime import datetime
import time
import gc
import psutil

class UpsertProcess:
    """
    Class riêng biệt để xử lý upsert operations
    Tách biệt khỏi ETLProcess để dễ maintain và test
    """
    
    def __init__(self, table_name: str, batch_size: int = 10000):
        self.AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
        self.bucket_name = Variable.get("s3-bucket")
        self.table = table_name
        self.s3_hook = S3Hook(aws_conn_id="aws_conn_id")
        
        # Optimization settings - tăng batch_size mặc định từ 5000 lên 10000
        self.chunk_size = min(batch_size, 15000)  # Tăng max chunk size
        self.parquet_compression = 'snappy'
        self.max_memory_usage_mb = 1024  # Tăng từ 512MB lên 1024MB
        self.gc_frequency = 2000  # Simple fixed frequency
        
        # Tables với timestamp columns
        self.tables_with_created_at = [
            'category', 'users', 'product', 'orders', 'topup', 'transaction', 'order_product'
        ]
    
    def _monitor_memory_usage(self, operation: str = ""):
        """Monitor memory usage và log warnings"""
        import psutil
        
        memory_percent = psutil.virtual_memory().percent
        process = psutil.Process(os.getpid())
        process_memory_mb = process.memory_info().rss / 1024 / 1024
        
        print(f"Memory usage {operation}: {memory_percent}% (Process: {process_memory_mb:.1f}MB)")
        
        if memory_percent > 80:
            print(f"WARNING: High memory usage ({memory_percent}%) during {operation}")
            
        if process_memory_mb > self.max_memory_usage_mb:
            print(f"INFO: Forcing garbage collection (Process memory: {process_memory_mb:.1f}MB)")
            gc.collect()
            
        return memory_percent, process_memory_mb
    
    def _simple_gc_check(self):
        """Simple garbage collection check - thay thế adaptive throttling"""
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > 80:
            print(f"💾 High memory usage ({memory_percent}%), triggering GC")
            gc.collect()
            time.sleep(1)  # Simple 1s delay
    
    def extract_with_date_range(self, start_date, end_date):
        """
        Trích xuất dữ liệu từ Postgres với khoảng thời gian chỉ định
        """
        import gc
        
        # Validate date inputs
        self._validate_date_range(start_date, end_date)
        
        # Monitor memory trước khi bắt đầu
        self._monitor_memory_usage("before extract")
        
        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn_id")
        s3_hook = S3Hook(aws_conn_id="aws_conn_id")
        
        # Tạo điều kiện thời gian cho truy vấn SQL
        if self.table in self.tables_with_created_at:
            date_condition = f"""AND (
                (created_at >= '{start_date}' AND created_at < '{end_date}')
                OR (updated_at >= '{start_date}' AND updated_at < '{end_date}')
            )"""
            print(f"Table {self.table} has timestamps, filtering by both created_at and updated_at")
        else:
            date_condition = ""
            print(f"Table {self.table} doesn't have created_at column, extracting all data")
        
        sql_template_path = f"{self.AIRFLOW_HOME}/sql/postgres/extract/extract.sql"
        with open(sql_template_path, "r") as file:
            sql_query = file.read().format(table_name=self.table, condition=date_condition)

        print(f"Executing SQL query with date range: {sql_query}")
        
        try:
            # Đọc data với memory optimization
            df = postgres_hook.get_pandas_df(sql_query)
            
            # Monitor memory usage after reading
            memory_percent, process_memory_mb = self._monitor_memory_usage("after reading data")
            
            # Simple GC check thay vì complex throttling
            self._simple_gc_check()
            
            if df.empty:
                print(f"No data found for table {self.table} in date range {start_date} to {end_date}")
                return None

            # Handle null values for deleted_at column
            if self.table in ['category', 'product', 'users', 'orders']:
                df["deleted_at"] = df["deleted_at"].fillna("1970-01-01 00:00:00")
            
            print(f"DataFrame shape: {df.shape}")
            self._monitor_memory_usage("after data processing")

            # Tạo tên file với thông tin khoảng thời gian
            date_suffix = start_date.replace('-', '').replace(' ', '_').replace(':', '')
            output_path = f"{self.AIRFLOW_HOME}/{self.table}_{date_suffix}.parquet"
            print(f"Saving DataFrame to {output_path}")
            
            # Save with optimization
            df.to_parquet(
                output_path, 
                index=False, 
                compression=self.parquet_compression,
                engine='pyarrow'
            )
            
            # Clear DataFrame từ memory
            del df
            gc.collect()
            
            if not os.path.exists(output_path):
                raise FileNotFoundError(f"Failed to create parquet file at {output_path}")

            key = f"data/{self.table}/{self.table}_{date_suffix}.parquet"
            print(f"Uploading file to S3: {key}")
            
            s3_hook.load_file(
                filename=output_path, key=key, bucket_name=self.bucket_name, replace=True
            )
            
            # Clean up local file
            os.remove(output_path)
            
            # Final memory check
            self._monitor_memory_usage("after upload")
            
            print(f"Successfully extracted and uploaded data for {self.table}")
            return key
            
        except Exception as e:
            print(f"Extract failed for {self.table}: {str(e)}")
            # Clean up local file if it exists
            if 'output_path' in locals() and os.path.exists(output_path):
                os.remove(output_path)
            # Force garbage collection on error
            gc.collect()
            raise
    
    def upsert_to_clickhouse(self, **context):
        """
        Thực hiện upsert dữ liệu từ Postgres vào ClickHouse với khoảng thời gian chỉ định
        """
        # Lấy start_date và end_date từ templates_dict
        start_date = context.get('templates_dict', {}).get('start_date')
        end_date = context.get('templates_dict', {}).get('end_date')
        
        # Fallback nếu không có templates_dict
        if not start_date or not end_date:
            start_date = context.get('start_date')
            end_date = context.get('end_date')
        
        # Validate date inputs
        self._validate_date_range(start_date, end_date)
        
        print(f"Starting PROPER upsert for {self.table} between {start_date} and {end_date}")
        
        try:
            # Trích xuất dữ liệu
            s3_key = self.extract_with_date_range(start_date, end_date)
            
            # Nếu không có dữ liệu trong khoảng thời gian này, kết thúc
            if not s3_key:
                print(f"No data to upsert for {self.table} between {start_date} and {end_date}")
                return
            
            # Thực hiện proper upsert bằng cách sử dụng temporary table
            result = self._perform_safe_upsert(s3_key, start_date, end_date)
            
            # Giữ S3 file để tái sử dụng - không cleanup
            print(f"✅ Upsert completed successfully. S3 file preserved: {s3_key}")
            
            return result
                
        except Exception as e:
            print(f"Upsert failed for {self.table}: {str(e)}")
            # Không cleanup S3 file khi có lỗi - giữ lại để debug
            print(f"💾 S3 file preserved for debugging: {s3_key if 's3_key' in locals() else 'N/A'}")
            raise
    
    def _perform_safe_upsert(self, s3_key, start_date, end_date):
        """
        Thực hiện upsert an toàn sử dụng temporary table và transaction-like logic
        """
        # Lấy thông tin AWS
        aws_access_key = Variable.get("aws_access_key")
        aws_secret_key = Variable.get("aws_secret_key")
        
        # Kết nối ClickHouse
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_conn_id')
        conn = clickhouse_hook.get_conn()
        
        # Lấy thông tin cấu trúc bảng ClickHouse
        columns_query = f"DESC TABLE {self.table}"
        columns_info = conn.execute(columns_query)
        
        clickhouse_columns = [row[0] for row in columns_info] 
        clickhouse_columns_type = [row[1] for row in columns_info]
        
        structure = ', '.join(f"{col} {dtype}" for col, dtype in zip(clickhouse_columns, clickhouse_columns_type))
        
        # Tạo tên temporary table
        temp_table = f"{self.table}_temp_{int(time.time())}"
        
        try:
            print(f"Creating temporary table: {temp_table}")
            
            # Tạo temporary table với cấu trúc giống table gốc
            create_temp_table_query = f"""
            CREATE TABLE {temp_table} AS {self.table}
            ENGINE = Memory
            """
            conn.execute(create_temp_table_query)
            
            # Load dữ liệu mới vào temporary table
            print(f"Loading new data into temporary table: {temp_table}")
            load_temp_query = f"""
            INSERT INTO {temp_table} ({', '.join(clickhouse_columns)})
            SELECT {', '.join(clickhouse_columns)} FROM s3(
                's3://{self.bucket_name}/{s3_key}',
                '{aws_access_key}',
                '{aws_secret_key}',
                'Parquet',
                '{structure}'
            )
            """
            conn.execute(load_temp_query)
            
            # Đếm số records trong temp table
            count_query = f"SELECT COUNT(*) FROM {temp_table}"
            temp_count = conn.execute(count_query)[0][0]
            print(f"Loaded {temp_count} records into temporary table")
            
            if temp_count == 0:
                print("No records to upsert")
                return 0
            
            # Thực hiện upsert thực sự
            upsert_count = 0
            
            # Bước 1: UPDATE records đã tồn tại
            if self.table in self.tables_with_created_at:
                print("Performing UPDATE operation for existing records...")
                
                # Xóa records cũ trong khoảng thời gian
                existing_ids_query = f"SELECT id FROM {temp_table}"
                existing_ids = conn.execute(existing_ids_query)
                
                if existing_ids:
                    existing_ids_list = [str(row[0]) for row in existing_ids]
                    print(f"Found {len(existing_ids_list)} records to update/insert")
                    
                    # Xóa records cũ chỉ với IDs cụ thể (an toàn hơn)
                    delete_specific_query = f"""
                    ALTER TABLE {self.table} DELETE
                    WHERE id IN ({', '.join(existing_ids_list)})
                    """
                    print(f"Deleting specific records with IDs: {existing_ids_list[:10]}...")
                    
                    try:
                        conn.execute(delete_specific_query)
                        print(f"Successfully deleted {len(existing_ids_list)} existing records")
                    except Exception as e:
                        print(f"Delete operation failed: {str(e)}")
                
            # Bước 2: INSERT tất cả records từ temp table
            print("Performing INSERT operation for all records...")
            insert_from_temp_query = f"""
            INSERT INTO {self.table} ({', '.join(clickhouse_columns)})
            SELECT {', '.join(clickhouse_columns)} FROM {temp_table}
            """
            
            try:
                conn.execute(insert_from_temp_query)
                upsert_count = temp_count
                print(f"Successfully inserted {upsert_count} records from temporary table")
            except Exception as e:
                print(f"Insert operation failed: {str(e)}")
                raise
            
            print(f"Successfully upserted {upsert_count} records for {self.table}")
            return upsert_count
            
        except Exception as e:
            print(f"Safe upsert failed for {self.table}: {str(e)}")
            raise
        finally:
            # Cleanup temporary table
            try:
                drop_temp_query = f"DROP TABLE IF EXISTS {temp_table}"
                conn.execute(drop_temp_query)
                print(f"Cleaned up temporary table: {temp_table}")
            except Exception as e:
                print(f"Failed to cleanup temporary table {temp_table}: {str(e)}")
    
    def _validate_date_range(self, start_date, end_date):
        """
        Validate date inputs to prevent SQL injection and ensure proper format
        """
        if not start_date or not end_date:
            raise ValueError("start_date and end_date are required")
        
        try:
            # Validate date format
            datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
            datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            raise ValueError(f"Invalid date format. Expected 'YYYY-MM-DD HH:MM:SS', got: {start_date}, {end_date}")
        
        # Ensure start_date < end_date
        if start_date >= end_date:
            raise ValueError(f"start_date ({start_date}) must be before end_date ({end_date})")
    
