from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import pandas as pd
import os
from datetime import datetime


class ETLProcess:
    def __init__(self, table_name: str, batch_size: int = 5000):
        self.AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
        self.bucket_name = Variable.get("s3-bucket")
        self.table = table_name
        # Initialize S3Hook
        self.s3_hook = S3Hook(aws_conn_id="aws_conn_id")
        
        # Optimization settings - configurable batch size
        self.chunk_size = min(batch_size, 10000)  # Không vượt quá 10k rows
        self.parquet_compression = 'snappy'  # Fast compression
        
        # Memory optimization settings
        self.max_memory_usage_mb = 512  # Giới hạn memory usage
        self.gc_frequency = 1000  # Garbage collection frequency
        
        # Tables with created_at and updated_at columns for date filtering
        self.tables_with_created_at = [
            'category', 'users', 'product', 'orders', 'topup', 'transaction', 'order_product'
        ]
    
    def _monitor_memory_usage(self, operation: str = ""):
        """Monitor memory usage và log warnings"""
        import psutil
        import gc
        
        # Get current memory usage
        memory_percent = psutil.virtual_memory().percent
        process = psutil.Process(os.getpid())
        process_memory_mb = process.memory_info().rss / 1024 / 1024
        
        print(f"Memory usage {operation}: {memory_percent}% (Process: {process_memory_mb:.1f}MB)")
        
        # Warning nếu memory usage cao
        if memory_percent > 80:
            print(f"WARNING: High memory usage ({memory_percent}%) during {operation}")
            
        # Force garbage collection nếu process memory cao
        if process_memory_mb > self.max_memory_usage_mb:
            print(f"INFO: Forcing garbage collection (Process memory: {process_memory_mb:.1f}MB)")
            gc.collect()
            
        return memory_percent, process_memory_mb

    
    def extract(self):
        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn_id")
        s3_hook = S3Hook(aws_conn_id="aws_conn_id")
        
        sql_template_path = f"{self.AIRFLOW_HOME}/sql/postgres/extract/extract.sql"
        with open(sql_template_path, "r") as file:
            sql_query = file.read().format(table_name=self.table, condition="")

        print(f"Executing SQL query: {sql_query}")
        df = postgres_hook.get_pandas_df(sql_query)
        print(f"DataFrame shape: {df.shape}")
        
        if df.empty:
            raise ValueError(f"No data found for table {self.table}")

        if self.table == 'category' or self.table == 'product' or self.table == 'users' or self.table == 'orders':
            df["deleted_at"] = df["deleted_at"].fillna("1970-01-01 00:00:00")

        output_path = f"{self.AIRFLOW_HOME}/{self.table}.parquet"
        print(f"Saving DataFrame to {output_path}")
        df.to_parquet(output_path, index=False, compression=self.parquet_compression)
        
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Failed to create parquet file at {output_path}")

        key = f"data/{self.table}/{self.table}.parquet"
        print(f"Uploading file to S3: {key}")
        s3_hook.load_file(
            filename=output_path, key=key, bucket_name=self.bucket_name, replace=True
        )
        os.remove(output_path)

        return key

    def create_clickhouse_table(self):
      
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id="clickhouse_conn_id")
        
      
        sql_path = f"{self.AIRFLOW_HOME}/sql/clickhouse/ddl/{self.table}.sql"
        with open(sql_path, "r", encoding="utf-8") as file:
            ddl_query = file.read()
        
    
        conn = clickhouse_hook.get_conn()
        conn.execute(ddl_query)

    def load_to_clickhouse(self, s3_key: str):
       
        aws_access_key = Variable.get("aws_access_key")
        aws_secret_key = Variable.get("aws_secret_key")
      

        clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_conn_id')
        conn = clickhouse_hook.get_conn()

      
        columns_query = f"DESC TABLE {self.table}"
        columns_info = conn.execute(columns_query)
        
        clickhouse_columns = [row[0] for row in columns_info] 
        clickhouse_columns_type = [row[1] for row in columns_info]  

        structure = ', '.join(f"{col} {dtype}" for col, dtype in zip(clickhouse_columns, clickhouse_columns_type))

      
        sql_query = f"""
        INSERT INTO {self.table} ({', '.join(clickhouse_columns)})
        SELECT {', '.join(clickhouse_columns)} FROM s3(
            's3://{self.bucket_name}/{s3_key}',
            '{aws_access_key}',
            '{aws_secret_key}',
            'Parquet',
            '{structure}'
        );
        """
        print(sql_query)
        conn.execute(sql_query)
    