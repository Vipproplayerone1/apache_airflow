from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os

# Các biến cấu hình
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
bucket_name = Variable.get("s3-bucket")
table = "product"  # Tên bảng cần extract

# Các thông tin kết nối ClickHouse
CLICKHOUSE_HOST = Variable.get("clickhouse_host")
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = Variable.get("clickhouse_user")
CLICKHOUSE_PASSWORD = Variable.get("clickhouse_pass")
CLICKHOUSE_DB = Variable.get("clickhouse_db")

@dag(
    dag_id="extract_product_table",
    schedule=None,
    start_date=datetime(2021, 12, 1),
    catchup=False
)
def extract_product():
    
    @task(task_id="extract_product")
    def extract():
        # Kết nối tới Postgres và S3
        postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
        s3_hook = S3Hook(aws_conn_id="aws_conn_id")
        
        # Đọc file SQL template và format theo bảng cần extract
        sql_template_path = f"{AIRFLOW_HOME}/sql/postgres/extract/extract.sql"
        with open(sql_template_path, "r") as file:
            sql_query = file.read().format(table_name=table, condition='')
        
        # Lấy dữ liệu và chuyển sang DataFrame
        df = postgres_hook.get_pandas_df(sql_query)
        
        # Lưu DataFrame ra file Parquet
        output_path = f"{AIRFLOW_HOME}/{table}.parquet"
        df.to_parquet(output_path, index=False)
        
        # Đẩy file lên S3
        key = f"data/{table}/{table}.parquet"
        s3_hook.load_file(
            filename=output_path,
            key=key,
            bucket_name=bucket_name,
            replace=True
        )
        os.remove(output_path)
        
        # Trả về key của file trên S3 để task sau sử dụng
        return key

    @task(task_id="create_clickhouse_table")
    def create_table():
        from clickhouse_driver import Client
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB
        )
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS product
            (
                id UInt32,
                name String,
                description Nullable(String),
                price Decimal(10, 2),
                stock Int32 DEFAULT 0,
                category_id Nullable(Int32),
                is_deleted UInt8 DEFAULT 0,
                deleted_at Date DEFAULT toDate('1970-01-01'),
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY id;
        """
        client.execute(create_table_sql)
    
    @task(task_id="load_to_clickhouse")
    def load_to_clickhouse(parquet_key: str):
        # Kết nối S3 để tải file xuống tạm thời
        s3_hook = S3Hook(aws_conn_id="aws_conn_id")
        local_file = f"/tmp/{os.path.basename(parquet_key)}"
        
        # Tải file từ S3 về local
        s3_obj = s3_hook.get_key(key=parquet_key, bucket_name=bucket_name)
        s3_obj.download_file(local_file)
        
        # Đọc file Parquet thành DataFrame
        df = pd.read_parquet(local_file)
        os.remove(local_file)
        
        # Kết nối tới ClickHouse
        from clickhouse_driver import Client
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB
        )
        
        # Lấy danh sách cột và chuyển dữ liệu sang dạng tuple
        columns = list(df.columns)
        data = [tuple(row) for row in df.to_numpy()]
        
        # Câu lệnh INSERT (bảng trong ClickHouse cần có cấu trúc tương ứng)
        insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES"
        client.execute(insert_query, data)
    
    # Sắp xếp các task:
    # Task extract thực hiện việc lấy dữ liệu và đẩy lên S3, trả về key file
    parquet_key = extract()
    
    # Task create_table tạo bảng trong ClickHouse nếu chưa tồn tại
    table_created = create_table()
    
    # Đảm bảo rằng bảng đã được tạo trước khi load dữ liệu vào ClickHouse
    table_created >> load_to_clickhouse(parquet_key)

extract_product()
