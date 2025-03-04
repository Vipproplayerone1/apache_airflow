from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime
import pandas as pd
import os

# Các biến cấu hình
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
bucket_name = Variable.get("s3-bucket")
table = "users"

@dag(
    dag_id="extract_users_table",
    schedule=None,
    start_date=datetime(2021, 12, 1),
    catchup=False
)
def extract_users():
    
    @task(task_id="extract_users")
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

    # Tạo bảng trong ClickHouse thông qua airflow-clickhouse-plugin
    create_clickhouse_table = ClickHouseOperator(
         task_id="create_clickhouse_table",
         sql="""
         CREATE TABLE IF NOT EXISTS users
         (
             id UInt32,
             name String,
             email String,
             phone String,
             is_activated UInt8 DEFAULT 1,
             is_deleted UInt8 DEFAULT 0,
             deleted_at DateTime DEFAULT '1970-01-01 00:00:00',
             created_at DateTime DEFAULT now(),
             updated_at DateTime DEFAULT now()
         )
         ENGINE = MergeTree()
         ORDER BY id;
         """,
         clickhouse_conn_id='clickhouse_conn_id'
    )

    # Load dữ liệu từ file Parquet trên S3 vào bảng ClickHouse sử dụng engine S3 của ClickHouse
    load_to_clickhouse = ClickHouseOperator(
         task_id="load_to_clickhouse",
         sql="""
        INSERT INTO users
        SELECT * FROM s3(
        's3://{{ var.value["s3-bucket"] }}/{{ ti.xcom_pull(task_ids="extract_users") }}',
        '{{ var.value["aws_access_key"] }}',
        '{{ var.value["aws_secret_key"] }}',
        Parquet
);

         """,
         clickhouse_conn_id='clickhouse_conn_id'
    )
    
    # Sắp xếp thứ tự thực hiện: tạo file trên s3 đồng thời tạo bảng ClickHouse -> load dữ liệu từ file Parquet
    parquet_key = extract()
    create_clickhouse_table >> load_to_clickhouse

extract_users()
