from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime
import pandas as pd
import os

# Các biến cấu hình
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
bucket_name = Variable.get("s3-bucket")
table = "category"

@dag(
    dag_id="extract_category_table",
    schedule=None,
    start_date=datetime(2021, 12, 1),
    catchup=False
)
def extract_category():
    
    @task(task_id="extract_category")
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

        # Thay đổi giá trị null ở cột 'deleted_at' thành '1970-01-01 00:00:00'
        df['deleted_at'] = df['deleted_at'].fillna('1970-01-01 00:00:00')

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
    def create_clickhouse_table():
        # Khởi tạo hook với connection id đã định nghĩa
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id="clickhouse_conn_id")
        
        # Đọc file SQL từ đường dẫn đã chỉ định
        sql_path = f"{AIRFLOW_HOME}/sql/clickhouse/ddl/category.sql"
        with open(sql_path, "r", encoding="utf-8") as file:
            ddl_query = file.read()
        
        # Lấy connection và thực thi câu lệnh DDL
        conn = clickhouse_hook.get_conn()
        conn.execute(ddl_query)


    @task(task_id="load_to_clickhouse")
    def load_to_clickhouse(s3_key: str):
        # Lấy giá trị biến từ Airflow Variables
        s3_bucket = Variable.get("s3-bucket")
        aws_access_key = Variable.get("aws_access_key")
        aws_secret_key = Variable.get("aws_secret_key")
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_conn_id')
        # Tạo chuỗi SQL với giá trị thực
        sql_query = f"""
        INSERT INTO category
        SELECT * FROM s3(
            's3://{s3_bucket}/{s3_key}',
            '{aws_access_key}',
            '{aws_secret_key}',
            'Parquet'
        );
        """
        # Thực thi câu lệnh SQL
        conn = clickhouse_hook.get_conn()
        conn.execute(sql_query)
    
    # Gọi task và xây dựng dependency:
    s3_key = extract()  # task1
    table_created = create_clickhouse_table()  # task2

    # Khi cả hai task1 và task2 hoàn thành, mới chạy task3
    load_to_clickhouse(s3_key) << [s3_key, table_created]

extract_category()
