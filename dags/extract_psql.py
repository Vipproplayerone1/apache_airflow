from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os

# Lấy biến môi trường AIRFLOW_HOME và kiểm tra
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")  # Default nếu biến không tồn tại
DATA_DIR = os.path.join(AIRFLOW_HOME, "data")  # Thư mục chứa dữ liệu

@dag(schedule=None, start_date=datetime(2021, 12, 1), catchup=False)
def etl_process():
    @task(task_id="extract", retries=2)
    def extract_data():
        postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')

        # Đọc file SQL, kiểm tra xem file có tồn tại không
        sql_file_path = os.path.join(AIRFLOW_HOME, "sql/postgres/extract/extract.sql")
        if not os.path.exists(sql_file_path):
            raise FileNotFoundError(f"Không tìm thấy file SQL: {sql_file_path}")

        with open(sql_file_path, "r") as file:
            sql_query = file.read().format(table_name="users", condition="")

        # Lấy dữ liệu từ PostgreSQL
        df = postgres_hook.get_pandas_df(sql_query)

        # Tạo thư mục lưu dữ liệu nếu chưa tồn tại
        os.makedirs(DATA_DIR, exist_ok=True)
        output_path = os.path.join(DATA_DIR, "users.parquet")

        # Lưu file
        df.to_parquet(output_path, index=False)

        # Kiểm tra file có tồn tại không
        file_exists = os.path.exists(output_path)
        return {"row_count": len(df), "file_exists": file_exists, "file_path": output_path}

    extract_data()

etl_process()
