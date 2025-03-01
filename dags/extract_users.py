from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
bucket_name = Variable.get("s3-bucket")

@dag(
    dag_id="extract_users_table",
    schedule=None,
    start_date=datetime(2021, 12, 1),
    catchup=False
)
def extract_users():
    
    @task(task_id="extract_users")
    def extract():
        postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
        s3_hook = S3Hook(aws_conn_id="aws_conn_id")
        '''
        # Ví dụ: ta có một list điều kiện để chèn vào truy vấn
        list_conditions = ["id = 10", "age > 20"]
        list_condition_formatted = []
        for condition in list_conditions:
            condition = "AND " + condition
            list_condition_formatted.append(condition)
        # Nối các điều kiện lại thành 1 chuỗi
        list_condition_formatted = ' '.join(list_condition_formatted)
        '''
        table = "users"
        # Đọc file SQL template
        with open(f'{AIRFLOW_HOME}/sql/postgres/extract/extract.sql') as file:
            sql_query = file.read().format(table_name=table, condition='')
        # Truy vấn dữ liệu
        df = postgres_hook.get_pandas_df(sql_query)
        # Lưu ra file Parquet riêng cho bảng "users"
        output_path = f"{AIRFLOW_HOME}/{table}.parquet"
        df.to_parquet(output_path, index=False)
        s3_hook.load_file(
                    filename=output_path,
                    key="data/users/users.parquet",
                    bucket_name=bucket_name,
                    replace=True
                )
        os.remove(output_path)
    
    extract()

extract_users()
