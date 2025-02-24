from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
def fetch_and_save_data():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')  # Thay bằng connection ID của bạn
    sql_query = "SELECT * FROM users;"  # Thay bằng tên bảng của bạn
    df = postgres_hook.get_pandas_df(sql_query)
    df.to_csv('/data/exported_data.csv', index=False)  # Lưu dữ liệu vào file CSV

@dag(schedule=None, start_date=datetime(2021, 12, 1), catchup=False)
def etl_process():
    @task(task_id="extract", retries=2)
    def extract_data():
        postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
        list_conditions = ["id = 10", "age > 20"]
        list_condition_formatted = []
        for condition in list_conditions:
            condition = "AND " + condition
            list_condition_formatted.append(condition)
        """
        list_conditions = ["id = 10", "age > 20"]
        list_condition_formatted = ' '.join(list_condition_formatted)
        AND id = 10 AND age > 20
        """
        list_condition_formatted = ' '.join(list_condition_formatted)
        # Read sql file
        with open(f'{AIRFLOW_HOME}/sql/postgres/extract/extract.sql') as file:
            sql_query = file.read().format(table_name="users",condition=list_condition_formatted)
        df = postgres_hook.get_pandas_df(sql_query)
        df.to_parquet('/data/users.parquet', index=False)
        return len(df)
    extract_data()

etl_process()