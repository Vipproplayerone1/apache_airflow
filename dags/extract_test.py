from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from base.etl_process import ETLProcess
from datetime import datetime
import os

table = "users"

@dag(
    dag_id="extract_users_test",
    schedule=None,
    start_date=datetime(2021, 12, 1),
    catchup=False
)
def extract_users_test():
    etl = ETLProcess(table_name=table)
    extract = PythonOperator(
        task_id=f'extract_{table}',
        python_callable=etl.extract
    )
    create_clickhouse_table = PythonOperator(
        task_id=f'create_clickhouse_{table}_table',
        python_callable=etl.create_clickhouse_table
    )

    load_to_clickhouse = PythonOperator(
        task_id=f'load_to_clickhouse_{table}',
        python_callable=etl.load_to_clickhouse,
        op_args=[etl.extract()]
    )
    [extract , create_clickhouse_table] >> load_to_clickhouse
extract_users_test()
