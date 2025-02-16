from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_postgres():
    hook = PostgresHook(postgres_conn_id="postgresql")  # ID của connection
    conn = hook.get_conn()  # Lấy connection từ Airflow
    cursor = conn.cursor()
    cursor.execute("SELECT version();")  # Truy vấn kiểm tra PostgreSQL
    version = cursor.fetchone()
    print(f"Connected to PostgreSQL: {version}")

default_args = {"owner": "airflow", "start_date": datetime(2024, 2, 14)}

with DAG("test_postgres_connection", default_args=default_args, schedule_interval=None) as dag:

    test_task = PythonOperator(
        task_id="test_postgres",
        python_callable=test_postgres
    )

    test_task
