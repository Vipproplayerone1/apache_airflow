from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime
import pandas as pd
import os


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
       
        postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
        s3_hook = S3Hook(aws_conn_id="aws_conn_id")

    
        sql_template_path = f"{AIRFLOW_HOME}/sql/postgres/extract/extract.sql"
        with open(sql_template_path, "r") as file:
            sql_query = file.read().format(table_name=table, condition='')

      
        df = postgres_hook.get_pandas_df(sql_query)

       
        df['deleted_at'] = df['deleted_at'].fillna('1970-01-01 00:00:00')

      
        output_path = f"{AIRFLOW_HOME}/{table}.parquet"
        df.to_parquet(output_path, index=False)

       
        key = f"data/{table}/{table}.parquet"
        s3_hook.load_file(
            filename=output_path,
            key=key,
            bucket_name=bucket_name,
            replace=True
        )
        os.remove(output_path)

  
        return key


    @task(task_id="create_clickhouse_table")
    def create_clickhouse_table():
  
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id="clickhouse_conn_id")
        
        sql_path = f"{AIRFLOW_HOME}/sql/clickhouse/ddl/users.sql"
        with open(sql_path, "r", encoding="utf-8") as file:
            ddl_query = file.read()
        
        conn = clickhouse_hook.get_conn()
        conn.execute(ddl_query)


    @task(task_id="load_to_clickhouse")
    def load_to_clickhouse(s3_key: str):
        
        aws_access_key = Variable.get("aws_access_key")
        aws_secret_key = Variable.get("aws_secret_key")
      

        clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_conn_id')
        conn = clickhouse_hook.get_conn()

        columns_query = "DESC TABLE users"
        columns_info = conn.execute(columns_query)
        
        clickhouse_columns = [row[0] for row in columns_info] 
        clickhouse_columns_type = [row[1] for row in columns_info]  

        structure = ', '.join(f"{col} {dtype}" for col, dtype in zip(clickhouse_columns, clickhouse_columns_type))

      
        sql_query = f"""
        INSERT INTO users ({', '.join(clickhouse_columns)})
        SELECT {', '.join(clickhouse_columns)} FROM s3(
            's3://{bucket_name}/{s3_key}',
            '{aws_access_key}',
            '{aws_secret_key}',
            'Parquet',
            '{structure}'
        );
        """

        conn.execute(sql_query)
    
    s3_key = extract()  
    table_created = create_clickhouse_table() 

    load_to_clickhouse(s3_key) << [s3_key, table_created]

extract_users()
