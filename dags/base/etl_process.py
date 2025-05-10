from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import os


class ETLProcess:
    def __init__(self, table_name: str):
        self.AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
        self.bucket_name = Variable.get("s3-bucket")
        self.table = table_name

    def extract(self):
        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn_id")
        s3_hook = S3Hook(aws_conn_id="aws_conn_id")
        #if self.table == 'transaction':
            #sql_template_path = f"{self.AIRFLOW_HOME}/sql/postgres/extract/extract_transaction.sql"
        #else:
        sql_template_path = f"{self.AIRFLOW_HOME}/sql/postgres/extract/extract.sql"
        with open(sql_template_path, "r") as file:
            sql_query = file.read().format(table_name=self.table, condition="")

        df = postgres_hook.get_pandas_df(sql_query)
        if self.table == 'category' or self.table == 'product' or self.table == 'users' or self.table == 'orders':
            df["deleted_at"] = df["deleted_at"].fillna("1970-01-01 00:00:00")

        output_path = f"{self.AIRFLOW_HOME}/{self.table}.parquet"
        df.to_parquet(output_path, index=False)

        key = f"data/{self.table}/{self.table}.parquet"
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