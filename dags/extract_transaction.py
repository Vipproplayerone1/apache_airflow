from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime as dt  # Đổi tên lớp datetime thành dt
import pandas as pd
import os
from decimal import Decimal

# Các biến cấu hình
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
bucket_name = Variable.get("s3-bucket")
table = "transaction"  # Tên bảng cần extract

# Các thông tin kết nối ClickHouse
CLICKHOUSE_HOST = Variable.get("clickhouse_host")
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = Variable.get("clickhouse_user")
CLICKHOUSE_PASSWORD = Variable.get("clickhouse_pass")
CLICKHOUSE_DB = Variable.get("clickhouse_db")

@dag(
    dag_id="extract_transaction_table",
    schedule=None,
    start_date=dt(2021, 12, 1),  # Sử dụng dt thay vì datetime
    catchup=False
)
def extract_transaction():
    
    @task(task_id="extract_transaction")
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
        CREATE TABLE IF NOT EXISTS transaction
        (
          id UInt32,
          user_id UInt32,
          order_id Nullable(UInt32),
          topup_id Nullable(UInt32),
          type Enum('purchase' = 1, 'topup' = 2),
          amount Decimal(10, 2),
          created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY id;
        """
        client.execute(create_table_sql)

    @task(task_id="load_to_clickhouse")
    def load_to_clickhouse(parquet_key: str):
        from clickhouse_driver import Client

        # Tải file từ S3 về local
        s3_hook = S3Hook(aws_conn_id="aws_conn_id")
        local_file = f"/tmp/{os.path.basename(parquet_key)}"
        s3_obj = s3_hook.get_key(key=parquet_key, bucket_name=bucket_name)
        s3_obj.download_file(local_file)
        
        # Đọc file Parquet thành DataFrame
        df = pd.read_parquet(local_file)
        os.remove(local_file)
        
        # Thay thế NaN bằng None cho toàn bộ DataFrame
        df = df.where(pd.notnull(df), None)
        
        # Hàm chuyển đổi từng dòng theo định dạng của bảng ClickHouse
        def convert_row(row: dict):
            try:
                _id = int(row.get('id')) if row.get('id') is not None else None
            except Exception:
                _id = None
            try:
                user_id = int(row.get('user_id')) if row.get('user_id') is not None else None
            except Exception:
                user_id = None
            try:
                order_id = int(row.get('order_id')) if row.get('order_id') is not None else None
            except Exception:
                order_id = None
            try:
                topup_id = int(row.get('topup_id')) if row.get('topup_id') is not None else None
            except Exception:
                topup_id = None
            typ = row.get('type')
            try:
                amount = Decimal(f"{row.get('amount'):.2f}") if row.get('amount') is not None else None
            except Exception:
                amount = None
            created_at = row.get('created_at')
            if created_at is not None:
                if isinstance(created_at, pd.Timestamp):
                    created_at = created_at.to_pydatetime()
                elif isinstance(created_at, (int, float)):
                    created_at = dt.fromtimestamp(created_at / 1000)
            else:
                created_at = None
            return (_id, user_id, order_id, topup_id, typ, amount, created_at)
        
        # Chuyển DataFrame thành danh sách dict rồi chuyển từng dòng
        records = df.to_dict(orient='records')
        data = [convert_row(rec) for rec in records]
        
        # Kết nối tới ClickHouse và thực hiện insert
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB
        )
        columns = list(df.columns)
        insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES"
        client.execute(insert_query, data)
    
    # Sắp xếp các task
    parquet_key = extract()
    table_created = create_table()
    table_created >> load_to_clickhouse(parquet_key)

extract_transaction()
