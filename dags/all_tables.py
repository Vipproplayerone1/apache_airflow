from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

# Lấy đường dẫn AIRFLOW_HOME (có thể thay mặc định nếu cần)
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")

@dag(schedule=None, start_date=datetime(2021, 12, 1), catchup=False)
def etl_process_extract():
    @task(task_id="extract_all_tables", retries=2)
    def extract_all_tables():
        # Khởi tạo kết nối đến PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
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
        # Danh sách các bảng muốn lấy
        tables = ["users", "orders", "order_product", "product", "category", "transaction", "topup"]
        
        all_data = []
        
        for table in tables:
            # Đọc file SQL template
            with open(f'{AIRFLOW_HOME}/sql/postgres/extract/extract.sql') as file:
                sql_query = file.read().format(
                    table_name=table,
                    condition=''
                )
            
            # Truy vấn dữ liệu và trả về DataFrame
            df = postgres_hook.get_pandas_df(sql_query)
            
            # (Tuỳ chọn) Thêm cột "table_name" để biết dữ liệu thuộc bảng nào
            df["table_name"] = table
            
            all_data.append(df)
        
        # Gộp toàn bộ dữ liệu từ các bảng
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            # Lưu dữ liệu ra file Parquet
            output_path = f"{AIRFLOW_HOME}/data/all_tables.parquet"
            combined_df.to_parquet(output_path, index=False)
            return len(combined_df)
        else:
            return 0
    
    extract_all_tables()

etl_process_extract()
