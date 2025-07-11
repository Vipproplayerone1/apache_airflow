from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from base import ETLProcess
from datetime import datetime, timedelta
import time

@dag(
    dag_id="extract_all_tables",
    schedule=None,  # Không có schedule, chỉ chạy khi trigger
    start_date=datetime(2025, 1, 19),
    catchup=False,  # Không chạy catchup
    description="ETL Sequential - Chạy từng task để tránh overload worker",
    max_active_runs=1,      # Chỉ 1 DAG run tại một thời điểm
    concurrency=1,          # Chỉ 1 task chạy đồng thời
    max_active_tasks=1,     # Tối đa 1 task active cùng lúc
    tags=['etl', 'sequential', 'optimized'],
    is_paused_upon_creation=False,  # DAG không bị pause khi tạo
    doc_md="""
    ## ETL Pipeline - Sequential Execution
    
    DAG này thực hiện ETL từ PostgreSQL sang ClickHouse một cách tuần tự.
    Chạy từng task một để tránh overload worker.
    
    ### Execution Flow (Sequential):
    1. category
    2. users  
    3. product
    4. orders
    5. topup
    6. order_product
    7. transaction
    """
)
def extract_all_tables():
    """
    DAG tổng hợp ETL với sequential execution
    Tasks sẽ chạy từng cái một để tránh overload worker
    """
    
    def extract_and_load_sequential(table_name):
        """Function tối ưu: extract và load với sequential processing"""
        try:
            print(f"🚀 Starting ETL for {table_name}...")
            start_time = time.time()
            
            # Small delay để giảm load trên worker
            time.sleep(2)
            
            etl = ETLProcess(table_name=table_name, batch_size=5000)
            
            # Extract data
            print(f"📤 Extracting {table_name} data...")
            s3_key = etl.extract()
            
            # Create table nếu chưa có
            print(f"🏗️ Creating ClickHouse table for {table_name}...")
            etl.create_clickhouse_table()
            
            # Load to ClickHouse
            print(f"📥 Loading {table_name} data to ClickHouse...")
            etl.load_to_clickhouse(s3_key)
            
            # Thời gian hoàn thành
            end_time = time.time()
            duration = end_time - start_time
            
            print(f"✅ Completed ETL for {table_name} in {duration:.2f} seconds")
            
            # Small delay sau khi hoàn thành task
            time.sleep(3)
            
            return f"✅ Completed ETL for {table_name} in {duration:.2f}s"
            
        except Exception as e:
            print(f"❌ Error in ETL for {table_name}: {str(e)}")
            raise
    
    # Tasks được thiết kế để chạy tuần tự
    category_task = PythonOperator(
        task_id='etl_category',
        python_callable=extract_and_load_sequential,
        op_args=['category'],
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=15),
        pool='default_pool',
        pool_slots=1
    )
    
    users_task = PythonOperator(
        task_id='etl_users',
        python_callable=extract_and_load_sequential,
        op_args=['users'],
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=15),
        pool='default_pool',
        pool_slots=1
    )
    
    product_task = PythonOperator(
        task_id='etl_product',
        python_callable=extract_and_load_sequential,
        op_args=['product'],
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=15),
        pool='default_pool',
        pool_slots=1
    )
    
    orders_task = PythonOperator(
        task_id='etl_orders',
        python_callable=extract_and_load_sequential,
        op_args=['orders'],
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=15),
        pool='default_pool',
        pool_slots=1
    )
    
    topup_task = PythonOperator(
        task_id='etl_topup',
        python_callable=extract_and_load_sequential,
        op_args=['topup'],
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=15),
        pool='default_pool',
        pool_slots=1
    )
    
    order_product_task = PythonOperator(
        task_id='etl_order_product',
        python_callable=extract_and_load_sequential,
        op_args=['order_product'],
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=15),
        pool='default_pool',
        pool_slots=1
    )
    
    transaction_task = PythonOperator(
        task_id='etl_transaction',
        python_callable=extract_and_load_sequential,
        op_args=['transaction'],
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=15),
        pool='default_pool',
        pool_slots=1
    )
    
    # Dependencies - Sequential execution (từng task một)
    # Thứ tự chạy: category → users → product → orders → topup → order_product → transaction
    category_task >> users_task >> product_task >> orders_task >> topup_task >> order_product_task >> transaction_task

# Tạo DAG
extract_all_tables() 