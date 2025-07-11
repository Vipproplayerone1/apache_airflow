from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from base import UpsertProcess
from datetime import datetime, timedelta
from airflow.models import DagRun
from airflow.utils.state import DagRunState

# Danh sách các bảng cần upsert
TABLES = [
    "category",   # Không phụ thuộc bảng nào khác
    "users",      # Không phụ thuộc bảng nào khác
    "product",    # Phụ thuộc vào category
    "orders",     # Phụ thuộc vào users
    "topup",      # Phụ thuộc vào users
    "order_product", # Phụ thuộc vào orders và product
    "transaction"    # Phụ thuộc vào users, orders và topup
]

# Hàm trợ giúp để tìm thời gian chạy thành công gần nhất
def get_last_successful_run():
    """
    Tìm thời gian chạy thành công gần nhất của DAG hiện tại. 
    Nếu không có, tìm thời gian chạy gần nhất của DAG extract_all_tables.
    Nếu cũng không có, raise lỗi.
    """
    try:
        # Tìm lần chạy thành công gần nhất của DAG hiện tại
        current_dag_id = 'upsert_all_tables_daily'
        current_dag_runs = DagRun.find(
            dag_id=current_dag_id,
            state=DagRunState.SUCCESS
        )
        
        if current_dag_runs and len(current_dag_runs) > 0:
            # Sắp xếp theo thời gian kết thúc để lấy lần chạy thành công gần nhất
            current_dag_runs.sort(key=lambda x: x.end_date if x.end_date else datetime.min, reverse=True)
            print(f"Tìm thấy lần chạy thành công gần nhất của DAG {current_dag_id}: {current_dag_runs[0].execution_date}")
            return current_dag_runs[0].execution_date
        
        # Nếu không tìm thấy lần chạy của DAG hiện tại, tìm lần chạy của DAG extract_all_tables
        extract_dag_id = 'extract_all_tables'
        extract_dag_runs = DagRun.find(
            dag_id=extract_dag_id,
            state=DagRunState.SUCCESS
        )
        
        if extract_dag_runs and len(extract_dag_runs) > 0:
            # Sắp xếp theo thời gian kết thúc để lấy lần chạy thành công gần nhất
            extract_dag_runs.sort(key=lambda x: x.end_date if x.end_date else datetime.min, reverse=True)
            print(f"Tìm thấy lần chạy thành công gần nhất của DAG {extract_dag_id}: {extract_dag_runs[0].execution_date}")
            return extract_dag_runs[0].execution_date
            
    except Exception as e:
        print(f"Lỗi khi tìm lần chạy thành công gần nhất: {e}")
        raise e
    
    # Raise lỗi nếu không tìm thấy lần chạy thành công nào
    raise ValueError(f"Không tìm thấy lần chạy thành công nào của DAG 'upsert_all_tables_daily' hoặc 'extract_all_tables'. "
                    f"Vui lòng chạy DAG 'extract_all_tables' trước khi chạy DAG upsert.")

# Tìm thời gian chạy thành công gần nhất để làm start_date
start_date = get_last_successful_run()

# Định nghĩa các tham số mặc định cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='upsert_all_tables_daily',
    default_args=default_args,
    description='Upsert dữ liệu hàng ngày cho tất cả các bảng - Optimized',
    schedule_interval='@daily',
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=3,  # Giới hạn số tasks chạy đồng thời
    concurrency=3,       # Tối đa 3 tasks cùng lúc
    tags=['etl', 'upsert', 'daily', 'optimized'],
)
def upsert_all_tables_daily():
    """
    ### DAG Upsert tất cả bảng hàng ngày
    
    DAG này upsert dữ liệu hàng ngày cho tất cả các bảng từ Postgres vào ClickHouse.
    Nó sẽ chạy với lịch trình hàng ngày và chỉ cho phép tối đa 1 run cùng lúc.
    
    Mỗi task sẽ trích xuất dữ liệu trong khoảng thời gian chỉ định và upsert vào ClickHouse.
    Các task được sắp xếp để đảm bảo thứ tự phụ thuộc đúng giữa các bảng dựa trên các khóa ngoại.
    """
    
    # Dictionary để lưu trữ các task
    tasks = {}
    
    # Tạo các task upsert cho từng bảng
    for table in TABLES:
        # Tạo một instance của UpsertProcess cho bảng hiện tại
        upsert_process = UpsertProcess(table_name=table)  # Thay đổi class
        
        # Thực hiện upsert dữ liệu với Jinja templating đúng cách
        tasks[table] = PythonOperator(
            task_id=f'upsert_{table}',
            python_callable=upsert_process.upsert_to_clickhouse,  # Method giữ nguyên
            templates_dict={
                'start_date': '{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}',
                'end_date': '{{ data_interval_end.strftime("%Y-%m-%d %H:%M:%S") }}'
            },
            retries=2,                           # Tăng retries
            retry_delay=timedelta(minutes=3),    # Giảm retry delay
            retry_exponential_backoff=True,      # Exponential backoff
            max_retry_delay=timedelta(minutes=10),
            pool='default_pool',
            pool_slots=1,                        # Chỉ dùng 1 slot
            trigger_rule='all_success',          # Explicit trigger rule
            # Thêm comment để hiểu rõ hơn về task
            doc_md=f"""
            ## Upsert {table}
            
            Task này trích xuất dữ liệu {table} từ PostgreSQL trong khoảng thời gian chỉ định
            và upsert vào ClickHouse. Nó thực hiện DELETE + INSERT cho dữ liệu trong khoảng
            thời gian từ {{{{ data_interval_start }}}} đến {{{{ data_interval_end }}}}.
            """
        )
    
    # Thiết lập các phụ thuộc dựa trên mối quan hệ giữa các bảng
    
    # product phụ thuộc vào category
    tasks["product"].set_upstream(tasks["category"])
    
    # orders phụ thuộc vào users
    tasks["orders"].set_upstream(tasks["users"])
    
    # topup phụ thuộc vào users
    tasks["topup"].set_upstream(tasks["users"])
    
    # order_product phụ thuộc vào orders và product
    tasks["order_product"].set_upstream([tasks["orders"], tasks["product"]])
    
    # transaction phụ thuộc vào users, orders và topup
    tasks["transaction"].set_upstream([tasks["users"], tasks["orders"], tasks["topup"]])

# Khởi tạo DAG
dag_instance = upsert_all_tables_daily() 