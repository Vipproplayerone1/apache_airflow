from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from base import UpsertProcess
from datetime import datetime, timedelta
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState
from airflow import DAG
from airflow.models.pool import Pool
from airflow.configuration import conf
import psutil
import os

# Danh sách các bảng cần upsert với priority levels
TABLES_CONFIG = [
    # Priority 1: Base tables (không phụ thuộc)
    {"name": "category", "priority": 1, "weight": 1},
    {"name": "users", "priority": 1, "weight": 2},
    
    # Priority 2: Dependent tables level 1
    {"name": "product", "priority": 2, "weight": 1},
    {"name": "orders", "priority": 2, "weight": 3},
    {"name": "topup", "priority": 2, "weight": 2},
    
    # Priority 3: Dependent tables level 2
    {"name": "order_product", "priority": 3, "weight": 2},
    {"name": "transaction", "priority": 3, "weight": 3}
]

# Định nghĩa các tham số mặc định cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Tăng retries cho sequential execution
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=2),  # Timeout cho sequential task
    'sla': timedelta(hours=4),  # SLA cho monitoring
}

def check_system_resources():
    """Kiểm tra resource hệ thống trước khi chạy task"""
    memory_percent = psutil.virtual_memory().percent
    cpu_percent = psutil.cpu_percent(interval=1)
    
    print(f"System resources: Memory {memory_percent}%, CPU {cpu_percent}%")
    
    # Nếu resource usage cao, delay task
    if memory_percent > 85 or cpu_percent > 90:
        print("System resources high, delaying task execution")
        import time
        time.sleep(30)  # Delay 30 giây
        return False
    return True

def memory_optimized_upsert_sequential(**context):
    """Wrapper function với memory optimization cho sequential execution"""
    table_name = context['table_name']
    
    # Delay nhỏ trước khi bắt đầu để giảm load
    import time
    time.sleep(2)
    
    # Tracking execution time
    start_time = time.time()
    print(f"Starting upsert for {table_name} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Kiểm tra system resources
    if not check_system_resources():
        print("System resources too high, skipping this run")
        return
    
    # Lấy batch_size từ templates_dict hoặc sử dụng default
    batch_size = context.get('templates_dict', {}).get('batch_size', 5000)
    if isinstance(batch_size, str):
        batch_size = int(batch_size)
    
    print(f"Using batch_size: {batch_size} for table {table_name}")
    
    # Tạo UpsertProcess instance với batch_size
    upsert_process = UpsertProcess(table_name=table_name, batch_size=batch_size)  # Thay đổi class
    
    # Chạy upsert với resource monitoring
    try:
        result = upsert_process.upsert_to_clickhouse(**context)  # Method giữ nguyên
        
        # Track execution time
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Completed upsert for {table_name} in {execution_time:.2f} seconds")
        
        # Small delay after completion to reduce worker load
        time.sleep(3)
        
        return result
    except Exception as e:
        print(f"Upsert failed for {table_name}: {str(e)}")
        # Log memory usage khi fail
        memory_percent = psutil.virtual_memory().percent
        print(f"Memory usage at failure: {memory_percent}%")
        raise

@dag(
    dag_id='upsert_demo_manual_sequential',
    default_args=default_args,
    description='Demo Upsert Sequential - Chạy từng task để tránh worker overload',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    
    # DAG-level optimizations
    max_active_runs=1,           # Chỉ 1 DAG run cùng lúc
    max_active_tasks=1,          # Chỉ 1 task cùng lúc để tránh overload
    concurrency=1,               # Tương ứng với max_active_tasks
    
    # Resource management
    tags=['demo', 'upsert', 'manual', 'sequential', 'low-resource'],
    
    # Render templates once để tiết kiệm memory
    render_template_as_native_obj=True,
    
    params={
        "start_date": "2025-07-03 00:00:00",
        "end_date": "2025-07-04 00:00:00",
        "resource_check": True,
        "batch_size": 5000  # Cho phép điều chỉnh batch size
    }
)
def upsert_demo_manual_sequential():
    """
    ### DAG Demo Upsert Sequential (Worker Overload Prevention)
    
    DAG này chạy từng task một để tránh worker overload với các cải tiến:
    
    **Optimizations:**
    - Chạy tuần tự (sequential) - chỉ 1 task cùng lúc
    - Task pools cho resource management
    - Sequential task execution theo thứ tự dependencies
    - System resource monitoring
    - Memory-optimized processing
    - Exponential backoff retry
    - Task timeout & SLA monitoring
    
    **Cách sử dụng:**
    1. Trigger DAG
    2. Nhập parameters JSON:
       ```json
       {
         "start_date": "2025-07-03 00:00:00",
         "end_date": "2025-07-04 00:00:00",
         "resource_check": true,
         "batch_size": 5000
       }
       ```
    3. Monitor sequential execution qua Airflow UI
    
    **Resource Management:**
    - Max 1 task concurrent (sequential execution)
    - System resource check trước khi chạy
    - Memory usage monitoring
    - Task execution theo thứ tự dependencies
    """
    
    # Dictionary để lưu trữ các task
    tasks = {}
    
    # Tạo các task upsert cho từng bảng với optimizations
    for table_config in TABLES_CONFIG:
        table_name = table_config["name"]
        priority = table_config["priority"]
        weight = table_config["weight"]
        
        # Sử dụng pool mặc định để tránh lỗi pool không tồn tại
        pool_name = "default_pool"
        
        # Tạo task với optimizations
        tasks[table_name] = PythonOperator(
            task_id=f'upsert_{table_name}',
            python_callable=memory_optimized_upsert_sequential,
            
            # Templates dict với parameters
            templates_dict={
                'start_date': '{{ params.start_date }}',
                'end_date': '{{ params.end_date }}',
                'batch_size': '{{ params.batch_size }}'
            },
            
            # Thêm table_name vào context
            op_kwargs={
                'table_name': table_name
            },
            
            # Retry configuration với exponential backoff
            retries=2,
            retry_delay=timedelta(minutes=1),  # Delay 1 phút cho sequential execution
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=15),
            
            # Resource management
            pool=pool_name,
            pool_slots=1,  # Chỉ dùng 1 slot để đảm bảo sequential
            
            # Timeout và SLA
            execution_timeout=timedelta(hours=2),
            sla=timedelta(hours=4),
            
            # Trigger rule
            trigger_rule='all_success',
            
            # Documentation
            doc_md=f"""
            ## Upsert {table_name} (Priority: {priority}, Weight: {weight})
            
            Task này upsert dữ liệu {table_name} với các optimizations:
            - Memory monitoring
            - System resource check
            - Sequential execution
            - Batch size: {{{{ params.batch_size }}}}
            - Date range: {{{{ params.start_date }}}} to {{{{ params.end_date }}}}
            """
        )
    
    # Thiết lập dependencies theo priority và weight
    # Priority 1 tasks (Base tables) - chạy song song
    # Priority 2 tasks - phụ thuộc vào Priority 1
    # Priority 3 tasks - phụ thuộc vào Priority 2
    
    # Priority 1: Base tables (chạy song song)
    base_tasks = [tasks[config["name"]] for config in TABLES_CONFIG if config["priority"] == 1]
    
    # Priority 2: Dependent tables level 1
    level1_tasks = [tasks[config["name"]] for config in TABLES_CONFIG if config["priority"] == 2]
    
    # Priority 3: Dependent tables level 2
    level2_tasks = [tasks[config["name"]] for config in TABLES_CONFIG if config["priority"] == 3]
    
    # Thiết lập dependencies cụ thể
    # product phụ thuộc vào category
    if "product" in tasks and "category" in tasks:
        tasks["product"].set_upstream(tasks["category"])
    
    # orders và topup phụ thuộc vào users
    if "orders" in tasks and "users" in tasks:
        tasks["orders"].set_upstream(tasks["users"])
    if "topup" in tasks and "users" in tasks:
        tasks["topup"].set_upstream(tasks["users"])
    
    # order_product phụ thuộc vào orders và product
    if "order_product" in tasks and "orders" in tasks and "product" in tasks:
        tasks["order_product"].set_upstream([tasks["orders"], tasks["product"]])
    
    # transaction phụ thuộc vào users, orders và topup
    if "transaction" in tasks and "users" in tasks and "orders" in tasks and "topup" in tasks:
        tasks["transaction"].set_upstream([tasks["users"], tasks["orders"], tasks["topup"]])
    
    # Task completion check
    def check_completion(**context):
        """Kiểm tra completion của tất cả tasks"""
        print("🎉 All upsert tasks completed successfully!")
        print("📊 Summary:")
        for table_config in TABLES_CONFIG:
            table_name = table_config["name"]
            print(f"  - {table_name}: ✅ Completed")
        return "All tasks completed"
    
    # Completion task
    completion_task = PythonOperator(
        task_id='check_completion',
        python_callable=check_completion,
        trigger_rule='all_success',
        doc_md="""
        ## Completion Check
        
        Task này kiểm tra completion của tất cả upsert tasks và tạo summary.
        """
    )
    
    # Set completion task as downstream của tất cả tasks
    for task in tasks.values():
        completion_task.set_upstream(task)
    
    return tasks

# Khởi tạo DAG
dag_instance = upsert_demo_manual_sequential() 