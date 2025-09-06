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
import time

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

def check_docker_limits():
    """Simple resource check - thay thế Adaptive Throttling phức tạp"""
    memory_percent = psutil.virtual_memory().percent
    cpu_percent = psutil.cpu_percent(interval=1)
    
    print(f"📊 System resources: Memory {memory_percent}%, CPU {cpu_percent}%")
    
    # Simple monitoring thay vì complex throttling
    if memory_percent > 85:
        print(f"⚠️  Approaching Docker memory limit: {memory_percent}%")
        print("💾 Triggering garbage collection...")
        import gc
        gc.collect()
        time.sleep(5)  # Simple delay thay vì complex throttling
        return True
    elif memory_percent > 70:
        print(f"ℹ️  Elevated memory usage: {memory_percent}%")
        time.sleep(2)  # Light delay
        return True
    
    if cpu_percent > 90:
        print(f"⚠️  High CPU usage: {cpu_percent}%")
        time.sleep(3)  # Simple delay
        return True
    
    print("✅ Resources within Docker limits")
    return True

def simple_upsert_with_monitoring(**context):
    """Simple upsert function với basic resource monitoring - thay thế adaptive throttling"""
    table_name = context['table_name']
    
    # Tracking execution time
    start_time = time.time()
    print(f"🚀 Starting upsert for {table_name} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Simple resource check thay vì complex throttling
    check_docker_limits()
    
    # Lấy batch_size từ templates_dict hoặc sử dụng default - tăng từ 5000 lên 10000
    batch_size = context.get('templates_dict', {}).get('batch_size', 10000)
    if isinstance(batch_size, str):
        batch_size = int(batch_size)
    
    print(f"📊 Using batch_size: {batch_size} for table {table_name}")
    
    # Tạo UpsertProcess instance với batch_size tối ưu
    upsert_process = UpsertProcess(table_name=table_name, batch_size=batch_size)
    
    # Chạy upsert với simple monitoring
    try:
        print(f"🔄 Starting upsert...")
        result = upsert_process.upsert_to_clickhouse(**context)
        
        # Track execution time
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"✅ Completed upsert for {table_name} in {execution_time:.2f} seconds")
        
        # Simple delay thay vì complex throttling
        time.sleep(1)  # Fixed 1s delay
        
        return result
    except Exception as e:
        print(f"❌ Upsert failed for {table_name}: {str(e)}")
        # Log memory usage khi fail
        memory_percent = psutil.virtual_memory().percent
        print(f"💾 Memory usage at failure: {memory_percent}%")
        raise

@dag(
    dag_id='upsert_demo_manual_sequential',
    default_args=default_args,
    description='Demo Upsert Sequential với Simple Monitoring - Tối ưu hóa hiệu suất với basic resource checks',
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
        "batch_size": 10000  # Tăng từ 5000 lên 10000 để tối ưu performance
    }
)
def upsert_demo_manual_sequential():
    """
    ### DAG Demo Upsert Sequential (Optimized Performance)
    
    DAG này chạy từng task một với simple monitoring và tối ưu hóa hiệu suất:
    
    **Optimizations:**
    - Chạy tuần tự (sequential) - chỉ 1 task cùng lúc
    - Simple resource monitoring thay vì complex throttling
    - Tăng batch size từ 5000 lên 10000 records
    - Memory optimization với Docker limits
    - Basic garbage collection checks
    - Exponential backoff retry
    
    **Cách sử dụng:**
    1. Trigger DAG
    2. Nhập parameters JSON:
       ```json
       {
         "start_date": "2025-07-03 00:00:00",
         "end_date": "2025-07-04 00:00:00",
         "resource_check": true,
         "batch_size": 10000
       }
       ```
    3. Monitor performance qua Airflow UI
    
    **Resource Management:**
    - Max 1 task concurrent (sequential execution)
    - Docker resource limits (4GB memory, 2 CPU)
    - Simple memory checks với basic GC
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
            python_callable=simple_upsert_with_monitoring,
            
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