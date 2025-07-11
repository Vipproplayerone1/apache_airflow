import os
import psutil
import gc
from datetime import datetime

def monitor_memory_usage(operation: str = "", max_memory_mb: int = 512):
    """
    Utility function để monitor memory usage
    Có thể dùng chung cho cả ETLProcess và UpsertProcess
    """
    memory_percent = psutil.virtual_memory().percent
    process = psutil.Process(os.getpid())
    process_memory_mb = process.memory_info().rss / 1024 / 1024
    
    print(f"Memory usage {operation}: {memory_percent}% (Process: {process_memory_mb:.1f}MB)")
    
    if memory_percent > 80:
        print(f"WARNING: High memory usage ({memory_percent}%) during {operation}")
        
    if process_memory_mb > max_memory_mb:
        print(f"INFO: Forcing garbage collection (Process memory: {process_memory_mb:.1f}MB)")
        gc.collect()
        
    return memory_percent, process_memory_mb

def validate_date_range(start_date, end_date):
    """
    Utility function để validate date range
    """
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required")
    
    try:
        datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError(f"Invalid date format. Expected 'YYYY-MM-DD HH:MM:SS', got: {start_date}, {end_date}")
    
    if start_date >= end_date:
        raise ValueError(f"start_date ({start_date}) must be before end_date ({end_date})")

def cleanup_s3_file(s3_hook, s3_key, bucket_name):
    """
    Utility function để cleanup S3 files
    """
    try:
        if s3_key:
            s3_hook.delete_object(key=s3_key, bucket_name=bucket_name)
            print(f"Cleaned up S3 file: {s3_key}")
    except Exception as e:
        print(f"Failed to cleanup S3 file {s3_key}: {str(e)}") 