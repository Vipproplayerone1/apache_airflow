"""
Base package for ETL and Upsert operations in Airflow DAGs.

This package provides:
- ETLProcess: Basic ETL operations (extract, load)
- UpsertProcess: Advanced upsert operations with date range support
- Utility functions: Memory monitoring, date validation, S3 cleanup

Version: 1.0.0
Author: ETL Team
"""

from .etl_process import ETLProcess
from .upsert_process import UpsertProcess
from .utils import monitor_memory_usage, validate_date_range, cleanup_s3_file

# Version information
__version__ = "1.0.0"
__author__ = "HNhan"

# Public API - only expose what's needed
__all__ = [
    'ETLProcess', 
    'UpsertProcess', 
    'monitor_memory_usage', 
    'validate_date_range', 
    'cleanup_s3_file'
]

# Package metadata
__package_info__ = {
    "name": "base",
    "version": __version__,
    "description": "Base package for ETL and Upsert operations",
    "main_classes": ["ETLProcess", "UpsertProcess"],
    "utility_functions": ["monitor_memory_usage", "validate_date_range", "cleanup_s3_file"]
}
