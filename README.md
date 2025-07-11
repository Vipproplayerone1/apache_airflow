# 🚀 Apache Airflow ETL Pipeline - PostgreSQL to ClickHouse

## 📋 Tổng Quan Dự Án

Dự án ETL Pipeline sử dụng Apache Airflow để trích xuất dữ liệu từ PostgreSQL, xử lý qua S3 như staging area, và load vào ClickHouse cho mục đích phân tích và báo cáo.

### 🏗️ Kiến Trúc Hệ Thống

```
PostgreSQL (Source) → S3 (Staging) → ClickHouse (Target)
       ↓                ↓               ↓
   OLTP Database    Parquet Files   Analytics DB
```

### 🎯 Mục Tiêu
- **Data Migration**: Chuyển đổi dữ liệu từ PostgreSQL sang ClickHouse
- **Real-time Analytics**: Hỗ trợ phân tích dữ liệu real-time
- **Scalability**: Xử lý lượng dữ liệu lớn với S3 làm staging
- **Reliability**: Đảm bảo tính toàn vẹn dữ liệu với retry mechanism

## 📊 Database Schema

### Tables được xử lý:
- **category** - Danh mục sản phẩm
- **users** - Thông tin người dùng  
- **product** - Thông tin sản phẩm (phụ thuộc category)
- **orders** - Đơn hàng (phụ thuộc users)
- **topup** - Nạp tiền (phụ thuộc users)
- **order_product** - Chi tiết đơn hàng (phụ thuộc orders + product)
- **transaction** - Giao dịch (phụ thuộc users + orders + topup)

### Dependencies Flow:
```
category ← product
users ← orders, topup
orders, product ← order_product
orders, topup ← transaction
```

## 🎛️ Airflow DAGs

### 1. **extract_all_tables** (DAG chính)
- **Mục đích**: Extract toàn bộ dữ liệu từ PostgreSQL sang ClickHouse
- **Execution**: Sequential để tránh overload worker
- **Tối ưu**: 7 tasks (thay vì 21), concurrency=1
- **Thời gian**: 3-5 phút

### 2. **upsert_all_tables_daily** 
- **Mục đích**: Upsert dữ liệu hàng ngày
- **Schedule**: @daily 
- **Tính năng**: Tự động cleanup S3 sau khi upsert
- **Dependencies**: Theo thứ tự phụ thuộc bảng

### 3. **upsert_demo_manual_sequential**
- **Mục đích**: Demo upsert với resource monitoring
- **Execution**: Manual trigger với parameters
- **Tối ưu**: Memory optimization, system resource check
- **Batch processing**: Configurable batch size

### 4. **extract_[table_name]** (Individual DAGs)
- **Mục đích**: Extract từng bảng riêng biệt
- **Tính năng**: Không cleanup S3 (giữ lại để backup)
- **Tables**: category, users, product, orders, topup, transaction, order_product

## 🛠️ Cấu Trúc Project

```
apache_airflow/
├── dags/                          # Airflow DAGs
│   ├── base/
│   │   └── etl_process.py        # ETL Process class chính
│   ├── extract_all_tables.py     # DAG extract tất cả bảng
│   ├── upsert_all_tables_daily.py # DAG upsert hàng ngày
│   ├── upsert_demo_manual.py     # DAG demo upsert
│   └── extract_[table].py        # DAG extract từng bảng
├── sql/                          # SQL Scripts
│   ├── postgres/
│   │   ├── ddl/                  # PostgreSQL table definitions
│   │   ├── insert/               # Insert sample data
│   │   └── extract/              # Extract queries
│   └── clickhouse/
│       └── ddl/                  # ClickHouse table definitions
├── data/                         # Local data files
├── logs/                         # Airflow logs
├── plugins/                      # Airflow plugins
├── docker-compose.yaml           # Docker services
└── *.py                         # Utility scripts
```

## 🔧 Utility Scripts

### Data Management
- `add_upsert_demo_data.py` - Thêm dữ liệu demo cho upsert
- `reset_postgres_db.py` - Reset PostgreSQL database
- `clear_clickhouse_data.py` - Xóa dữ liệu ClickHouse

### Monitoring & Verification
- `check_table_schemas.py` - Kiểm tra schema bảng
- `compare_psql_clickhouse.py` - So sánh dữ liệu giữa PostgreSQL và ClickHouse

### Setup & Configuration
- `setup_airflow_connections.py` - Setup Airflow connections
- `docker-compose.yaml` - Cấu hình Docker services

## 🚀 Cách Chạy Dự Án

### 1. Start Services
```bash
docker-compose up -d
```

### 2. Setup Connections
```bash
python setup_airflow_connections.py
```

### 3. Chạy ETL Pipeline

#### Option 1: Extract All Tables (Recommended)
1. Mở **Airflow UI**: http://localhost:8080
2. Đăng nhập: `airflow` / `airflow`
3. Tìm DAG: **`extract_all_tables`**
4. Click **"Trigger DAG"**

#### Option 2: Upsert Daily
1. Tìm DAG: **`upsert_all_tables_daily`**
2. Trigger với date range parameters

#### Option 3: Manual Upsert Demo
1. Tìm DAG: **`upsert_demo_manual_sequential`**
2. Trigger với parameters:
```json
{
  "start_date": "2025-01-01 00:00:00",
  "end_date": "2025-01-02 00:00:00",
  "batch_size": 5000
}
```

### 4. Verification
```bash
python compare_psql_clickhouse.py
```

## 📈 Monitoring & Performance

### Airflow UI Views
- **Graph View**: Xem task dependencies
- **Tree View**: Xem execution history
- **Gantt View**: Xem timeline execution
- **Logs**: Debug failed tasks

### Performance Metrics
- **Extract DAG**: 3-5 phút (7 tasks)
- **Upsert DAG**: 2-3 phút (depends on data volume)
- **Resource Usage**: Optimized với concurrency limits
- **S3 Cleanup**: Tự động cho upsert DAGs

## 🎯 Tính Năng Nâng Cao

### 1. **ETL Process Class** (`dags/base/etl_process.py`)
- Centralized logic cho tất cả ETL operations
- Memory optimization với configurable batch size
- Automatic S3 cleanup cho upsert operations
- Error handling và retry mechanism

### 2. **S3 Staging Management**
- **Extract operations**: Giữ lại files trên S3 (backup)
- **Upsert operations**: Tự động cleanup sau khi load
- Parquet format với snappy compression
- Organized folder structure: `data/{table_name}/`

### 3. **ClickHouse Integration**
- Dynamic table creation từ DDL files
- Efficient data loading từ S3 sử dụng s3() function
- Proper upsert logic với temporary tables
- Support cho ReplacingMergeTree engine

### 4. **Resource Optimization**
- Sequential execution để tránh worker overload
- Configurable concurrency limits
- Memory monitoring và garbage collection
- Exponential backoff retry strategy

## 🚨 Troubleshooting

### Common Issues
1. **Worker Overload**: Giảm concurrency, tăng retry delay
2. **Connection Errors**: Verify PostgreSQL, ClickHouse, S3 connections
3. **Memory Issues**: Giảm batch size, enable garbage collection
4. **Data Consistency**: Kiểm tra dependencies between tables

### Debug Steps
1. Check Airflow logs trong UI
2. Verify data trong source tables
3. Test connections manually
4. Run comparison scripts

## 📋 Requirements

### Infrastructure
- **Docker & Docker Compose**
- **PostgreSQL 13+**
- **ClickHouse 21+**
- **Apache Airflow 2.5+**
- **S3 Compatible Storage**

### Python Dependencies
- `apache-airflow[postgres,s3]`
- `airflow-clickhouse-plugin`
- `pandas`
- `pyarrow`
- `psutil`

## 🎯 Best Practices

1. **Data Consistency**: Chạy extract_all_tables trước khi upsert
2. **Resource Management**: Monitor worker resource usage
3. **Error Handling**: Kiểm tra logs khi DAG fail
4. **Backup Strategy**: Extract DAGs giữ lại S3 files cho backup
5. **Monitoring**: Thường xuyên verify data consistency

**Dự án ETL Pipeline đã được tối ưu và sẵn sàng sử dụng! 🚀**

