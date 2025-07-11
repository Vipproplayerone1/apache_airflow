#!/usr/bin/env python3
"""
Script để setup Airflow connections và variables cần thiết cho ETL pipeline
"""

from airflow.models import Connection, Variable
from airflow.settings import Session
import os

def setup_connections():
    """Setup các connections cần thiết cho Airflow"""
    
    session = Session()
    
    # Connection cho PostgreSQL
    postgres_conn = Connection(
        conn_id='postgres_conn_id',
        conn_type='postgres',
        host='postgres',  # Docker service name
        login='airflow',
        password='airflow',
        schema='airflow',
        port=5432
    )
    
    # Connection cho ClickHouse
    clickhouse_conn = Connection(
        conn_id='clickhouse_conn_id',
        conn_type='clickhouse',
        host='clickhouse',  # Docker service name
        login='myuser',
        password='mypassword',
        schema='mydatabase',
        port=9000
    )
    
    # Connection cho AWS S3
    aws_conn = Connection(
        conn_id='aws_conn_id',
        conn_type='aws',
        login='your_aws_access_key',  # Cần thay đổi
        password='your_aws_secret_key',  # Cần thay đổi
        extra='{"region_name": "us-east-1"}'  # Cần thay đổi region
    )
    
    # Thêm hoặc cập nhật connections
    connections = [postgres_conn, clickhouse_conn, aws_conn]
    
    for conn in connections:
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
        if existing_conn:
            print(f"Updating existing connection: {conn.conn_id}")
            existing_conn.conn_type = conn.conn_type
            existing_conn.host = conn.host
            existing_conn.login = conn.login
            existing_conn.password = conn.password
            existing_conn.schema = conn.schema
            existing_conn.port = conn.port
            existing_conn.extra = conn.extra
        else:
            print(f"Creating new connection: {conn.conn_id}")
            session.add(conn)
    
    session.commit()
    session.close()
    print("Connections setup completed!")

def setup_variables():
    """Setup các variables cần thiết cho Airflow"""
    
    # Variables cần thiết
    variables = {
        's3-bucket': 'your-s3-bucket-name',  # Cần thay đổi
        'aws_access_key': 'your_aws_access_key',  # Cần thay đổi
        'aws_secret_key': 'your_aws_secret_key'   # Cần thay đổi
    }
    
    for key, value in variables.items():
        existing_var = Variable.get(key, default_var=None)
        if existing_var is None:
            print(f"Creating new variable: {key}")
            Variable.set(key, value)
        else:
            print(f"Variable {key} already exists, skipping...")
    
    print("Variables setup completed!")

def main():
    """Main function"""
    print("Setting up Airflow connections and variables...")
    
    try:
        setup_connections()
        setup_variables()
        print("\nSetup completed successfully!")
        print("\nIMPORTANT: Please update the following values:")
        print("1. AWS credentials in aws_conn_id connection")
        print("2. S3 bucket name in s3-bucket variable")
        print("3. AWS access/secret keys in variables")
        
    except Exception as e:
        print(f"Setup failed: {str(e)}")
        print("Please make sure Airflow is running and you have proper permissions.")

if __name__ == "__main__":
    main() 