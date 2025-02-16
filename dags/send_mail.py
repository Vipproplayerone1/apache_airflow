from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import requests

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 10),
    "email": 'nhanthanh535@gmail.com',
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    description="Gửi email dự báo thời tiết Đà Nẵng ngay khi được bật",
    schedule_interval=None,  # Chạy ngay khi được bật
    catchup=False,
    tags=["weather", "email"],
)
def daily_weather_email():
    @task()
    def get_weather():
        city = "DaNang"
        response = requests.get(f"https://wttr.in/{city}?format=%C+%t")
        return response.text
    
    @task()
    def save_weather_to_file(weather):
        file_path = "/tmp/weather_report.txt"
        with open(file_path, "w") as file:
            file.write(f"Thời tiết hiện tại ở Đà Nẵng: {weather}")
        return file_path
    
    @task()
    def read_weather_from_file(file_path):
        with open(file_path, "r") as file:
            return file.read()
    
    weather = get_weather()
    file_path = save_weather_to_file(weather)
    email_content = read_weather_from_file(file_path)
    
    send_email_task = EmailOperator(
        task_id="send_weather_email",
        to="nhanthanh535@gmail.com",
        subject="Dự báo thời tiết Đà Nẵng hôm nay",
        html_content=email_content,
    )
    
    send_email_task.set_upstream(email_content)

dag_instance = daily_weather_email()
