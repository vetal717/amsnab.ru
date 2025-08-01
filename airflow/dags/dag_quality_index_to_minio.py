from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import os


STATUS_FILE = "quality_index"
APPLICATION = "/opt/airflow/spark_jobs/quality_index_to_minio.py"
DAG_ID="quality_index_to_minio_1" 
DESCRIPTION="Возвращает историю изменения индекса качества сайта (ИКС)."

yandex_access_token = Variable.get("YANDEX_ACCESS_TOKEN", default_var=None)
conn = BaseHook.get_connection("minio_s3_conn")
extra = conn.extra_dejson  # парсится как dict
endpoint_url = extra.get("endpoint_url")
aws_access_key_id = extra.get("aws_access_key_id")
aws_secret_access_key = extra.get("aws_secret_access_key")


def check_spark_status(**context):
    start_date = context["yesterday_ds"]
    global STATUS_FILE
    STATUS_FILE = f"/opt/temporarily/{STATUS_FILE}_{start_date}.txt"
    try:
        with open(STATUS_FILE, "r") as f:
            status = f.read().strip()
        
        if status == "NO_DATA":
            raise AirflowSkipException("❌ No data for this date, skipping the task.")
        elif status == "OK":
            return "✅ Data processed successfully"
        else:
            raise Exception("Unknown status")
    finally:
        try:
            if os.path.exists(STATUS_FILE):
                os.remove(STATUS_FILE)
                print(f"Файл статуса {STATUS_FILE} удалён.")
        except Exception as e:
            print(f"Ошибка при удалении файла статуса: {e}")


default_args = {
    "start_date": datetime(2025, 7, 25),
    "retries": 0
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="0 2 * * *",
    catchup=True,
    description=DESCRIPTION,
    max_active_runs=10
) as dag:

    run_external_spark_job = SparkSubmitOperator(
        task_id='run_external_spark_job',
        application=APPLICATION,  # путь до скрипта (volume)
        conn_id='spark_default',  # "Admin → Connections"
        conf={
            'spark.hadoop.fs.s3a.endpoint': endpoint_url,
            'spark.hadoop.fs.s3a.access.key': aws_secret_access_key,
            'spark.hadoop.fs.s3a.secret.key': aws_access_key_id,
            'spark.hadoop.fs.s3a.path.style.access': 'true',
        },
        jars='/opt/jars/hadoop-aws-3.3.1.jar,/opt/jars/aws-java-sdk-bundle-1.11.901.jar',
        application_args=[
            '--start_date', '{{ yesterday_ds }}',
            '--end_date', '{{ ds }}',
            '--minio_access_key', aws_access_key_id,
            '--minio_secret_key', aws_secret_access_key,
            '--yandex_access_token', yandex_access_token
        ]
    )

    check_status = PythonOperator(
    task_id='check_status',
    python_callable=check_spark_status
    )

    run_external_spark_job >> check_status