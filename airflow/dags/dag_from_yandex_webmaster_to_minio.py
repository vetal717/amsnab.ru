from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.models import Variable


yandex_access_token = Variable.get("YANDEX_ACCESS_TOKEN", default_var=None)
conn = BaseHook.get_connection("minio_s3_conn")
extra = conn.extra_dejson  # парсится как dict
endpoint_url = extra.get("endpoint_url")
aws_access_key_id = extra.get("aws_access_key_id")
aws_secret_access_key = extra.get("aws_secret_access_key")


default_args = {
    'start_date': datetime(2025, 7, 25),
}

with DAG(
    dag_id='from_yandex_webmaster_to_minio_4',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Run PySpark job writing to MinIO',
) as dag:

    run_external_spark_job = SparkSubmitOperator(
        task_id='run_external_spark_job',
        application='/opt/airflow/spark_jobs/from_yandex_webmaster_to_minio.py',  # путь до скрипта (volume!)
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