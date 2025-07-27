from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 23),
}

with DAG(
    dag_id='from_yandex_webmaster_to_minio',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Run PySpark job writing to MinIO',
) as dag:

    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --jars /opt/jars/hadoop-aws-3.3.1.jar,/opt/jars/aws-java-sdk-bundle-1.11.901.jar \
        /opt/airflow/spark_jobs/from_yandex_webmaster_to_minio.py
        """
    )