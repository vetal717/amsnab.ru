import sys
import json
sys.path.insert(0, '/opt/airflow/lib') # Добавляем путь, где теперь доступна библиотека
sys.path.insert(0, '/opt/spark/lib') # Добавляем путь, где теперь доступна библиотека
from yandex_webmaster import YandexWebmasterAPI  # Импортируем модуль
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import to_date, year, month


# Достаем переданные даты из Dag
parser = argparse.ArgumentParser()
parser.add_argument('--start_date', type=str, required=True, help='Start date in YYYY-MM-DD format')
parser.add_argument('--end_date', type=str, required=True, help='End date in YYYY-MM-DD format')
parser.add_argument('--minio_access_key', required=True)
parser.add_argument('--minio_secret_key', required=True)
parser.add_argument('--yandex_access_token', required=True)
args = parser.parse_args()
start_date = args.start_date
end_date = args.end_date
minio_access_key = args.minio_access_key
minio_secret_key = args.minio_secret_key
yandex_access_token = args.yandex_access_token

obj_webmaster = YandexWebmasterAPI(access_token=yandex_access_token)

sites = ["https:amsnab.ru:443", "https:balashiha.amsnab.ru:443"]
# sites = ["https:amsnab.ru:443", "https:balashiha.amsnab.ru:443", "https:bronnicy.amsnab.ru:443", "https:chekhov.amsnab.ru:443", "https:chernogolovka.amsnab.ru:443"]

# site = "https:amsnab.ru:443"

result_list = [] # Список для хранения данных
for site in sites:
    data = obj_webmaster.getting_history_changes_number_pages_search(site, start_date, end_date)
    result_list.append(data)
    
    # Здесь нужно проверить, есть ли данные или нет
    if not result_list or all(item == '[]' for item in result_list):
        sys.exit(0)

# Список словарей (dict) полученных из json.loads
data = [json.loads(item)[0] for item in result_list]

spark = SparkSession.builder \
    .appName("from_yandex_webmaster_to_minio") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


# Уровень логов — скрываем INFO
spark.sparkContext.setLogLevel("ERROR")


schema = StructType([
    StructField("domain", StringType(), True),
    StructField("date", StringType(), True),  # сначала строка, потом можно преобразовать в timestamp
    StructField("value", IntegerType(), True),
])

df_yw = spark.createDataFrame(data, schema=schema)
df_yw = df_yw.withColumn("date", to_date("date"))\
             .withColumn("year", year("date"))\
             .withColumn("month", month("date"))

# Сохраняем его в MinIO в формате Parquet
df_yw.coalesce(1).write.mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("s3a://yandex-webmaster/pages_index")

print("✅ DataFrame успешно записан в MinIO")

spark.stop()