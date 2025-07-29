from pyspark.sql import SparkSession



spark = SparkSession.builder \
    .appName("from_yandex_webmaster_to_minio") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


# Уровень логов — скрываем INFO
spark.sparkContext.setLogLevel("ERROR")

# Создаём простой датафрейм
data = [("Alice", 1), ("Bob", 2), ("Catherine", 3)]
df = spark.createDataFrame(data, ["name", "id"])

# Сохраняем его в MinIO в формате Parquet
df.coalesce(1).write.mode("overwrite").parquet("s3a://yandex-webmaster/pages_index")

print("✅ DataFrame успешно записан в MinIO")

spark.stop()