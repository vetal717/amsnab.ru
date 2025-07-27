FROM apache/airflow:2.10.5-python3.11

USER root

# apt-get update && - Обновляет список доступных пакетов из репозиториев
# apt-get install -y  — автоматически отвечает "да" на все вопросы (yes). 
# --no-install-recommends — устанавливает только самые необходимые пакеты, без дополнительных зависимостей
# openjdk-11-jdk \ - Устанавливает Java Development Kit версии 11 — нужен для запуска Apache Spark, так как Spark работает на Java.
# curl && \ - Устанавливает curl, инструмент командной строки для скачивания файлов по HTTP/HTTPS. Он нужен дальше для загрузки Spark.
# apt-get clean && \ - Очищает временные данные apt, чтобы уменьшить размер образа (удаляет кэш установленных пакетов).
# rm -rf /var/lib/apt/lists/* - Удаляет файлы с метаинформацией о пакетах (скачанные списки .deb), тоже чтобы сократить размер образа.

RUN apt-get update && \ 
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        curl \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Скачивает архив с Apache Spark 3.5.0 (бинарная сборка для Hadoop 3) с официального архива Apache
# curl -s — загружает архив в "тихом режиме".
# | tar xz -C /usr/local — распаковывает архив прямо в папку /usr/local.
# ln -s /usr/local/spark-3.5.0-bin-hadoop3 /usr/local/spark && \ - Создаёт символьную ссылку
# chown -R airflow:airflow /usr/local/spark* - Меняет владельца установленного Spark на пользователя airflow. Это нужно, чтобы Airflow-процессы могли работать с этой директорией (Spark).

RUN curl -s https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz | tar xz -C /usr/local && \
    ln -s /usr/local/spark-3.5.0-bin-hadoop3 /usr/local/spark && \
    chown -R airflow /usr/local/spark*

# Устанавливаем необходимые JAR-файлы    
RUN mkdir -p /opt/jars && \
    curl -L -o /opt/jars/hadoop-aws-3.3.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    curl -L -o /opt/jars/aws-java-sdk-bundle-1.11.901.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar && \
    chown -R airflow /opt/jars

ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow