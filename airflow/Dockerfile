FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk procps &&\
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Устанавливаем JAVA_HOME и добавляем java в PATH
ENV JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

RUN pip uninstall -y pyspark && \
    pip install --upgrade pip --no-cache-dir pyspark==3.5.0 apache-airflow-providers-apache-spark

