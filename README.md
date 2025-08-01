# Анализ сайтов

## Порты
- http://localhost:8080: **Airflow UI**
- http://localhost:9001: **MinIO Console**
- http://localhost:8123: **HTTP-интерфейс ClickHouse**
- http://localhost:8088: **Spark Master**
- http://localhost:8081: **Spark Worker**
- http://localhost:8888/?token=airflow **For development**


## Исходные данные 
Загружаем нужные сайты для анализа -> input_files/yw_list_ids_all_sites.txt
Формат списка, каждый новый сайт с новой строки.
Пример записи: https:domain.ru:443

## Дополнительные библиотеки
В папку "jars" загружаем 2 файла которые нужны для выполнения скриптов спарку. 
Скачиваем по ссылкам:
- https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar
- https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar


## Токен от Яндекса
Перед работой нужно получить токен от приложения яндекса.