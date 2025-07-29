FROM bitnami/spark:3.5.0

USER root

# Установка pip, если еще не установлен (для образов Bitnami часто уже есть)
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --upgrade pip

# Устанавливаем необходимые библиотеки
RUN pip3 install requests pandas

# Возвращаем пользователя spark (опционально, если требуется безопасность)
USER 1001