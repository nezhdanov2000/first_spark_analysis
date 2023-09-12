# Используйте образ, включающий в себя Python и Java
FROM openjdk:11-jre

# Установите Python, инструменты для управления пакетами Python и Pandas
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip3 install pandas

# Установите Apache Spark
RUN pip3 install pyspark

# Установите openpyxl
RUN pip3 install openpyxl

# Копируйте ваш скрипт в контейнер
COPY script.py /app/script.py
COPY online_retail.xlsx /app/online_retail.xlsx

# Задайте рабочую директорию
WORKDIR /app

# Запустите ваш скрипт при запуске контейнера
CMD ["python3", "script.py"]
