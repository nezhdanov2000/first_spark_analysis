# Импорт библиотек
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import count_distinct
from pyspark.sql.functions import count
from pyspark.sql.functions import round
from pyspark.sql.functions import sum

# ----------------------------------------------------------------------------------------------

# Преобразование файла из .xlsx в .csv формат
df = pd.read_excel('online_retail.xlsx')
df.info()
df.head()
df.tail()
df.to_csv('online_retail.csv', index = False, sep = ';')

# ----------------------------------------------------------------------------------------------

# Инициализация Spark-сессии
spark = SparkSession.builder\
    .master("local[*]")\
    .appName("SparkFirst")\
    .config("spark.executor.memory", "10g")\
    .config("spark.executor.cores", 5)\
    .config("spark.dynamicAllocation.enabled", "true")\
    .config("spark.dynamicAllocation.maxExecutors", 5)\
    .config("spark.shuffle.service.enabled", "true")\
.getOrCreate()

# ----------------------------------------------------------------------------------------------

# Создание dataframe из скачанного файла
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampNTZType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", FloatType(), True),
    #StructField("CustomerID",IntegerType(), True),
    StructField("Country", StringType(), True),
    ])

df_spark = spark.read.csv('online_retail.csv', sep = ";", header=True, schema=schema)
df_spark.show(10)
df_spark.dtypes

# ----------------------------------------------------------------------------------------------

# Расчет показателей:
# 1 Количество строк в файле
df_spark.count()
# 2 Количество уникальных клиентов
df_spark.select(count_distinct("CustomerID")).show()
# 3 В какой стране совершается больше покупок
df_spark.select("Country").groupby("Country").count().sort("count", ascending=False).limit(1).show()
# 4 Дата самой ранней и самой последней покупки на платформе
df_spark.agg({'InvoiceDate': 'min'}).show()
df_spark.agg({'InvoiceDate': 'max'}).show()

# ----------------------------------------------------------------------------------------------

# RFM-анализ клиентов платформы
df_spark.show(5)
df_spark.printSchema()

# Проверка нулевых наличия нулевых значений
def my_count(df_in):
    df_in.agg(*[count(c).alias(c) for c in df_in.columns]).show()
my_count(df_spark)

# Удалим записи с нулевыми значениями из набора данных
df_spark_clean = df_spark.dropna(how='any')
my_count(df_spark_clean)

# добавление столбца "recency"
recency = df_spark_clean.withColumn('recency', F.datediff(F.current_date(), df_spark_clean.InvoiceDate))
recency.show(5)

# расчет показателя "frequency", создание нового датасета.
frequency = df_spark_clean.groupBy('CustomerID', 'InvoiceNo').count().\
                           groupBy('CustomerID').agg(count("*").alias("frequency"))
frequency.show(5)

# расчет значения "TotalPrice", создание нового столбца с данными значениями
total_price_clean = df_spark_clean.withColumn('TotalPrice', round( df_spark.Quantity * df_spark.UnitPrice, 2 ) )

# расчет показателя "monetary", создание нового датасета
monetary = total_price_clean.groupBy("CustomerID").agg(round(sum('TotalPrice'),2).alias('monetary'))
monetary.show(5)

# добавление новых столбцов в датасет
total_price_new = recency.join(frequency, 'CustomerID', how = 'inner').join(monetary, 'CustomerID', how = 'inner')
total_price_new.show(5)

# В связи с тем, что все покупатели совершили покупку более года назад, разбиение клиентов по столбцу "recency" на значения:
# "последняя покупка в прошлом году", "последняя покупка на прошлой неделе", "последняя покупка в прошлом месяце"
# нецелесообразна, других критериев нет. Разделим на группы опираясь на описательную статистику.
# Описательная статистика по новым столбцам
total_price_new.select('recency','frequency','monetary').summary().show()

# Разделим клиентов на группы. А, В, С, где А - клиенты с наибольшим приоритетом, С - с наименьшим.
total_price_new = total_price_new.withColumn('recency_group', F.when(F.col("recency")>4409, "C")\
                                            .when(F.col('recency') > 4325, "B")\
                                            .otherwise("A"))
total_price_new = total_price_new.withColumn('frequency_group', F.when(F.col("frequency")< 8, "C")\
                                            .when(F.col('frequency') < 18, "B")\
                                            .otherwise("A"))
total_price_new = total_price_new.withColumn('monetary_group', F.when(F.col("monetary")< 2616, "C")\
                                            .when(F.col('monetary') < 6147, "B")\
                                            .otherwise("A"))
total_price_new.show(5)

# Проверим распределение по группам
total_price_new = total_price_new.withColumn('groups',
                                            F.concat(F.col('recency_group'),F.col('frequency_group'),F.col('monetary_group')))
total_price_new.select('CustomerID','recency','frequency','monetary','recency_group','frequency_group','monetary_group', 'groups').show(5)

# Определим id клиентов, у которых значение по всем трем показателям "recency","frequency","monetary" соответствуют группе А.
result = total_price_new.select(['CustomerID']).filter(total_price_new.groups == 'AAA').distinct()
result.show(5)

# Сохраним результат в отдельный csv файл
result.toPandas().to_csv('4_7_result.csv', index = False)