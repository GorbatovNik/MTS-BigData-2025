from onetl.connection import Hive, SparkHDFS
from onetl.db import DBWriter
from onetl.file import FileDFReader
from onetl.file.format import CSV
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.master("yarn")
    .appName("spark-with-yarn")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .config("spark.hive.metastore.uris", "thrift://$NN:9083")
    .enableHiveSupport().getOrCreate()
)

hdfs = SparkHDFS(host="$NN", port=9000, spark=spark, cluster="test")
hdfs.check()

data_name = "$DATA_NAME"
reader = FileDFReader(connection=hdfs, format=CSV(delimiter=",", header=True), source_path="/input")
df = reader.run([data_name])
print(f"Количество записей в исходной таблице: {df.count()}")

hive = Hive(spark=spark, cluster="x")
print(f"Проверка подключения к Hive: {hive.check()}")

writer = DBWriter(
    connection=hive,
    table=f"test.{data_name}"
)
writer.run(df)
print(f"Число партиций до репартиционирования: {df.rdd.getNumPartitions()}")

# Репартиционирование по стране правообладателя
df = df.repartition("right_holder_country_code")
writer = DBWriter(
    connection=hive,
    table=f"test.{data_name}_1",
    options=Hive.WriteOptions(partitionBy=["right_holder_country_code"])
)
writer.run(df)
print(f"Число партиций после репартиционирования: {df.rdd.getNumPartitions()}")

# --- Демонстрация манипуляций с данными ---

filtered_df = df.filter(F.col("right_holder_country_code").isNotNull() &
                        (F.col("right_holder_country_code") != ""))
print(f"Количество записей после фильтрации (непустой country_code): {filtered_df.count()}")

country_counts = filtered_df.groupBy("right_holder_country_code") \
    .agg(F.count("*").alias("record_count")) \
    .orderBy(F.col("record_count").desc())

print("Топ-5 стран по количеству записей:")
country_counts.show(5)

selected_columns = df.select(
    "registration_number",
    "registration_date",
    "right_holder_name",
    "right_holder_country_code",
).limit(10)

print("Выборка первых 10 записей с выбранными столбцами:")
selected_columns.show(10, truncate=False)
