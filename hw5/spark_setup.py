from onetl.connection import Hive, SparkHDFS
from onetl.db import DBWriter
from onetl.file import FileDFReader
from onetl.file.format import CSV
from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

NN = "$NN"
DATA_NAME = "$DATA_NAME"


@task
def init_spark():
    spark = (
        SparkSession.builder.master("yarn")
        .appName("spark-with-yarn")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("spark.hive.metastore.uris", f"thrift://{NN}:9083")
        .enableHiveSupport().getOrCreate()
    )
    return spark


@task
def stop_spark(spark):
    spark.stop()


@task
def extract(spark):
    hdfs = SparkHDFS(host=NN, port=9000, spark=spark, cluster="test")
    hdfs.check()
    reader = FileDFReader(connection=hdfs, format=CSV(delimiter=",", header=True), source_path="/input")
    df = reader.run([DATA_NAME])

    print(f"Количество записей в исходной таблице: {df.count()}")
    return df


@task
def transform(df):
    df = df.repartition("right_holder_country_code")

    filtered_df = df.filter(F.col("right_holder_country_code").isNotNull() &
                            (F.col("right_holder_country_code") != ""))

    country_counts = filtered_df.groupBy("right_holder_country_code") \
        .agg(F.count("*").alias("record_count")) \
        .orderBy(F.col("record_count").desc())

    print("Топ-5 стран по количеству записей:")
    country_counts.show(5)

    return df


@task
def load(spark, df):
    hive = Hive(spark=spark, cluster="x")
    writer = DBWriter(
        connection=hive,
        table=f"test.{DATA_NAME}_2",
        options=Hive.WriteOptions(partitionBy=["right_holder_country_code"])
    )
    writer.run(df)
    print(f"Число партиций после репартиционирования: {df.rdd.getNumPartitions()}")


@flow
def process_data():
    spark = init_spark()

    df = extract(spark)
    df_transformed = transform(df)

    load(spark, df_transformed)

    stop_spark(spark)


if __name__ == "__main__":
    process_data()
