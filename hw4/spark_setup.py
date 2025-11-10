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
print(df.count())

hive = Hive(spark=spark, cluster="x")
print(hive.check())

writer = DBWriter(
    connection=hive,
    table=f"test.{data_name}"
)
writer.run(df)
print(df.rdd.getNumPartitions())

df = df.repartition("right_holder_country_code")
writer = DBWriter(
    connection=hive,
    table=f"test.{data_name}_1"
)
writer.run(df)
print(df.rdd.getNumPartitions())

