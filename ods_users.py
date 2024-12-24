import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


DATE = os.getenv("DATE")
RAW_PATH = 'data/raw/users_dump.json'
ODS_PATH = 'data/ods/users/1d/' + DATE


spark = SparkSession.builder.master("local").appName("ETL_Pipeline") \
    .getOrCreate()


users_dump = spark.read.json(RAW_PATH)

users_dump \
    .select(
        "id",
        "info.first_name",
        "info.last_name",
        "info.age",
    ) \
    .withColumnRenamed("id", "puid") \
    .withColumn("msk_dt", F.to_date(F.lit(DATE), "yyyy-MM-dd")) \
    .repartition(1) \
    .write.mode("overwrite").parquet(ODS_PATH)

spark.stop()
