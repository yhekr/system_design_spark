import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

DATE = os.getenv("DATE")
if DATE is None:
    raise Exception("data ne vistavlena")
RAW_PATH = 'system_design_spark/raw/event_log.json'
ODS_PATH = 'system_design_spark/ods/events/1d/' + DATE

# Создание SparkSession
spark = SparkSession.builder.master("local").appName("ETL_Pipeline") \
    .getOrCreate()


drivers_dump = spark.read.json(RAW_PATH)

drivers_dump \
    .select(
        "id",
        "info.first_name",
        "info.last_name",
        "info.age",
    ) \
    .withColumnRenamed("id", "driver_id") \
    .withColumn("msk_dt", F.to_date(F.lit(DATE), "yyyy-MM-dd")) \
    .repartition(1) \
    .write.mode("overwrite").parquet(ODS_PATH)

spark.stop()

