import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

DATE = os.getenv("DATE")
ODS_PATH = 'data/ods/drivers/1d/' + DATE
DDS_PATH = 'data/dds/drivers_hist'

SCHEMA = """
    valid_to DATE,
    valid_from DATE,
    driver_id INTEGER,
    first_name STRING,
    last_name STRING,
    age INTEGER,
    licence_id STRING
"""

# Создание SparkSession
spark = SparkSession.builder.master("local").appName("ETL_Pipeline") \
    .getOrCreate()


hist = spark.read.schema(SCHEMA).parquet(DDS_PATH)
new_data = spark.read.parquet(ODS_PATH)

updates_df = hist.alias("h") \
    .join(new_data.alias("n"), "driver_id", "left") \
    .select("h.*", "n.age", "n.first_name", "n.last_name", "n.licence_id") \
    .withColumn(
        "is_changed",
        (F.col("n.age") != F.col("h.age")) |
        (F.col("n.first_name") != F.col("h.first_name")) |
        (F.col("n.last_name") != F.col("h.last_name")) |
        (F.col("n.licence_id") != F.col("h.licence_id"))
    ) \
    .withColumn("valid_to", F.when(F.col("is_changed") & (F.col("valid_to") == "9999-12-31"), F.lit(DATE)).otherwise(F.col("valid_to"))) \
    .select("valid_to", "valid_from", "driver_id", "h.first_name", "h.last_name", "h.age", "h.licence_id")

new_scd_df = updates_df.filter(updates_df.valid_to == DATE).drop("valid_to") \
    .withColumn("valid_to", F.lit("9999-12-31")) \
    .withColumn("valid_from", F.lit(DATE)) \
    .union(
        updates_df.filter(updates_df.valid_to != DATE).select("driver_id", "first_name", "last_name", "age", "licence_id", "valid_from", "valid_to")
    ) \
    .union(
        new_data.join(hist, "driver_id", "left_anti")
           .withColumn("valid_from", F.to_date(F.lit(DATE), 'yyyy-MM-dd'))
           .withColumn("valid_to", F.to_date(F.lit("9999-12-31"), 'yyyy-MM-dd'))
           .drop("msk_dt")
    )

import time; time.sleep(10)
new_scd_df.show()

spark.stop()