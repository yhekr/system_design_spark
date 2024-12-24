import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


DATE = os.getenv("DATE")
RAW_PATH = 'data/raw/event_log.json'
ODS_PATH = 'data/ods/events/1d/' + DATE


spark = SparkSession.builder.master("local").appName("ETL_Pipeline") \
    .getOrCreate()


event_log = spark.read.json(RAW_PATH)

event_log \
    .select(
        "event.user_id",
        "event.event_type",
        "event.device_id",
        "event.driver_id",
        "event.event_ts",
        "event.order.cost",
        "event.order.currency",
        "event.order.cancel.reason",
        "event.order.cancel.is_driver_cancellation",
        "event.order.id",
        "event.order.region_id"
    ) \
    .withColumnRenamed("user_id", "puid") \
    .withColumnRenamed("reason", "cancel_reason") \
    .withColumnRenamed("is_driver_cancellation", "driver_cancel_flg") \
    .withColumnRenamed("id", "order_id") \
    .repartition(1) \
    .write.mode("overwrite").parquet(ODS_PATH)

spark.stop()
