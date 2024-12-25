import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def ods_event_log(*args, **kwargs):
    DATE_STR = kwargs['execution_date']
    RAW_PATH = '/opt/airflow/data/raw/event_log.json'
    ODS_PATH = '/opt/airflow/data/ods/events/1d/' + DATE_STR


    spark = SparkSession.builder.master("local").appName("ods_event_log") \
        .config("spark.jars", "/opt/airflow/plugins/postgresql-42.2.18.jar") \
        .getOrCreate()
    
    properties = {
        "user": "cape",
        "password": "wlevb14vu4rru3",
        "driver": "org.postgresql.Driver"
    }
    url = "jdbc:postgresql://cape-pg:5432/cape"


    event_log = spark.read.json(RAW_PATH)

    result = event_log \
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
    
    result.repartition(1) \
        .write.mode("overwrite").parquet(ODS_PATH)

    result \
        .repartition(1) \
        .write.jdbc(url=url, table=f"ods_events_partition_{DATE_STR.replace('-', '_')}", mode="overwrite", properties=properties)

    spark.stop()
