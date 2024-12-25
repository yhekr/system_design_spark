import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def dds_events(*args, **kwargs):
    spark = SparkSession.builder.master("local").appName("ETL_Pipeline") \
        .config("spark.jars", "/opt/airflow/plugins/postgresql-42.2.18.jar") \
        .getOrCreate()

    properties = {
        "user": "cape",
        "password": "wlevb14vu4rru3",
        "driver": "org.postgresql.Driver"
    }
    url = "jdbc:postgresql://cape-pg:5432/cape"

    DATE_STR = kwargs['execution_date'][:19]

    ODS_PATH = '/opt/airflow/data/ods/events/1d/' + DATE_STR
    DDS_PATH = '/opt/airflow/data/dds/fct_orders_act/1d/' + DATE_STR


    events = spark.read.parquet(ODS_PATH)

    result = events \
        .groupBy("order_id") \
        .agg(
            F.max("puid").alias("puid"),
            F.max("driver_id").alias("driver_id"),
            F.max("device_id").alias("device_id"),
            F.max("region_id").alias("region_id"),

            F.max(F.when(F.col("event_type") == "order_created", F.col("cost")), ).alias("cost_lcl"),
            F.max(F.when(F.col("event_type") == "order_created", F.col("currency"))).alias("currency"),

            F.max(F.when(F.col("event_type") == "order_cancelled", F.col("cancel_reason"))).alias("cancel_reason"),
            F.max(F.when(F.col("event_type") == "order_cancelled", F.col("driver_cancel_flg"))).alias("driver_cancel_flg"),

            F.max(F.when(F.col("event_type") == "order_created",   F.from_unixtime(F.col("event_ts")))).alias("msk_created_dttm"),
            F.max(F.when(F.col("event_type") == "order_assigned",  F.from_unixtime(F.col("event_ts")))).alias("msk_assigned_dttm"),
            F.max(F.when(F.col("event_type") == "order_started",   F.from_unixtime(F.col("event_ts")))).alias("msk_started_dttm"),
            F.max(F.when(F.col("event_type") == "order_delivered", F.from_unixtime(F.col("event_ts")))).alias("msk_delivered_dttm"),
            F.max(F.when(F.col("event_type") == "order_cancelled", F.from_unixtime(F.col("event_ts")))).alias("msk_cancelled_dttm"),
        ) \
        .withColumn("delivered_flg", F.col("msk_delivered_dttm").isNotNull()) \
        .withColumn("started_flg", F.col("msk_started_dttm").isNotNull()) \
        .withColumn("assigned_flg", F.col("msk_assigned_dttm").isNotNull()) \
        .withColumn("cancelled_flg", F.col("msk_cancelled_dttm").isNotNull())

    result \
        .repartition(1) \
        .write.mode("overwrite").parquet(DDS_PATH)

    result \
        .repartition(1) \
        .write.jdbc(url=url, table=f"dds_orders_partition_{DATE_STR.replace('-', '_')}", mode="overwrite", properties=properties)


    spark.stop()
