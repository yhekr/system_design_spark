import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def cdm_user(*args, **kwargs):
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
    DATE = F.to_date(F.lit(DATE_STR), "yyyy-MM-dd")

    DDS_USERS = '/opt/airflow/data/dds/users_hist/' + DATE_STR
    CDM_ORDERS = '/opt/airflow/data/cdm/dm_order/1d/' + DATE_STR
    CDM_USERS = '/opt/airflow/data/cdm/dm_user/1d/' + DATE_STR


    users_act = spark.read.parquet(DDS_USERS) \
        .groupBy("puid") \
        .agg(
            F.min_by("first_name", "valid_from").alias("initial_first_name"),
            F.min_by("last_name", "valid_from").alias("initial_last_name"),
            F.min_by("age", "valid_from").alias("initial_age"),

            F.max(  # можно брать любую запись
                F.when(
                    (F.col("valid_from") <= DATE) & (DATE <= F.col("valid_to")),
                    F.col("first_name")
                )
            ).alias("actual_first_name"),
            F.max(  # можно брать любую запись
                F.when(
                    (F.col("valid_from") <= DATE) & (DATE <= F.col("valid_to")),
                    F.col("last_name")
                )
            ).alias("actual_last_name"),
            F.max(  # можно брать любую запись
                F.when(
                    (F.col("valid_from") <= DATE) & (DATE <= F.col("valid_to")),
                    F.col("age")
                )
            ).alias("actual_age"),
        )

    orders = spark.read.parquet(CDM_ORDERS) \
        .groupBy("puid") \
        .agg(
            F.count_distinct("driver_id").alias("unique_drivers_cnt"),
            F.count("order_id").alias("orders_cnt"),
            F.min_by("order_id", "msk_created_dttm").alias("first_order_id"),

            F.max(~F.when(F.col("delivered_flg").isNotNull(), F.lit(True))).alias("newbie_flg"),
            F.lit(True).alias("has_orders_flg"),

            F.sum("cost_lcl").alias("gmv_spent_sum"),
            F.sum(F.when(F.col("cancelled_flg"), 0).otherwise(1)).alias("order_cancelled_cnt"),
            F.sum(F.when(F.col("delivered_flg"), 0).otherwise(1)).alias("order_delivered_cnt"),
        ) \
        .withColumn(
            "cancellation_pct",
            F.when(
                (F.col("order_delivered_cnt") != 0) & (F.col("order_cancelled_cnt") != 0),
                F.col("order_cancelled_cnt") / (F.col("order_cancelled_cnt") + F.col("order_delivered_cnt"))
            ).otherwise(None)
        )

    result =users_act \
        .join(orders, "puid", "left") \
    
    result.repartition(1) \
        .write.mode("overwrite").parquet(CDM_USERS)

    result.repartition(1) \
        .write.jdbc(url=url, table=f"cdm_dm_users_partition_{DATE_STR.replace('-', '_')}", mode="overwrite", properties=properties)

    spark.stop()
