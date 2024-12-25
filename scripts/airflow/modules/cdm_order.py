import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def cdm_order(*args, **kwargs):
    # Создание SparkSession
    spark = SparkSession.builder.master("local").appName("ETL_Pipeline") \
        .config("spark.jars", "/opt/airflow/plugins/postgresql-42.2.18.jar") \
        .getOrCreate()

    properties = {
        "user": "cape",
        "password": "wlevb14vu4rru3",
        "driver": "org.postgresql.Driver"
    }
    url = "jdbc:postgresql://cape-pg:5432/cape"

    DATE_STR = kwargs['execution_date']
    DATE = F.to_date(F.lit(DATE_STR), "yyyy-MM-dd")

    DDS_ORDERS = '/opt/airflow/data/dds/fct_orders_act/1d/'
    DDS_DRIVERS = '/opt/airflow/data/dds/drivers_hist/' + DATE_STR
    CDM_PATH = '/opt/airflow/data/cdm/dm_order/1d/' + DATE_STR


    orders = spark.read.option("recursiveFileLookup", "true").parquet(DDS_ORDERS)
    drivers_act = spark.read.parquet(DDS_DRIVERS)

    result = orders.alias('o') \
        .withColumn("previous_order_created_time", F.lag("msk_created_dttm").over(Window.partitionBy("puid").orderBy("msk_created_dttm"))) \
        .join(drivers_act.alias('d').select("driver_id", "licence_id", "valid_from", "valid_to"), "driver_id", "left") \
        .filter(
            (F.col("msk_created_dttm") >= F.col("valid_from")) & (F.col("msk_created_dttm") <= F.col("valid_to"))
        ) \
        .drop("valid_from", "valid_to")

    result \
        .repartition(1) \
        .write.mode("overwrite").parquet(CDM_PATH)

    result \
        .repartition(1) \
        .write.jdbc(url=url, table=f"cdm.dm_order.partition_{DATE_STR}", mode="overwrite", properties=properties)

    spark.stop()
