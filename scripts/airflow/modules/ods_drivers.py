import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def ods_drivers(*args, **kwargs):
    DATE_STR = kwargs['execution_dttm'][:19]
    RAW_PATH = '/opt/airflow/data/raw/drivers_dump.json'
    ODS_PATH = '/opt/airflow/data/ods/drivers/5m/' + DATE_STR


    spark = SparkSession.builder.master("local").appName("ETL_Pipeline") \
        .getOrCreate()


    drivers_dump = spark.read.json(RAW_PATH)

    drivers_dump \
        .select(
            "id",
            "info.first_name",
            "info.last_name",
            "info.age",
            "info.license_id",
        ) \
        .withColumnRenamed("id", "driver_id") \
        .withColumnRenamed("license_id", "licence_id") \
        .withColumn("msk_dt", F.to_date(F.lit(DATE_STR), "yyyy-MM-dd")) \
        .repartition(1) \
        .write.mode("overwrite").parquet(ODS_PATH)

    spark.stop()
