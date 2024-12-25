import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def ods_drivers(*args, **kwargs):
    DATE_STR = kwargs['execution_date'][:19]
    RAW_PATH = '/opt/airflow/data/raw/drivers_dump.json'
    ODS_PATH = '/opt/airflow/data/ods/drivers/1d/' + DATE_STR
    
    spark = SparkSession.builder.master("local").appName("ETL_Pipeline") \
        .config("spark.jars", "/opt/airflow/plugins/postgresql-42.2.18.jar") \
        .getOrCreate()

    properties = {
        "user": "cape",
        "password": "wlevb14vu4rru3",
        "driver": "org.postgresql.Driver"
    }
    url = "jdbc:postgresql://cape-pg:5432/cape"


    drivers_dump = spark.read.json(RAW_PATH)

    result = drivers_dump \
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
    
    result.repartition(1) \
        .write.mode("overwrite").parquet(ODS_PATH)

    result \
        .repartition(1) \
        .write.jdbc(url=url, table=f"ods_drivers_partition_{DATE_STR.replace('-', '_')}", mode="overwrite", properties=properties)


    spark.stop()
