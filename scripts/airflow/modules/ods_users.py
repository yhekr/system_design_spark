import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def ods_users(*args, **kwargs):
    DATE_STR = kwargs['execution_date'][:19]
    RAW_PATH = '/opt/airflow/data/raw/users_dump.json'
    ODS_PATH = '/opt/airflow/data/ods/users/1d/' + DATE_STR

    spark = SparkSession.builder.master("local").appName("ETL_Pipeline") \
        .config("spark.jars", "/opt/airflow/plugins/postgresql-42.2.18.jar") \
        .getOrCreate()

    properties = {
        "user": "cape",
        "password": "wlevb14vu4rru3",
        "driver": "org.postgresql.Driver"
    }
    url = "jdbc:postgresql://cape-pg:5432/cape"

    users_dump = spark.read.json(RAW_PATH)

    result = users_dump \
        .select(
            "id",
            "info.first_name",
            "info.last_name",
            "info.age",
        ) \
        .withColumnRenamed("id", "puid") \
        .withColumn("msk_dt", F.to_timestamp(F.lit(DATE_STR), "yyyy-MM-dd")) \
    
    result.repartition(1) \
        .write.mode("overwrite").parquet(ODS_PATH)
    
    result \
        .repartition(1) \
        .write.jdbc(url=url, table=f"ods_users_partition_{DATE_STR.replace('-', '_')}", mode="overwrite", properties=properties)

    spark.stop()
