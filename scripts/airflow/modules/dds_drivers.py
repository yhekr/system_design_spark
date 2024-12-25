import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def dds_drivers(*args, **kwargs):
    SCHEMA = """
        valid_to DATE,
        valid_from DATE,
        driver_id STRING,
        first_name STRING,
        last_name STRING,
        age LONG,
        licence_id STRING
    """

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
    current_date = datetime.datetime.strptime(DATE_STR, '%Y-%m-%d')
    prev_date = current_date - datetime.timedelta(days=1)
    PREV_DATE_STR = prev_date.strftime('%Y-%m-%d')

    DATE = F.to_date(F.lit(DATE_STR), "yyyy-MM-dd")
    # PREV_DATE = F.date_sub(DATE, 1)
    PREV_DATE = DATE - F.expr("INTERVAL 1 DAY")
    LAST_DATE = F.to_date(F.lit("9999-12-31"), "yyyy-MM-dd")

    ODS_PATH = '/opt/airflow/data/ods/drivers/1d/' + DATE_STR
    DDS_PREV_PATH = '/opt/airflow/data/dds/drivers_hist/' + PREV_DATE_STR
    DDS_PATH = '/opt/airflow/data/dds/drivers_hist/' + DATE_STR

    try:
        hist = spark.read.schema(SCHEMA).parquet(DDS_PREV_PATH)
    except Exception:
        hist = spark.createDataFrame([], SCHEMA)
    new_data = spark.read.parquet(ODS_PATH)

    updated_hist = hist.alias("h") \
        .join(new_data.alias("n"), "driver_id", "left") \
        .withColumn(
            "is_changed",
            (F.col("n.age") != F.col("h.age")) |
            (F.col("n.first_name") != F.col("h.first_name")) |
            (F.col("n.last_name") != F.col("h.last_name")) |
            (F.col("n.licence_id") != F.col("h.licence_id"))
        ) \
        .withColumn("new_valid_to", F.when(F.col("is_changed") & (F.col("valid_to") == LAST_DATE), PREV_DATE).otherwise(F.col("valid_to"))) \

    changed = updated_hist.select(
            "valid_from", "new_valid_to",
            "driver_id",
            "h.age", "h.first_name", "h.last_name", "h.licence_id",
        ) \
        .withColumnRenamed("new_valid_to", "valid_to")

    new_rows = new_data.alias("n") \
        .join(
            updated_hist.alias('h').filter(~F.col("is_changed")), "driver_id", "left_anti"
        ) \
        .withColumn("valid_from", DATE) \
        .withColumn("valid_to", LAST_DATE) \
        .select(
            "valid_from", "valid_to",
            "driver_id",
            "n.age", "n.first_name", "n.last_name", "n.licence_id",
        )

    result = changed \
        .union(new_rows) \
        .sort(["valid_to", "valid_from", "driver_id"]) \
    
    result.repartition(1) \
        .write.mode("overwrite").parquet(DDS_PATH)

    result \
        .repartition(1) \
        .write.jdbc(url=url, table=f"dds_drivers_partition_{DATE_STR.replace('-', '_')}", mode="overwrite", properties=properties)


    spark.stop()
