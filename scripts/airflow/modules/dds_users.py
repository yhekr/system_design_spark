import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def dds_users(*args, **kwargs):
    SCHEMA = """
        valid_to DATE,
        valid_from DATE,
        puid STRING,
        first_name STRING,
        last_name STRING,
        age LONG
    """


    spark = SparkSession.builder.master("local").appName("ETL_Pipeline") \
        .getOrCreate()


    DATE_STR = kwargs['execution_date'][:19]
    current_date = datetime.datetime.strptime(DATE_STR, '%Y-%m-%d')
    prev_date = current_date - datetime.timedelta(days=1)
    PREV_DATE_STR = prev_date.strftime('%Y-%m-%d')

    DATE = F.to_date(F.lit(DATE_STR), "yyyy-MM-dd")
    # PREV_DATE = F.date_sub(DATE, 1)
    PREV_DATE = DATE - F.expr("INTERVAL 1 DAY")
    LAST_DATE = F.to_date(F.lit("9999-12-31"), "yyyy-MM-dd")

    ODS_PATH = '/opt/airflow/data/ods/users/1d/' + DATE_STR
    DDS_PREV_PATH = '/opt/airflow/data/dds/users_hist/' + PREV_DATE_STR
    DDS_PATH = '/opt/airflow/data/dds/users_hist/' + DATE_STR


    hist = spark.read.schema(SCHEMA).parquet(DDS_PREV_PATH)
    new_data = spark.read.parquet(ODS_PATH)

    updated_hist = hist.alias("h") \
        .join(new_data.alias("n"), "puid", "left") \
        .withColumn(
            "is_changed",
            (F.col("n.age") != F.col("h.age")) |
            (F.col("n.first_name") != F.col("h.first_name")) |
            (F.col("n.last_name") != F.col("h.last_name"))
        ) \
        .withColumn("new_valid_to", F.when(F.col("is_changed") & (F.col("valid_to") == LAST_DATE), PREV_DATE).otherwise(F.col("valid_to"))) \

    changed = updated_hist.select(
            "valid_from", "new_valid_to",
            "puid",
            "h.age", "h.first_name", "h.last_name",
        ) \
        .withColumnRenamed("new_valid_to", "valid_to")

    new_rows = new_data.alias("n") \
        .join(
            updated_hist.alias('h').filter(~F.col("is_changed")), "puid", "left_anti"
        ) \
        .withColumn("valid_from", DATE) \
        .withColumn("valid_to", LAST_DATE) \
        .select(
            "valid_from", "valid_to",
            "puid",
            "n.age", "n.first_name", "n.last_name",
        )

    result = changed \
        .union(new_rows) \
        .sort(["valid_to", "valid_from", "puid"]) \
        .repartition(1) \
        .write.mode("overwrite").parquet(DDS_PATH)

    spark.stop()
