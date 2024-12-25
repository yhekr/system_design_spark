import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def dds_devices(*args, **kwargs):
    SCHEMA = """
        valid_to DATE,
        valid_from DATE,
        device_id STRING,
        platform STRING,
        version LONG,
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

    DATE_STR = kwargs['execution_date'][:19]
    current_date = datetime.datetime.strptime(DATE_STR, '%Y-%m-%d')
    prev_date = current_date - datetime.timedelta(days=1)
    PREV_DATE_STR = prev_date.strftime('%Y-%m-%d')

    DATE = F.to_date(F.lit(DATE_STR), 'yyyy-MM-dd HH:mm:ss')
    PREV_DATE = F.date_sub(DATE, 1)
    LAST_DATE = F.to_date(F.lit("9999-12-31"), 'yyyy-MM-dd HH:mm:ss')

    ODS_PATH = '/opt/airflow/data/ods/devices/5m/' + DATE_STR
    DDS_PREV_PATH = '/opt/airflow/data/dds/devices_hist/' + PREV_DATE_STR
    DDS_PATH = '/opt/airflow/data/dds/devices_hist/' + DATE_STR

    try:
        hist = spark.read.schema(SCHEMA).parquet(DDS_PREV_PATH)
    except Exception:
        hist = spark.createDataFrame([], SCHEMA)

    new_data = spark.read.parquet(ODS_PATH)

    updated_hist = hist.alias("h") \
        .join(new_data.alias("n"), "device_id", "left") \
        .withColumn(
            "is_changed",
            (F.col("n.platform") != F.col("h.platform")) |
            (F.col("n.version") != F.col("h.version"))
        ) \
        .withColumn("new_valid_to", F.when(F.col("is_changed") & (F.col("valid_to") == LAST_DATE), PREV_DATE).otherwise(F.col("valid_to"))) \

    changed = updated_hist.select(
            "valid_from", "new_valid_to",
            "device_id",
            "h.platform", "h.version",
        ) \
        .withColumnRenamed("new_valid_to", "valid_to")

    new_rows = new_data.alias("n") \
        .join(
            updated_hist.alias('h').filter(~F.col("is_changed")), "device_id", "left_anti"
        ) \
        .withColumn("valid_from", DATE) \
        .withColumn("valid_to", LAST_DATE) \
        .select(
            "valid_from", "valid_to",
            "device_id",
            "n.platform", "n.version",
        )

    result = changed \
        .union(new_rows) \
        .sort(["valid_to", "valid_from", "device_id"]) \
        .repartition(1) \
        .write.mode("overwrite").parquet(DDS_PATH)

    spark.stop()
