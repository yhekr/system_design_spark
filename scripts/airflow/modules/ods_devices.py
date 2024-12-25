from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def ods_drivers(*args, **kwargs):
    DATE_STR = kwargs['execution_dttm'][:19]
    RAW_PATH = '/opt/airflow/data/raw/devices_dump.json'
    ODS_PATH = '/opt/airflow/data/ods/devices/5m/' + DATE_STR


    spark = SparkSession.builder.master("local").appName("ETL_Pipeline") \
        .getOrCreate()


    devices_dump = spark.read.json(RAW_PATH)

    devices_dump \
        .select(
            "info.device_id",
            "info.platform",
            "info.version",
        ) \
        .withColumn("msk_dt", F.to_date(F.lit(DATE_STR), "yyyy-MM-dd")) \
        .repartition(1) \
        .write.mode("overwrite").parquet(ODS_PATH)

    spark.stop()
