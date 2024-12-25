from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def ods_currencies(*args, **kwargs):
    DATE_STR = kwargs['execution_date'][:19]
    RAW_PATH = '/opt/airflow/data/raw/currencies_dump.json'
    ODS_PATH = '/opt/airflow/data/ods/currencies/1d/' + DATE_STR

    spark = SparkSession.builder.master("local").appName("ETL_Pipeline") \
        .config("spark.jars", "/opt/airflow/plugins/postgresql-42.2.18.jar") \
        .getOrCreate()

    properties = {
        "user": "cape",
        "password": "wlevb14vu4rru3",
        "driver": "org.postgresql.Driver"
    }
    url = "jdbc:postgresql://cape-pg:5432/cape"


    currencies_dump = spark.read.json(RAW_PATH)

    currencies_dump \
        .select(
            "info.currency",
            "info.value",
        ) \
        .withColumn("msk_dt", F.to_date(F.lit(DATE_STR), "yyyy-MM-dd")) \
        .repartition(1) \
        .write.mode("overwrite").parquet(ODS_PATH)

    spark.stop()
