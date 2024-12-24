from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Создание SparkSession
spark = SparkSession.builder.master("local").appName("ETL_Pipeline") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2") \
    .getOrCreate()

# Схемы таблиц
# RAW Layer
# Logs Table Schema
raw_logs_schema = """
    event_type STRING,
    msk_event_dttm TIMESTAMP,
    puid STRING,
    device_id STRING,
    driver_id STRING,
    car_registration_number STRING,
    cost_lcl DOUBLE,
    currency STRING,
    city_id STRING,
    city_name STRING,
    country_id STRING,
    country_name STRING,
    ab_flags STRING
"""

# Devices Table Schema
raw_devices_schema = """
    device_id STRING,
    platform_name STRING,
    model STRING
"""

# Drivers Table Schema
raw_drivers_schema = """
    driver_id STRING,
    driver_name STRING,
    car_model STRING,
    car_submodel STRING,
    car_registration_number STRING
"""

# Users Table Schema
raw_users_schema = """
    puid STRING
"""

# Currency Table Schema
raw_currency_schema = """
    currency STRING,
    rate DOUBLE,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP
"""

# ODS Layer Schema
ods_logs_schema = """
    event_type STRING,
    msk_event_dttm TIMESTAMP,
    puid STRING,
    device_id STRING,
    driver_id STRING,
    car_registration_number STRING,
    cost_lcl DOUBLE,
    currency STRING,
    city_id STRING,
    city_name STRING,
    country_id STRING,
    country_name STRING,
    ab_flags STRING
"""

# DDS Layer Schemas
# Currency Dimension Schema
dds_dim_currency_schema = """
    currency STRING,
    rate DOUBLE,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP
"""

# Devices Dimension Schema
dds_dim_devices_schema = """
    device_id STRING,
    platform_name STRING,
    model STRING
"""

# Drivers Dimension Schema
dds_dim_drivers_schema = """
    driver_id STRING,
    driver_name STRING,
    car_model STRING,
    car_submodel STRING,
    car_registration_number STRING
"""

# Users Dimension Schema
dds_dim_users_schema = """
    puid STRING
"""

# Events Fact Table Schema
dds_fct_events_schema = """
    event_type STRING,
    msk_event_dttm TIMESTAMP,
    puid STRING,
    device_id STRING,
    driver_id STRING,
    car_registration_number STRING,
    cost_lcl DOUBLE,
    cost_rub DOUBLE,
    currency STRING,
    city_id STRING,
    city_name STRING,
    country_id STRING,
    country_name STRING,
    ab_flags STRING
"""

# CDM Layer Schemas
# Assign Orders Table Schema
assign_orders_schema = """
    assign_order_id STRING,
    order_id STRING,
    executer_id STRING,
    base_coin_amount FLOAT,
    coin_coef FLOAT,
    bonus_amount FLOAT,
    final_coin_amount FLOAT,
    route_information STRING,
    assign_time TIMESTAMP,
    acquire_time TIMESTAMP,
    is_canceled BOOLEAN
"""

# raw_logs = spark.read.json("raw_logs.json")
# raw_logs.show()


# RAW Layer
# Загружаем данные из захардкоженных JSON файлов
raw_logs = spark.read.schema(raw_logs_schema).json("raw_logs.json")
raw_devices = spark.read.schema(raw_devices_schema).json("raw_devices.json")
raw_drivers = spark.read.schema(raw_drivers_schema).json("raw_drivers.json")
raw_users = spark.read.schema(raw_users_schema).json("raw_users.json")

raw_logs.show()
raw_devices.show()
raw_drivers.show()
raw_users.show()


# ODS Layer (парсинг логов и базовая фильтрация)
ods_logs = raw_logs.select(
    F.col("event_type"),
    F.col("msk_event_dttm"),
    F.col("puid"),
    F.col("device_id"),
    F.col("driver_id"),
    F.col("car_registration_number"),
    F.col("cost_lcl"),
    F.col("currency"),
    F.col("city_id"),
    F.col("city_name"),
    F.col("country_id"),
    F.col("country_name"),
    F.col("ab_flags")
)
ods_logs.write.csv("ods_logs", header=True, mode="overwrite")

# DDS Layer
# Dimension Tables

dim_devices = raw_devices.select(
    F.col("device_id"),
    F.col("platform_name"),
    F.col("model")
)
dim_devices.write.csv("dds_dim_devices", header=True, mode="overwrite")

dim_drivers = raw_drivers.select(
    F.col("driver_id"),
    F.col("driver_name"),
    F.col("car_model"),
    F.col("car_submodel"),
    F.col("car_registration_number")
)
dim_drivers.write.csv("dds_dim_drivers", header=True, mode="overwrite")

dim_users = raw_users.select(
    F.col("puid")
)
dim_users.write.csv("ds_dim_users", header=True, mode="overwrite")

# Fact Table
dds_events = ods_logs \
    .join(dim_devices.withColumnRenamed("device_id", "dim_device_id"), ods_logs.device_id == F.col("dim_device_id"), "left") \
    .join(dim_drivers.withColumnRenamed("driver_id", "dim_driver_id"), ods_logs.driver_id == F.col("dim_driver_id"), "left") \
    .withColumn("cost_rub", F.col("cost_lcl") * 75) \
    .drop("dim_device_id", "dim_driver_id")

# вот тут у меня не работает
dds_events.write.csv("dds_fct_events", header=True, mode="overwrite")

# Assign Orders Table Example
dds_assign_orders = dds_events.select(
    F.monotonically_increasing_id().alias("assign_order_id"),
    F.col("event_type").alias("order_id"),
    F.col("driver_id").alias("executer_id"),
    F.col("cost_lcl").alias("base_coin_amount"),
    F.lit(1.0).alias("coin_coef"),
    F.lit(50.0).alias("bonus_amount"),
    (F.col("cost_lcl") * F.lit(1.0) + F.lit(50.0)).alias("final_coin_amount"),
    F.lit("Route information here").alias("route_information"),
    F.col("msk_event_dttm").alias("assign_time"),
    F.col("msk_event_dttm").alias("acquire_time"),
    F.lit(False).alias("is_canceled")
)
dds_assign_orders.write.csv("dds_assign_orders", header=True, mode="overwrite")
