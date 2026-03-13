# Databricks Notebook — kafka_spark_streaming
# Project 3: Real-Time E-Commerce Order Streaming Pipeline

# Cell 1 — Verify Spark
# print(spark.version)

# Cell 2 — Install libraries
# %pip install kafka-python confluent-kafka

# Cell 3 — Restart Python
# dbutils.library.restartPython()

# Cell 4 — Schema + Kafka Stream
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

order_schema = StructType([
    StructField("order_id",       StringType(),  True),
    StructField("customer_id",    StringType(),  True),
    StructField("product",        StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("quantity",       IntegerType(), True),
    StructField("unit_price",     DoubleType(),  True),
    StructField("discount_pct",   DoubleType(),  True),
    StructField("total_amount",   DoubleType(),  True),
    StructField("payment_method", StringType(),  True),
    StructField("city",           StringType(),  True),
    StructField("state",          StringType(),  True),
    StructField("city_tier",      IntegerType(), True),
    StructField("status",         StringType(),  True),
    StructField("timestamp",      StringType(),  True),
])

jaas = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="TPNOC3H64TWZ3Y6S" password="cfltoD6ErLeBjM5PljKh1ejlUq4/18ftM0OZGTiKjZdC38SSZaugK7U2qKIyqosg";'

raw_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-921jm.us-east-2.aws.confluent.cloud:9092") \
    .option("subscribe", "ecommerce-orders") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", jaas) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_stream = raw_stream.select(
    col("value").cast("string").alias("raw_json")
).select(
    from_json(col("raw_json"), order_schema).alias("data")
).select("data.*")

# Cell 5 — Data Quality Transformations
from pyspark.sql.functions import when, lit, to_timestamp, current_timestamp

def apply_data_quality(df):
    return df \
        .filter(col("order_id").isNotNull()) \
        .filter(col("quantity") > 0) \
        .filter(col("unit_price").isNotNull()) \
        .filter(col("total_amount").isNotNull()) \
        .withColumn("customer_id",
            when(col("customer_id").isNull(), lit("GUEST-UNKNOWN"))
            .otherwise(col("customer_id"))) \
        .withColumn("payment_method",
            when(col("payment_method").isNull(), lit("Unknown"))
            .otherwise(col("payment_method"))) \
        .withColumn("unit_price",
            when(col("unit_price") < 0, col("unit_price") * -1)
            .otherwise(col("unit_price"))) \
        .withColumn("total_amount",
            when(col("total_amount") < 0, col("total_amount") * -1)
            .otherwise(col("total_amount"))) \
        .withColumn("order_type",
            when(col("total_amount") < 0, lit("refund"))
            .otherwise(lit("purchase"))) \
        .withColumn("timestamp",
            when(
                to_timestamp(col("timestamp")) > current_timestamp(),
                current_timestamp().cast("string")
            ).otherwise(col("timestamp")))

clean_stream = apply_data_quality(parsed_stream)

# Cell 6 — Write to Delta Lake
clean_query = clean_stream \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "/Volumes/main/ecommerce/streaming_data/checkpoints_clean") \
    .start("/Volumes/main/ecommerce/streaming_data/delta_orders_clean")

clean_query.awaitTermination()

# Cell 7 — Load to Snowflake
import snowflake.connector

clean_df = spark.read.format("delta").load("/Volumes/main/ecommerce/streaming_data/delta_orders_clean")
pandas_df = clean_df.toPandas()
pandas_df = pandas_df.where(pandas_df.notna(), None)

conn = snowflake.connector.connect(
    account='eulwggz-fvb65417',
    user='KARTIK9495',
    password='YOUR_PASSWORD',  # use environment variable in production
    warehouse='NYC_TAXI_WH',
    database='ECOMMERCE_DB',
    schema='ECOMMERCE_SCHEMA'
)

cursor = conn.cursor()
cursor.execute("TRUNCATE TABLE ORDERS_CLEAN")
rows = [tuple(row) for _, row in pandas_df.iterrows()]
cursor.executemany("""
    INSERT INTO ORDERS_CLEAN VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
""", rows)
conn.commit()
cursor.close()
conn.close()
print(f"✅ {len(rows)} records loaded into Snowflake!")
