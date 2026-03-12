# Databricks notebook source
print(spark.version)

# COMMAND ----------

# MAGIC %pip install kafka-python confluent-kafka

# COMMAND ----------

dbutils.library.restartPython()


# COMMAND ----------

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

print("✅ Stream defined successfully!")
parsed_stream.printSchema()

# COMMAND ----------

# Create Unity Catalog volume for our project
spark.sql("CREATE CATALOG IF NOT EXISTS main")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.ecommerce")

spark.sql("CREATE VOLUME IF NOT EXISTS main.ecommerce.streaming_data")

print("✅ Volume created!")

# COMMAND ----------

query = parsed_stream \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "/Volumes/main/ecommerce/streaming_data/checkpoints") \
    .start("/Volumes/main/ecommerce/streaming_data/delta_orders")

print("✅ Streaming started!")
print(query.status)

# COMMAND ----------

df = spark.read.format("delta").load("/Volumes/main/ecommerce/streaming_data/delta_orders")
print(f"✅ Total records in Delta Lake: {df.count()}")
df.show(5, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col, when, lit, to_timestamp, current_timestamp

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
print("✅ Data quality transformations applied!")
clean_stream.printSchema()

# COMMAND ----------

clean_query = clean_stream \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "/Volumes/main/ecommerce/streaming_data/checkpoints_clean") \
    .start("/Volumes/main/ecommerce/streaming_data/delta_orders_clean")

clean_query.awaitTermination()
print("✅ Clean data written to Delta Lake!")

# COMMAND ----------

clean_df = spark.read.format("delta").load("/Volumes/main/ecommerce/streaming_data/delta_orders_clean")

print(f"✅ Total clean records: {clean_df.count()}")
print(f"\n--- Data Quality Check ---")
print(f"Null customer_id: {clean_df.filter(col('customer_id').isNull()).count()}")
print(f"Null payment_method: {clean_df.filter(col('payment_method').isNull()).count()}")
print(f"Negative prices: {clean_df.filter(col('unit_price') < 0).count()}")
print(f"Zero quantity: {clean_df.filter(col('quantity') == 0).count()}")
print(f"Refund orders: {clean_df.filter(col('order_type') == 'refund').count()}")
print(f"Purchase orders: {clean_df.filter(col('order_type') == 'purchase').count()}")

clean_df.show(5, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col

silver_df = spark.read.format("delta").load("/Volumes/main/ecommerce/streaming_data/delta_orders_clean")

silver_df = silver_df.withColumn(
    "order_value",
    col("unit_price") * col("quantity")
)

silver_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import sum, col

revenue_city = silver_df.groupBy("city") \
    .agg(sum("order_value").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc())
silver_df = silver_df.fillna({"city": "Unknown"})

revenue_city.show()

# COMMAND ----------

revenue_city.write.format("delta") \
.mode("overwrite") \
.save("/Volumes/main/ecommerce/streaming_data/gold_revenue_city")

# COMMAND ----------

from pyspark.sql.functions import count

top_products = silver_df.groupBy("product") \
    .agg(count("*").alias("orders")) \
    .orderBy(col("orders").desc())

top_products.show()

# COMMAND ----------

top_products.write.format("delta") \
.mode("overwrite") \
.save("/Volumes/main/ecommerce/streaming_data/gold_top_products")

# COMMAND ----------

payment_stats = silver_df.groupBy("payment_method") \
    .agg(count("*").alias("total_orders"))

payment_stats.show()


# COMMAND ----------

payment_stats.write.format("delta") \
.mode("overwrite") \
.save("/Volumes/main/ecommerce/streaming_data/gold_payment_stats")

# COMMAND ----------

from pyspark.sql.functions import col

latest_orders = silver_df.orderBy(col("timestamp").desc())

latest_orders.show(10, truncate=False)

# COMMAND ----------

silver_df.count()

# COMMAND ----------

silver_df.select("timestamp") \
.orderBy("timestamp", ascending=False) \
.show(5, truncate=False)

# COMMAND ----------

silver_df.select("order_id") \
.orderBy("order_id", ascending=False) \
.show(5)