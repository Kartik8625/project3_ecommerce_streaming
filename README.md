# Real-Time E-Commerce Order Streaming Pipeline 🚀

![Status](https://img.shields.io/badge/Status-Complete-brightgreen)
![Difficulty](https://img.shields.io/badge/Difficulty-9%2F10-red)
![Spark](https://img.shields.io/badge/Spark-4.1.0-orange)
![Kafka](https://img.shields.io/badge/Kafka-Confluent%20Cloud-blue)

## Overview
A real-time streaming pipeline that processes e-commerce orders as they happen — ingesting live data from Kafka, cleaning it with Spark Structured Streaming, storing in Delta Lake, and loading into Snowflake for analytics.

## Pipeline Architecture
```
Python Producer → Confluent Kafka → Databricks Spark 4.1.0 → Delta Lake → Snowflake
   (1 order/sec)    (message broker)   (stream processing)    (storage)    (analytics)
```

## Tech Stack
| Tool | Purpose | Version |
|------|---------|---------|
| Python | Order data producer | 3.12 |
| Confluent Cloud Kafka | Managed message broker | Latest |
| Databricks Serverless | Spark runtime | Spark 4.1.0 |
| Spark Structured Streaming | Stream processing | 4.1.0 |
| Delta Lake | ACID streaming storage | Unity Catalog |
| Snowflake | Analytics warehouse | Latest |

## Dataset — Simulated E-Commerce Orders
- **Source:** Python producer generating 1 order/second
- **Volume:** 3,866+ records processed
- **Fields:** 15 columns including order_id, product, city, price, payment_method
- **Cities:** 12 Indian cities across tier 1/2/3
- **Products:** 12 categories with realistic INR price ranges

## Real-World Data Issues Injected
| Issue | Frequency | Real Cause |
|-------|-----------|-----------|
| Null customer_id | 3% | Guest checkout |
| Null unit_price | 4% | Pricing service timeout |
| Negative price | 5% | Refund/reversal bug |
| Quantity = 0 | 5% | Cart race condition |
| Future timestamp | 4% | Mobile timezone bug |
| Null payment_method | 5% | Payment gateway dropout |

## Data Quality Transformations (Spark)
- Drop rows with null `order_id`, `unit_price`, `total_amount`
- Drop rows with `quantity = 0`
- Fill null `customer_id` → `GUEST-UNKNOWN`
- Fill null `payment_method` → `Unknown`
- Convert negative prices → absolute value
- Fix future timestamps → current timestamp
- Enrich with `order_type` column (purchase/refund)

## Key Results
- **3,866 clean records** loaded into Snowflake
- **Top product:** Keyboard (368 orders)
- **Top city by revenue:** Kolkata (₹1.4Cr)
- **Most used payment:** Net Banking (632 orders)
- **Data quality:** 0 nulls, 0 negative prices after transformation

## Project Structure
```
project3_ecommerce_streaming/
├── src/
│   ├── producer.py                    # Kafka order producer
│   └── databricks/
│       └── kafka_spark_streaming.py   # Spark streaming pipeline
├── .gitignore
└── README.md
```

## Pipeline Flow — Step by Step
1. `producer.py` generates 1 realistic order/second with intentional dirty data
2. Orders published to Confluent Kafka topic `ecommerce-orders` (3 partitions)
3. Databricks Spark reads stream using `readStream` with Kafka connector
4. Raw binary messages decoded → JSON → typed DataFrame
5. Data quality transformations applied — clean DataFrame produced
6. Clean stream written to Delta Lake using `availableNow` trigger
7. Delta Lake data loaded into Snowflake `ORDERS_CLEAN` table
8. SQL analytics run on Snowflake for business insights

## Key Concepts Demonstrated
- **Batch vs Streaming:** File processing vs infinite live data
- **Kafka partitions:** Parallel message lanes for scalability
- **Checkpointing:** Crash recovery — knows where stream left off
- **Delta Lake:** ACID guarantees on streaming writes
- **Data quality layer:** Detect, flag, fix bad data before storage

## Challenges & Solutions
| Challenge | Solution |
|-----------|----------|
| Databricks DBFS disabled | Used Unity Catalog Volumes |
| `ProcessingTime` trigger not supported | Switched to `availableNow=True` |
| Kafka JAAS config error | Used `kafkashaded.org.apache.kafka` prefix |
| Row-by-row Snowflake insert too slow | Used `executemany()` for bulk insert |

## Production Enhancements (Future Scope)
- Schema Registry for Kafka message validation
- Airflow scheduling for automated Delta Lake loads
- dbt models for Snowflake transformations
- Real-time dashboards on Snowflake

## Author
**Kartik Inamdar** — 3rd Year E&TC Engineering Student
- GitHub: [Kartik8625](https://github.com/Kartik8625)
- LinkedIn: [Kartik Inamdar](https://linkedin.com/in/kartik-inamdar)
- Email: inamdarkartik31@gmail.com
