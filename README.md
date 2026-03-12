# Real-Time E-Commerce Streaming Pipeline

A real-time data engineering pipeline that simulates e-commerce orders and processes them using Kafka, Spark Structured Streaming, and Delta Lake.

---

## Architecture

Python Producer → Kafka → Spark Streaming → Delta Lake → Snowflake

---

## Tech Stack

- Python
- Confluent Kafka
- Databricks
- Spark Structured Streaming
- Delta Lake
- Snowflake

---

## Pipeline Flow

1. Python producer generates fake e-commerce orders.
2. Orders are sent to Kafka topic.
3. Spark Structured Streaming consumes Kafka messages.
4. Data quality transformations clean the data.
5. Clean records stored in Delta Lake.
6. Gold tables generate analytics insights.

---

## Analytics Tables

- Revenue by City
- Top Selling Products
- Payment Method Statistics

---

## Example Output

Revenue by City:

| City | Revenue |
|-----|-----|
| Delhi | 9.1M |
| Pune | 8.5M |
| Nagpur | 9.6M |

---

## Project Structure

```
src/
notebooks/
docs/
README.md
```

---

## Future Improvements

- Real-time dashboards
- Kafka consumer lag monitoring
- Streaming alerts
