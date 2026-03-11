import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer

# Confluent Cloud config
config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'TPNOC3H64TWZ3Y6S',
    'sasl.password': 'cfltoD6ErLeBjM5PljKh1ejlUq4/18ftM0OZGTiKjZdC38SSZaugK7U2qKIyqosg',
    'client.id': 'ecommerce-producer'
}

producer = Producer(config)

PRODUCTS = ['Laptop', 'Phone', 'Headphones', 'Tablet', 'Charger', 'Keyboard', 'Mouse']
CITIES = ['Mumbai', 'Delhi', 'Bangalore', 'Pune', 'Hyderabad', 'Chennai']
STATUSES = ['placed', 'confirmed', 'shipped', 'delivered', 'cancelled']

def generate_order():
    return {
        'order_id': f'ORD-{random.randint(10000, 99999)}',
        'customer_id': f'CUST-{random.randint(1000, 9999)}',
        'product': random.choice(PRODUCTS),
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(100, 50000), 2),
        'city': random.choice(CITIES),
        'status': random.choice(STATUSES),
        'timestamp': datetime.now().isoformat()
    }

def delivery_report(err, msg):
    if err:
        print(f'❌ Delivery failed: {err}')
    else:
        print(f'✅ Sent to partition {msg.partition()} | offset {msg.offset()}')

print("🚀 Starting E-Commerce Order Producer...")

try:
    while True:
        order = generate_order()
        producer.produce(
            topic='ecommerce-orders',
            key=order['order_id'],
            value=json.dumps(order),
            callback=delivery_report
        )
        producer.poll(0)
        print(f"📦 Order: {order['order_id']} | {order['product']} | {order['city']} | ₹{order['price']}")
        time.sleep(1)

except KeyboardInterrupt:
    print("\n⏹ Producer stopped.")
finally:
    producer.flush()