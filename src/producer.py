import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'TPNOC3H64TWZ3Y6S',
    'sasl.password': 'cfltoD6ErLeBjM5PljKh1ejlUq4/18ftM0OZGTiKjZdC38SSZaugK7U2qKIyqosg',
    'client.id': 'ecommerce-producer'
}

producer = Producer(config)

PRODUCTS = {
    'Laptop':       {'min': 35000,  'max': 120000, 'category': 'Electronics'},
    'Smartphone':   {'min': 8000,   'max': 80000,  'category': 'Electronics'},
    'Headphones':   {'min': 500,    'max': 8000,   'category': 'Electronics'},
    'Tablet':       {'min': 12000,  'max': 60000,  'category': 'Electronics'},
    'Charger':      {'min': 299,    'max': 2500,   'category': 'Accessories'},
    'Keyboard':     {'min': 400,    'max': 6000,   'category': 'Accessories'},
    'Mouse':        {'min': 250,    'max': 3000,   'category': 'Accessories'},
    'Monitor':      {'min': 8000,   'max': 40000,  'category': 'Electronics'},
    'Smartwatch':   {'min': 1500,   'max': 35000,  'category': 'Wearables'},
    'Earbuds':      {'min': 800,    'max': 12000,  'category': 'Electronics'},
    'Pen Drive':    {'min': 199,    'max': 1200,   'category': 'Accessories'},
    'Hard Disk':    {'min': 2500,   'max': 9000,   'category': 'Storage'},
}

CITIES = {
    'Mumbai':    {'state': 'Maharashtra',  'tier': 1},
    'Delhi':     {'state': 'Delhi',        'tier': 1},
    'Bangalore': {'state': 'Karnataka',    'tier': 1},
    'Pune':      {'state': 'Maharashtra',  'tier': 1},
    'Hyderabad': {'state': 'Telangana',    'tier': 1},
    'Chennai':   {'state': 'Tamil Nadu',   'tier': 1},
    'Kolkata':   {'state': 'West Bengal',  'tier': 1},
    'Jaipur':    {'state': 'Rajasthan',    'tier': 2},
    'Lucknow':   {'state': 'Uttar Pradesh','tier': 2},
    'Surat':     {'state': 'Gujarat',      'tier': 2},
    'Nagpur':    {'state': 'Maharashtra',  'tier': 2},
    'Solapur':   {'state': 'Maharashtra',  'tier': 3},
}

PAYMENT_METHODS = ['UPI', 'Credit Card', 'Debit Card', 'Net Banking', 'Cash on Delivery', 'EMI']
ORDER_STATUSES  = ['placed', 'confirmed', 'shipped', 'out_for_delivery', 'delivered', 'cancelled', 'returned']
STATUS_WEIGHTS  = [30, 25, 20, 10, 8, 5, 2]

order_counter = 1

def maybe_null(value, null_chance=0.05):
    """Randomly return None instead of value — simulates missing data"""
    return None if random.random() < null_chance else value

def generate_order():
    global order_counter

    product_name = random.choice(list(PRODUCTS.keys()))
    product      = PRODUCTS[product_name]
    city_name    = random.choice(list(CITIES.keys()))
    city         = CITIES[city_name]

    unit_price = round(random.uniform(product['min'], product['max']), 2)
    quantity   = random.randint(1, 3)
    discount   = round(random.uniform(0, 0.25), 2)
    total      = round(unit_price * quantity * (1 - discount), 2)

    # Inject different types of real-world data issues
    issue = random.random()

    if issue < 0.05:
        # Case 1: Negative price — payment reversal / refund bug
        unit_price = -abs(unit_price)
        total      = -abs(total)

    elif issue < 0.10:
        # Case 2: Quantity is 0 — cart bug, item removed but order fired
        quantity = 0
        total    = 0.0

    elif issue < 0.14:
        # Case 3: Future timestamp — timezone bug in mobile app
        from datetime import timedelta
        timestamp = (datetime.now() + timedelta(hours=random.randint(1, 48))).isoformat()
    else:
        timestamp = datetime.now().isoformat()

    order = {
        'order_id':       f'ORD-2024-{order_counter:06d}',
        'customer_id':    maybe_null(f'CUST-{random.randint(10000, 99999)}', 0.03),  # 3% missing
        'product':        product_name,
        'category':       product['category'],
        'quantity':       quantity,
        'unit_price':     maybe_null(unit_price, 0.04),   # 4% missing price
        'discount_pct':   discount,
        'total_amount':   maybe_null(total, 0.04),        # 4% missing total
        'payment_method': maybe_null(random.choice(PAYMENT_METHODS), 0.05),  # 5% missing
        'city':           maybe_null(city_name, 0.03),
        'state':          city['state'],
        'city_tier':      city['tier'],
        'status':         random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
        'timestamp':      timestamp if 'timestamp' in dir() else datetime.now().isoformat(),
    }

    order_counter += 1
    return order

def delivery_report(err, msg):
    if err:
        print(f'❌ Delivery failed: {err}')
    else:
        print(f'✅ Partition {msg.partition()} | Offset {msg.offset()}')

print("🚀 Starting E-Commerce Order Producer (with real-world data issues)...")

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
        print(f"📦 {order['order_id']} | {order['product']} x{order['quantity']} | ₹{order['total_amount']} | {order['city']} | {order['status']}")
        time.sleep(1)

except KeyboardInterrupt:
    print("\n⏹ Producer stopped.")
finally:
    producer.flush()