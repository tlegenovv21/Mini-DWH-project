import os
import random
import psycopg2
from faker import Faker
from datetime import datetime, timedelta

# Configuration
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 50
NUM_ORDERS = 20000  # Will generate roughly 5-10 items per order, so ~100k-200k items
BATCH_SIZE = 1000

# Connect to Postgres
try:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "oltp"),
        user=os.getenv("POSTGRES_USER", "demo"),
        password=os.getenv("POSTGRES_PASSWORD", "demo_pass"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )
    conn.autocommit = False
    cursor = conn.cursor()
    print("Connected to PostgreSQL")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    exit(1)

fake = Faker()

def generate_customers(n):
    print(f"Generating {n} customers...")
    customers = []
    for _ in range(n):
        customers.append((fake.name(), fake.unique.email(), fake.date_time_this_decade()))
    
    execute_batch("INSERT INTO customers (full_name, email, created_at) VALUES (%s, %s, %s) ON CONFLICT (email) DO NOTHING", customers)

def generate_products(n):
    print(f"Generating {n} products...")
    categories = ['Electronics', 'Books', 'Home', 'Clothing', 'Toys']
    products = []
    for _ in range(n):
        products.append((fake.word().capitalize(), random.choice(categories), round(random.uniform(10, 1000), 2)))
    
    execute_batch("INSERT INTO products (name, category, price) VALUES (%s, %s, %s)", products)

def generate_orders(n):
    print(f"Generating {n} orders...")
    
    # Get customer IDs and Product IDs
    cursor.execute("SELECT customer_id FROM customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT product_id, price FROM products")
    products = {row[0]: row[1] for row in cursor.fetchall()}
    product_ids = list(products.keys())

    if not customer_ids or not product_ids:
        print("No customers or products found. skipping orders.")
        return

    orders = []
    order_items = []
    
    start_date = datetime.now() - timedelta(days=180)
    
    for _ in range(n):
        customer_id = random.choice(customer_ids)
        order_ts = fake.date_time_between(start_date=start_date, end_date='now')
        status = random.choices(['new', 'paid', 'cancelled'], weights=[20, 70, 10])[0]
        
        orders.append((customer_id, order_ts, status))
        
        # We need order_id for items, so we'll have to insert orders first or use a different approach.
        # For simplicity in bulk insert, let's just insert orders, then fetch them back or assume serial (risky).
        # Better approach: Insert orders, get IDs, then generate items.
        # But for 20k orders, let's do batches.
        
    # Execute order batch
    execute_batch("INSERT INTO orders (customer_id, order_ts, status) VALUES (%s, %s, %s)", orders)
    
    # Now generate items for existing orders
    # This is a bit inefficient for huge datasets but fine for 20k.
    print("Generating order items...")
    cursor.execute("SELECT order_id FROM orders")
    order_ids = [row[0] for row in cursor.fetchall()]
    
    for order_id in order_ids:
        num_items = random.randint(1, 10)
        for _ in range(num_items):
            product_id = random.choice(product_ids)
            qty = random.randint(1, 5)
            price = products[product_id]
            order_items.append((order_id, product_id, qty, price))
            
        if len(order_items) >= BATCH_SIZE:
             execute_batch("INSERT INTO order_items (order_id, product_id, qty, item_price) VALUES (%s, %s, %s, %s)", order_items)
             order_items = []

    if order_items:
        execute_batch("INSERT INTO order_items (order_id, product_id, qty, item_price) VALUES (%s, %s, %s, %s)", order_items)


def execute_batch(query, data):
    try:
        psycopg2.extras.execute_batch(cursor, query, data, page_size=BATCH_SIZE)
        conn.commit()
    except Exception as e:
        print(f"Error executing batch: {e}")
        conn.rollback()

if __name__ == "__main__":
    # Ensure psycopg2.extras is available
    import psycopg2.extras
    
    # Check if data exists
    cursor.execute("SELECT count(*) FROM customers")
    if cursor.fetchone()[0] > 0:
        print("Data already exists. Skipping generation.")
    else:
        generate_customers(NUM_CUSTOMERS)
        generate_products(NUM_PRODUCTS)
        generate_orders(NUM_ORDERS)
        print("Data generation complete.")

    cursor.close()
    conn.close()
