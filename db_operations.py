import pandas as pd
import re
import logging

logging.basicConfig(
    filename='data_ingestion.log', 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def is_valid_email(email):
    pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    return re.match(pattern, email) is not None

def is_positive_price(price):
    try:
        return float(price) > 0
    except (ValueError, TypeError):
        return False

def is_positive_integer(value):
    try:
        return int(value) > 0
    except (ValueError, TypeError):
        return False


def insert_customers_from_csv(cur, csv_path):
    df = pd.read_csv(csv_path)
    for _, row in df.iterrows():
        if is_valid_email(row['email']):
            cur.execute(
                """
                INSERT INTO customers (name, email)
                VALUES (%s, %s)
                ON CONFLICT (email) DO NOTHING;
                """,
                (row['name'], row['email'])
            )
        else:
            logging.warning(f"Skipped customer with invalid email: {row['email']}")

def insert_products_from_csv(cur, csv_path):
    df = pd.read_csv(csv_path)
    for _, row in df.iterrows():
        if is_positive_price(row['price']):
            cur.execute(
                """
                INSERT INTO products (product_name, price)
                VALUES (%s, %s)
                ON CONFLICT (product_name) DO NOTHING;
                """,
                (row['product_name'], row['price'])
            )
        else:
            logging.warning(f"Skipped product with invalid price: {row['price']} - Product: {row['product_name']}")

def insert_orders_from_csv(cur, csv_path):
    df = pd.read_csv(csv_path)
    for _, row in df.iterrows():
        if is_positive_integer(row['customer_id']):
            customer_id = int(row['customer_id']) #convert native to int
            order_date = row['order_date']  # Assuming order_date is in a valid format
            cur.execute(
                """
                INSERT INTO orders (customer_id, order_date)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (customer_id, order_date)
            )
        else:
            logging.warning(f"Skipped order with invalid customer_id: {row['customer_id']}")

def insert_order_items_from_csv(cur, csv_path):
    df = pd.read_csv(csv_path)
    for _, row in df.iterrows():
        if  (is_positive_integer(row['order_id']) and
            is_positive_integer(row['product_id']) and
            is_positive_integer(row['quantity'])):

            order_id = int(row['order_id'])
            product_id = int(row['product_id'])
            quantity = int(row['quantity'])
            cur.execute(
                """
                INSERT INTO order_items (order_id, product_id, quantity)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (order_id, product_id, quantity)
            )
        else:
            logging.warning(f"Skipped order_item with invalid data: {row.to_dict()}")

def total_sales_over_time(cur):
    """
    Returns list of tuples (order_date, total_sales) aggregated by order_date.
    """
    cur.execute("""
        SELECT DATE(order_date) AS sale_date,
               SUM(p.price * oi.quantity) AS total_sales
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.product_id
        GROUP BY sale_date
        ORDER BY sale_date;
    """)
    return cur.fetchall()

def total_sales_over_time_weekly(cur):
    """
    Returns list of tuples (week_start_date, total_sales) aggregated by week.
    """
    cur.execute("""
        SELECT DATE_TRUNC('week', order_date) AS sale_week,
               SUM(p.price * oi.quantity) AS total_sales
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.product_id
        GROUP BY sale_week
        ORDER BY sale_week;
    """)
    return cur.fetchall()

def insert_sample_data(cur):
    # Insert sample customers
    cur.execute("""
        INSERT INTO customers (name, email) VALUES
        ('Manija Latifi', 'ML@gmail.com'),
        ('Julian Galarza', 'JG@gmail.com'),
        ('Nilya Latifi', 'NL@outlook.com'),
        ('David Galarza', 'DG@outlook.com'),
        ('Stephanie Galarza', 'SG@yahoo.com'),
        ('Eric Hughes', 'EH@yahoo.com'),
        ('Michael Rech', 'MR@aol.com'),
        ('Travis Geraci', 'TG@aol.com')
        ON CONFLICT DO NOTHING;
    """)
    
    # Insert sample products
    cur.execute("""
        INSERT INTO products (product_name, price) VALUES
        ('Callaway Irons Set', 699.99),
        ('Taylormade Irons Set', 599.99),
        ('Cobra Irons Set', 499.99),
        ('Ping Irons Set', 799.99),
        ('Callaway Driver', 449.99),
        ('TaylorMade Driver', 349.99),
        ('Cobra Driver', 349.99),
        ('Ping Driver', 549.99)
        ON CONFLICT DO NOTHING;
    """)
    
    # Insert sample orders
    cur.execute("""
        INSERT INTO orders (customer_id) VALUES
        (1), (2)
        ON CONFLICT DO NOTHING;
    """)
    
    # Insert sample order items
    cur.execute("""
        INSERT INTO order_items (order_id, product_id, quantity) VALUES
        (1, 1, 3),
        (1, 2, 1),
        (2, 2, 2)
        ON CONFLICT DO NOTHING;
    """)

def list_orders_with_customers(cur):
    # Return list of orders with customer names and order dates
    cur.execute("""
        SELECT o.order_id, c.name, o.order_date
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id;
    """)
    return cur.fetchall()

def total_sales_per_customer(cur):
    # Return total sales per customer
    cur.execute("""
        SELECT c.name, SUM(p.price * oi.quantity) AS total_spent
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.product_id
        GROUP BY c.name;
    """)
    return cur.fetchall()
