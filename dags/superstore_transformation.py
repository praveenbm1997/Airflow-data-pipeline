from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlite3

# SQLite Database Path
DB_FILE_PATH = "/usr/local/airflow/include/superstore.db"

# Function to create the Customers Dimension Table
def create_dim_customers():
    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_id TEXT PRIMARY KEY,
        customer_name TEXT,
        segment TEXT
    );
    """)

    cursor.execute("""
    INSERT OR IGNORE INTO dim_customers (customer_id, customer_name, segment)
    SELECT DISTINCT customer_id, customer_name, segment FROM sales_data;
    """)

    conn.commit()
    conn.close()

# Function to create the Products Dimension Table
def create_dim_products():
    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_products (
        product_id TEXT PRIMARY KEY,
        product_name TEXT,
        category TEXT,
        sub_category TEXT
    );
    """)

    cursor.execute("""
    INSERT OR IGNORE INTO dim_products (product_id, product_name, category, sub_category)
    SELECT DISTINCT product_id, product_name, category, sub_category FROM sales_data;
    """)

    conn.commit()
    conn.close()

# Function to create the Dates Dimension Table
def create_dim_dates():
    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_dates (
        date_id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_date DATE UNIQUE,
        ship_date DATE UNIQUE,
        year INTEGER,
        quarter INTEGER,
        month INTEGER,
        day INTEGER
    );
    """)

    cursor.execute("""
    INSERT OR IGNORE INTO dim_dates (order_date, ship_date, year, quarter, month, day)
    SELECT DISTINCT 
        order_date, 
        ship_date, 
        strftime('%Y', order_date), 
        (strftime('%m', order_date) - 1) / 3 + 1, 
        strftime('%m', order_date), 
        strftime('%d', order_date)
    FROM sales_data;
    """)

    conn.commit()
    conn.close()

# Function to create the Location Dimension Table
def create_dim_location():
    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_location (
        location_id INTEGER PRIMARY KEY AUTOINCREMENT,
        country TEXT,
        city TEXT,
        state TEXT,
        region TEXT,
        postal_code TEXT
    );
    """)

    cursor.execute("""
    INSERT OR IGNORE INTO dim_location (country, city, state, region, postal_code)
    SELECT DISTINCT country, city, state, region, postal_code FROM sales_data;
    """)

    conn.commit()
    conn.close()

# Function to create the Fact Sales Table
def create_fact_sales():
    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fact_sales (
        order_product_id TEXT PRIMARY KEY,
        order_id TEXT,
        customer_id TEXT,
        product_id TEXT,
        date_id INTEGER,
        location_id INTEGER,
        ship_mode TEXT,
        sales NUMERIC,
        quantity INTEGER,
        discount NUMERIC,
        profit NUMERIC,
        profit_margin NUMERIC,
        discount_amount NUMERIC,
        shipping_duration INTEGER,
        profit_category TEXT,
        sales_tier TEXT,
        FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
        FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
        FOREIGN KEY (date_id) REFERENCES dim_dates(date_id),
        FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
    );
    """)

    cursor.execute("""
    INSERT OR REPLACE INTO fact_sales (
        order_product_id, order_id, customer_id, product_id, date_id, location_id,
        ship_mode, sales, quantity, discount, profit, profit_margin, discount_amount,
        shipping_duration, profit_category, sales_tier
    )
    SELECT 
        s.order_product_id, s.order_id, s.customer_id, s.product_id, 
        d.date_id, l.location_id, s.ship_mode, s.sales, s.quantity, 
        s.discount, s.profit, s.profit_margin, s.discount_amount,
        s.shipping_duration, s.profit_category, s.sales_tier
    FROM sales_data s
    LEFT JOIN dim_dates d ON s.order_date = d.order_date
    LEFT JOIN dim_location l ON s.city = l.city AND s.state = l.state;
    """)

    conn.commit()
    conn.close()
