from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from sqlalchemy import create_engine
from dags.superstore_transformation import create_dim_customers, create_dim_products, create_dim_dates, create_dim_location, create_fact_sales

# SQLite database path
DB_FILE_PATH = "/usr/local/airflow/include/superstore.db"
DB_URL = f"sqlite:///{DB_FILE_PATH}"

# CSV Data File Path
DATA_FILE_PATH = "/usr/local/airflow/include/Superstore.csv"

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Function to process and load data
def process_and_load_data():
    df = pd.read_csv(DATA_FILE_PATH)
    
    # Data cleaning and transformation
    df.drop(columns=["Row ID"], inplace=True, errors="ignore")
    df["Postal Code"].fillna("Unknown", inplace=True)

    # Convert data types
    df["Order Date"] = pd.to_datetime(df["Order Date"], dayfirst=True, errors="coerce")
    df["Ship Date"] = pd.to_datetime(df["Ship Date"], dayfirst=True, errors="coerce")

    # creating new calculated fields for analysis
    df["Profit Margin"] = df["Profit"] / df["Sales"]
    df["Discount Amount"] = df["Sales"] * df["Discount"]
    df["Shipping Duration"] = (df["Ship Date"] - df["Order Date"]).dt.days

    # Create categorical columnsfor profit and sales
    df["Profit Category"] = pd.cut(df["Profit"], bins=[-float("inf"), 0, 100, float("inf")], labels=["Loss", "Low Profit", "High Profit"])
    df["Sales Tier"] = pd.cut(df["Sales"], bins=[0, 500, 2000, float("inf")], labels=["Low", "Medium", "High"])

    # Extract date components
    df["Year"] = df["Order Date"].dt.year
    df["Quarter"] = df["Order Date"].dt.quarter
    df["Month"] = df["Order Date"].dt.month
    df["Day"] = df["Order Date"].dt.day

    # Create a unique identifier for each order product
    df["order_product_id"] = df["Order ID"].astype(str) + "_" + df["Product ID"].astype(str)
    df.drop_duplicates(subset=["order_product_id"], inplace=True)

    # Standardize column names 
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df.rename(columns={"sub-category": "sub_category"}, inplace=True)

    # SQLite connection
    engine = create_engine(DB_URL)

    # Create Table if not exists
    with engine.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sales_data (
                order_product_id TEXT,
                order_id TEXT,
                product_id TEXT,
                order_date TEXT,
                ship_date TEXT,
                ship_mode TEXT,
                customer_id TEXT,
                customer_name TEXT,
                segment TEXT,
                country TEXT,
                city TEXT,
                state TEXT,
                postal_code TEXT,
                region TEXT,
                category TEXT,
                sub_category TEXT,
                product_name TEXT,
                sales REAL,
                quantity INTEGER,
                discount REAL,
                profit REAL,
                profit_margin REAL,
                discount_amount REAL,
                shipping_duration INTEGER,
                profit_category TEXT,
                sales_tier TEXT,
                year INTEGER,
                quarter INTEGER,
                month INTEGER,
                day INTEGER
            );
        """)

    # Load data into SQLite
    df.to_sql("sales_data", engine, if_exists="append", index=False)
    print("Data successfully loaded into SQLite!")

# DAG 
dag = DAG(
    dag_id="superstore_data_pipeline_sqlite",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Tasks
load_data_task = PythonOperator(
    task_id="process_and_load_data",
    python_callable=process_and_load_data,
    dag=dag,
)

create_customers_task = PythonOperator(
    task_id="create_dim_customers",
    python_callable=create_dim_customers,
    dag=dag,
)

create_products_task = PythonOperator(
    task_id="create_dim_products",
    python_callable=create_dim_products,
    dag=dag,
)

create_dates_task = PythonOperator(
    task_id="create_dim_dates",
    python_callable=create_dim_dates,
    dag=dag,
)

create_location_task = PythonOperator(
    task_id="create_dim_location",
    python_callable=create_dim_location,
    dag=dag,
)

create_fact_sales_task = PythonOperator(
    task_id="create_fact_sales",
    python_callable=create_fact_sales,
    dag=dag,
)

# Task Dependencies
load_data_task >> [create_customers_task, create_products_task, create_dates_task, create_location_task] >> create_fact_sales_task
