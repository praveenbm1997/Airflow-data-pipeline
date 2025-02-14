from sqlalchemy import create_engine
import pandas as pd

# Define SQLite database path
DB_FILE_PATH = "/usr/local/airflow/include/superstore.db"
DB_URL = f"sqlite:///{DB_FILE_PATH}"

# Create a connection to the database
engine = create_engine(DB_URL)

# Define your SQL query
query = "SELECT * FROM dim_location LIMIT 10;"  # Fetch first 10 rows

# loading results into a Pandas DataFrame
df = pd.read_sql_query(query, engine)

print(df)
