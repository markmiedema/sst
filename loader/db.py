# D:\sst\loader\db.py

from dotenv import load_dotenv
import os
import psycopg2

# This line finds and loads the .env file from your project root (D:\sst)
load_dotenv()

def get_connection():
    """Return a psycopg2 connection using credentials from environment vars."""
    return psycopg2.connect(
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE"),
        sslmode=os.getenv("PGSSLMODE", "require")
    )