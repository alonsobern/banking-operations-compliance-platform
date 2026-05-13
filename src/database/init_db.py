"""
Initialize the PostgreSQL database for the Banking Platform.
Executes the schema.sql file to create tables and indexes.
"""
import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "banking-platform")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "root")

def init_database():
    """Execute schema.sql against the target database."""
    conn = None
    try:
        print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}...")
        # Connect to 'postgres' first to create the target DB if it doesn't exist
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database="postgres"
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Check if DB exists
        cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
        exists = cur.fetchone()
        if not exists:
            print(f"Creating database '{DB_NAME}'...")
            cur.execute(f"CREATE DATABASE {DB_NAME}")
        
        cur.close()
        conn.close()

        # Connect to the target database
        print(f"Initializing schema in '{DB_NAME}'...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        cur = conn.cursor()

        # Read schema.sql
        schema_path = os.path.join(os.path.dirname(__file__), "..", "..", "sql", "schema.sql")
        with open(schema_path, "r") as f:
            schema_sql = f.read()

        # Execute schema
        cur.execute(schema_sql)
        conn.commit()
        print("✅ Database schema initialized successfully.")

    except Exception as e:
        print(f"❌ Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    init_database()
