"""
ETL Pipeline for Banking Operations & Compliance Monitoring Platform.
Loads synthetic CSV datasets from data/raw/ into PostgreSQL tables.
"""
import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_process.log')
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "banking-platform")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "root")

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_RAW_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "..", "data", "raw"))

def get_db_engine():
    """Create and return a SQLAlchemy engine."""
    try:
        connection_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_url)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"Successfully connected to PostgreSQL at {DB_HOST}/{DB_NAME}")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def load_csv_to_df(file_name):
    """Read a CSV file into a pandas DataFrame with error handling."""
    file_path = os.path.join(DATA_RAW_DIR, file_name)
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        df = pd.read_csv(file_path)
        logger.info(f"Successfully read {file_name} ({len(df):,} rows)")
        return df
    except Exception as e:
        logger.error(f"Error reading {file_name}: {e}")
        raise

def insert_dataframe_to_table(df, table_name, engine, if_exists='append'):
    """Insert a DataFrame into a PostgreSQL table using SQLAlchemy."""
    try:
        # Optimization: Use method='multi' and chunksize for large datasets like transactions
        chunksize = 10000 if table_name == 'transactions' else 1000
        
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
            method='multi'
        )
        logger.info(f"Successfully loaded {len(df):,} rows into table '{table_name}'")
    except SQLAlchemyError as e:
        logger.error(f"Database error while loading table '{table_name}': {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while loading table '{table_name}': {e}")
        raise

def validate_load(table_name, expected_count, engine):
    """Verify the number of rows in a table matches the expected count."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            actual_count = result.scalar()
            
            if actual_count == expected_count:
                logger.info(f"✅ Validation PASSED for '{table_name}': {actual_count:,} rows.")
            else:
                logger.warning(f"⚠️ Validation WARNING for '{table_name}': Expected {expected_count:,}, found {actual_count:,}.")
            return actual_count
    except Exception as e:
        logger.error(f"Validation failed for '{table_name}': {e}")
        return None

def run_etl():
    """Main ETL execution flow."""
    logger.info("Starting ETL process...")
    engine = None
    try:
        engine = get_db_engine()
        
        # Define the load order (respecting foreign key constraints)
        # 1. Branches (no FKs)
        # 2. Customers (no FKs)
        # 3. Accounts (FKs to Customers, Branches)
        # 4. Transactions (FKs to Accounts)
        # 5. Compliance Flags (FKs to Transactions, Accounts)
        
        load_tasks = [
            ("branches.csv", "branches"),
            ("customers.csv", "customers"),
            ("accounts.csv", "accounts"),
            ("transactions.csv", "transactions"),
            ("compliance_flags.csv", "compliance_flags")
        ]
        
        # Clear existing data before reload (Optional, based on if_exists='append' logic)
        # In this portfolio context, we'll clear first to ensure a clean state
        with engine.begin() as conn:
            logger.info("Clearing existing data (cascading)...")
            conn.execute(text("TRUNCATE branches, customers, accounts, transactions, compliance_flags CASCADE"))

        for csv_file, table_name in load_tasks:
            df = load_csv_to_df(csv_file)
            
            # Data Cleaning/Preparation (Specific to our banking dataset)
            if table_name == 'transactions':
                # Convert 'transaction_timestamp' strings to datetime objects
                df['transaction_timestamp'] = pd.to_datetime(df['transaction_timestamp'])
                df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
            elif table_name == 'compliance_flags':
                df['flagged_at'] = pd.to_datetime(df['flagged_at'])
                df['resolved_at'] = pd.to_datetime(df['resolved_at'])
            
            insert_dataframe_to_table(df, table_name, engine)
            validate_load(table_name, len(df), engine)

        logger.info("🎉 ETL process completed successfully!")

    except Exception as e:
        logger.critical(f"ETL process failed: {e}")
    finally:
        if engine:
            engine.dispose()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    run_etl()
