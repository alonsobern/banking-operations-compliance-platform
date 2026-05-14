"""
KPI Calculation Module for Banking Operations.
Aggregates operational and compliance metrics from PostgreSQL.
"""
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# DB Connection
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "banking-platform")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "root")

def get_engine():
    url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)

def deploy_kpi_views():
    """Execute kpi_metrics.sql to create database views."""
    engine = get_engine()
    sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "sql", "kpi_metrics.sql")
    
    try:
        with open(sql_path, "r") as f:
            sql = f.read()
            with engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        print("DONE: KPI Views deployed successfully.")
    except Exception as e:
        print(f"ERROR deploying KPI views: {e}")

def calculate_top_level_kpis():
    """Extract top-level KPIs for a summary report."""
    engine = get_engine()
    
    # Query the previously created global KPI view
    query = "SELECT * FROM dashboard_global_kpis"
    
    try:
        df = pd.read_sql(query, engine)
        kpis = df.iloc[0].to_dict()
        
        # Save to JSON for portfolio display
        with open("data/outputs/summary_kpis.json", "w") as f:
            json.dump(kpis, f, indent=4)
            
        print("\n=== TOP LEVEL BANKING KPIs ===")
        for k, v in kpis.items():
            print(f"{k:<20} : {v:>15,}")
        print("==============================")
        
    except Exception as e:
        print(f"ERROR calculating KPIs: {e}")

if __name__ == "__main__":
    # Ensure output directory exists
    os.makedirs("data/outputs", exist_ok=True)
    
    deploy_kpi_views()
    calculate_top_level_kpis()
