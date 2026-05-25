"""
KPI Calculation Module for Banking Operations.
Aggregates operational and compliance metrics from PostgreSQL.
"""
import os
import json
import decimal
from datetime import timedelta, datetime, date
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# DB Connection
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "banking-platform")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "root")


class BankingJSONEncoder(json.JSONEncoder):
    """Custom encoder to handle PostgreSQL types that are not JSON-serializable."""

    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, timedelta):
            return str(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def get_engine():
    url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def deploy_kpi_views():
    """Execute kpi_metrics.sql to create/replace database views.

    Uses individual statement execution so that a failure on one view
    does not roll back the others, and gives clear per-view feedback.
    """
    engine = get_engine()
    sql_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "sql", "kpi_metrics.sql"
    )

    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_content = f.read()

        with engine.begin() as conn:  # auto-commits on exit
            conn.execute(text(sql_content))

        print("DONE: KPI Views deployed successfully.")
    except Exception as e:
        print(f"ERROR deploying KPI views: {e}")


def calculate_top_level_kpis():
    """Extract top-level KPIs and save as JSON + print summary."""
    engine = get_engine()

    query = "SELECT * FROM dashboard_global_kpis"

    try:
        df = pd.read_sql(query, engine)

        if df.empty:
            print("WARNING: dashboard_global_kpis returned no rows.")
            return

        # Convert to plain Python types using the custom encoder round-trip
        raw_dict = df.iloc[0].to_dict()
        kpis = json.loads(json.dumps(raw_dict, cls=BankingJSONEncoder))

        # Ensure output directory exists
        os.makedirs("data/outputs", exist_ok=True)

        output_path = "data/outputs/summary_kpis.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(kpis, f, indent=4)

        print("\n=== TOP LEVEL BANKING KPIs ===")
        labels = {
            "total_tx":             "Total Transactions",
            "total_volume":         "Total Volume (USD)",
            "success_rate_pct":     "Success Rate (%)",
            "failure_rate_pct":     "Failure Rate (%)",
            "suspicious_rate_pct":  "Suspicious Rate (%)",
            "avg_tx_amount":        "Avg Transaction (USD)",
        }
        for key, label in labels.items():
            value = kpis.get(key, "N/A")
            print(f"  {label:<25} : {value:>15}")
        print("==============================")
        print(f"\n  JSON saved to: {output_path}")

    except Exception as e:
        print(f"ERROR calculating KPIs: {e}")


if __name__ == "__main__":
    os.makedirs("data/outputs", exist_ok=True)
    deploy_kpi_views()
    calculate_top_level_kpis()
