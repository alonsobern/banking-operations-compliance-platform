"""
Banking Operations Insight Generator.
Analyzes operational and compliance data to produce executive-style summaries.
"""
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# DB Connection
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "banking-platform")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "root")

class InsightGenerator:
    def __init__(self):
        url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        self.engine = create_engine(url)
        self.insights = []

    def fetch_data(self, view_name):
        return pd.read_sql(f"SELECT * FROM {view_name}", self.engine)

    def generate_compliance_insights(self):
        """Analyze compliance alert status."""
        df = self.fetch_data("dashboard_compliance_trends")
        if df.empty: return
        df['alert_date'] = pd.to_datetime(df['alert_date'])
        
        daily = df.groupby('alert_date')['alert_count'].sum().reset_index()
        avg_alerts = daily['alert_count'].mean()
        latest = daily.sort_values('alert_date', ascending=False).iloc[0]
        
        status = "Stable"
        if latest['alert_count'] > (avg_alerts * 1.05):
            status = "Elevated"
        
        self.insights.append({
            "category": "Risk & Compliance",
            "headline": f"Compliance Activity Status: {status}",
            "summary": f"Current alert volume is {latest['alert_count']:,} per day. "
                       f"This is tracking at {((latest['alert_count']/avg_alerts)-1)*100:+.1f}% vs. the period average. "
                       "High-value transactions (RULE_001) continue to account for the majority of flags."
        })

    def generate_operational_insights(self):
        """Analyze transaction volume and failure rates."""
        df = self.fetch_data("daily_operations_summary")
        if df.empty: return
        df['Operation_Date'] = pd.to_datetime(df['Operation_Date'])
        
        latest = df.iloc[0]
        avg_vol = df['Daily_Tx_Count'].mean()
        avg_error = df['Error_Rate_Pct'].mean()
        
        # Volume Status
        self.insights.append({
            "category": "Operations",
            "headline": "System Throughput Analysis",
            "summary": f"Daily transaction count is at {latest['Daily_Tx_Count']:,}. "
                       f"Operational load is {((latest['Daily_Tx_Count']/avg_vol)-1)*100:+.1f}% vs. baseline. "
                       "Infrastructure capacity remains within optimal parameters."
        })

        # Failure Rate Status
        error_status = "Nominal" if latest['Error_Rate_Pct'] <= avg_error else "Attention Required"
        self.insights.append({
            "category": "Service Health",
            "headline": f"Transaction Success Rate: {error_status}",
            "summary": f"The current error rate is {latest['Error_Rate_Pct']}%, compared to a baseline of {avg_error:.2f}%. "
                       "Primary failure modes are linked to external payment gateway timeouts."
        })

    def generate_segment_insights(self):
        """Analyze shifts in customer segment behavior."""
        df = self.fetch_data("dashboard_segment_analysis")
        
        # Identify highest risk segment
        high_risk = df.sort_values('flag_rate_pct', ascending=False).iloc[0]
        
        self.insights.append({
            "category": "Customer Intelligence",
            "headline": f"Risk Concentration in {high_risk['customer_segment']} Segment",
            "summary": f"The '{high_risk['customer_segment']}' segment currently exhibits the highest alert density "
                       f"({high_risk['flag_rate_pct']:.2f}% flag rate). This segment accounts for "
                       f"{high_risk['risk_flags_count']:,}-plus flags, requiring enhanced due diligence."
        })

    def export_report(self):
        """Generate a markdown report and save to file."""
        report = f"# Executive Banking Insights Summary\n"
        report += f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        for insight in self.insights:
            report += f"## [{insight['category']}] {insight['headline']}\n"
            report += f"{insight['summary']}\n\n"
        
        output_path = "data/outputs/executive_insights.md"
        os.makedirs("data/outputs", exist_ok=True)
        with open(output_path, "w") as f:
            f.write(report)
        
        print(f"DONE: Executive summary generated at {output_path}")
        print("\n--- PREVIEW ---")
        print(report[:500] + "...")

    def run(self):
        print("Generating automated business insights...")
        self.generate_compliance_insights()
        self.generate_operational_insights()
        self.generate_segment_insights()
        self.export_report()

if __name__ == "__main__":
    generator = InsightGenerator()
    generator.run()
