"""
Compliance Monitoring Engine for Banking Operations.
Detects suspicious activities and generates compliance alerts in PostgreSQL.
"""
import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import timedelta

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('compliance_monitoring.log')
    ]
)
logger = logging.getLogger(__name__)

# DB Connection
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "banking-platform")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "root")

class ComplianceEngine:
    def __init__(self):
        connection_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        self.engine = create_engine(connection_url)
        self.flags = []

    def fetch_data(self):
        """Fetch transactions and accounts for analysis."""
        logger.info("Fetching transaction data from database...")
        self.transactions = pd.read_sql("SELECT * FROM transactions", self.engine)
        self.transactions['transaction_timestamp'] = pd.to_datetime(self.transactions['transaction_timestamp'])
        
        self.accounts = pd.read_sql("SELECT * FROM accounts", self.engine)
        logger.info(f"Loaded {len(self.transactions):,} transactions.")

    def add_flag(self, tx_id, account_id, rule_id, reason, risk_level, score, segment, amount, country, timestamp):
        """Helper to structure compliance alerts."""
        self.flags.append({
            "flag_id": f"FLG-{tx_id}-{rule_id}",
            "transaction_id": tx_id,
            "account_id": account_id,
            "rule_id": rule_id,
            "flag_reason": reason,
            "risk_level": risk_level,
            "risk_score": score,
            "customer_segment": segment,
            "status": "Pending Review",
            "analyst_assigned": "System Engine",
            "amount": amount,
            "destination_country": country,
            "flagged_at": timestamp,
            "resolved_at": None,
            "notes": f"System generated alert based on rule {rule_id}."
        })

    def rule_001_high_value(self):
        """Detect transactions above 10,000."""
        logger.info("Applying Rule 001: High Value Transactions...")
        high_val = self.transactions[self.transactions['amount'] > 10000]
        for _, row in high_val.iterrows():
            self.add_flag(
                row['transaction_id'], row['account_id'], "RULE_001",
                "Transaction amount exceeds $10,000 threshold.", "Medium", 65,
                row['customer_segment'], row['amount'], row['destination_country'], row['transaction_timestamp']
            )

    def rule_002_rapid_velocity(self):
        """Detect rapid repeated transfers (3+ in 1 hour)."""
        logger.info("Applying Rule 002: Rapid Transfer Velocity...")
        # Sort by account and timestamp
        df = self.transactions.sort_values(['account_id', 'transaction_timestamp'])
        
        for acc_id, group in df.groupby('account_id'):
            if len(group) < 3: continue
            
            # Check 1-hour window for 3+ transfers
            for i in range(len(group) - 2):
                window = group.iloc[i:i+3]
                time_diff = window['transaction_timestamp'].max() - window['transaction_timestamp'].min()
                if time_diff <= timedelta(hours=1):
                    last_tx = window.iloc[-1]
                    self.add_flag(
                        last_tx['transaction_id'], acc_id, "RULE_002",
                        "Rapid velocity: 3+ transactions within 1 hour.", "High", 85,
                        last_tx['customer_segment'], last_tx['amount'], last_tx['destination_country'], last_tx['transaction_timestamp']
                    )
                    break # Flag once per burst

    def rule_003_high_risk_country(self):
        """Detect transfers to high-risk jurisdictions."""
        logger.info("Applying Rule 003: High-Risk Jurisdictions...")
        high_risk_countries = ['Cayman Islands', 'Panama', 'Seychelles', 'Malta', 'Cyprus']
        suspicious = self.transactions[self.transactions['destination_country'].isin(high_risk_countries)]
        for _, row in suspicious.iterrows():
            self.add_flag(
                row['transaction_id'], row['account_id'], "RULE_003",
                f"Transfer to high-risk jurisdiction: {row['destination_country']}.", "High", 90,
                row['customer_segment'], row['amount'], row['destination_country'], row['transaction_timestamp']
            )

    def rule_004_sudden_activity(self):
        """Detect inactive accounts becoming suddenly active."""
        logger.info("Applying Rule 004: Sudden Activity on Inactive Accounts...")
        # Merge account status for context
        df = self.transactions.merge(self.accounts[['account_id', 'account_status']], on='account_id')
        inactive_tx = df[df['account_status'].isin(['Inactive', 'Frozen'])]
        
        for _, row in inactive_tx.iterrows():
            self.add_flag(
                row['transaction_id'], row['account_id'], "RULE_004",
                f"Sudden activity on {row['account_status']} account.", "Medium", 70,
                row['customer_segment'], row['amount'], row['destination_country'], row['transaction_timestamp']
            )

    def rule_005_excessive_failures(self):
        """Detect excessive failed transactions (5+ in 24 hours)."""
        logger.info("Applying Rule 005: Excessive Failure Bursts...")
        failed_tx = self.transactions[self.transactions['status'] == 'Failed']
        if failed_tx.empty: return
        
        df = failed_tx.sort_values(['account_id', 'transaction_timestamp'])
        for acc_id, group in df.groupby('account_id'):
            if len(group) < 5: continue
            
            for i in range(len(group) - 4):
                window = group.iloc[i:i+5]
                time_diff = window['transaction_timestamp'].max() - window['transaction_timestamp'].min()
                if time_diff <= timedelta(days=1):
                    last_tx = window.iloc[-1]
                    self.add_flag(
                        last_tx['transaction_id'], acc_id, "RULE_005",
                        "Excessive failure rate: 5+ failed attempts in 24h.", "Medium", 60,
                        last_tx['customer_segment'], last_tx['amount'], last_tx['destination_country'], last_tx['transaction_timestamp']
                    )
                    break

    def save_flags(self):
        """Save generated flags back to PostgreSQL."""
        if not self.flags:
            logger.info("No new alerts detected.")
            return

        df_flags = pd.DataFrame(self.flags)
        logger.info(f"Saving {len(df_flags):,} detected alerts to 'compliance_flags' table...")
        
        try:
            # For this demo, we'll append. In a real system, we'd use UPSERT.
            df_flags.to_sql(
                "compliance_flags",
                self.engine,
                if_exists="append",
                index=False,
                method="multi"
            )
            logger.info("✅ Alerts successfully persisted to database.")
        except Exception as e:
            logger.error(f"Failed to save flags: {e}")

    def run_engine(self):
        """Run all monitoring rules."""
        logger.info("=== Initializing Compliance Monitoring Engine ===")
        self.fetch_data()
        
        self.rule_001_high_value()
        self.rule_002_rapid_velocity()
        self.rule_003_high_risk_country()
        self.rule_004_sudden_activity()
        self.rule_005_excessive_failures()
        
        self.save_flags()
        logger.info("=== Monitoring Session Complete ===")

if __name__ == "__main__":
    engine = ComplianceEngine()
    engine.run_engine()
