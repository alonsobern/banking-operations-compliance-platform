"""
Data Quality & Validation Suite for Banking ETL.
Performs pre-load checks to ensure data integrity and business rule compliance.
"""
import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_validation.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_RAW_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "..", "data", "raw"))

class DataValidator:
    def __init__(self):
        self.datasets = {}
        self.validation_results = []
        self.errors_found = 0
        self.warnings_found = 0

    def load_data(self):
        """Load all CSV files for cross-validation."""
        files = {
            "branches": "branches.csv",
            "customers": "customers.csv",
            "accounts": "accounts.csv",
            "transactions": "transactions.csv",
            "compliance_flags": "compliance_flags.csv"
        }
        for key, filename in files.items():
            path = os.path.join(DATA_RAW_DIR, filename)
            if os.path.exists(path):
                self.datasets[key] = pd.read_csv(path)
                logger.info(f"Loaded {key} dataset: {len(self.datasets[key]):,} rows.")
            else:
                logger.error(f"Missing dataset: {filename}")

    def check_nulls(self):
        """Detect missing values in critical columns."""
        logger.info("Checking for missing values...")
        critical_cols = {
            "customers": ["customer_id", "email", "registration_date"],
            "accounts": ["account_id", "customer_id", "branch_id", "opened_at"],
            "transactions": ["transaction_id", "account_id", "transaction_timestamp", "amount"],
            "branches": ["branch_id", "branch_name"]
        }
        
        for table, cols in critical_cols.items():
            df = self.datasets.get(table)
            if df is not None:
                null_counts = df[cols].isnull().sum()
                for col, count in null_counts.items():
                    if count > 0:
                        logger.error(f"NULL ERROR: Table '{table}', Column '{col}' has {count} missing values.")
                        self.errors_found += 1
                    else:
                        logger.debug(f"OK: Table '{table}', Column '{col}' is null-free.")

    def check_duplicates(self):
        """Identify duplicate primary keys."""
        logger.info("Checking for duplicate IDs...")
        id_cols = {
            "customers": "customer_id",
            "accounts": "account_id",
            "transactions": "transaction_id",
            "branches": "branch_id",
            "compliance_flags": "flag_id"
        }
        
        for table, col in id_cols.items():
            df = self.datasets.get(table)
            if df is not None:
                duplicates = df[col].duplicated().sum()
                if duplicates > 0:
                    logger.error(f"DUPLICATE ERROR: Table '{table}' has {duplicates} duplicate {col} values.")
                    self.errors_found += 1
                else:
                    logger.info(f"OK: Table '{table}' unique constraint verified.")

    def check_referential_integrity(self):
        """Validate foreign key consistency across datasets."""
        logger.info("Checking referential integrity...")
        
        # 1. Accounts -> Customers
        acc_df = self.datasets.get("accounts")
        cust_df = self.datasets.get("customers")
        if acc_df is not None and cust_df is not None:
            missing_cust = acc_df[~acc_df["customer_id"].isin(cust_df["customer_id"])]
            if len(missing_cust) > 0:
                logger.error(f"FK ERROR: {len(missing_cust)} accounts point to non-existent customers.")
                self.errors_found += 1
            else:
                logger.info("OK: Accounts -> Customers FK integrity verified.")

        # 2. Transactions -> Accounts
        tx_df = self.datasets.get("transactions")
        if tx_df is not None and acc_df is not None:
            missing_acc = tx_df[~tx_df["account_id"].isin(acc_df["account_id"])]
            if len(missing_acc) > 0:
                logger.error(f"FK ERROR: {len(missing_acc)} transactions point to non-existent accounts.")
                self.errors_found += 1
            else:
                logger.info("OK: Transactions -> Accounts FK integrity verified.")

    def validate_business_rules(self):
        """Validate banking-specific business rules."""
        logger.info("Validating business rules...")
        
        # 1. Transaction amounts must be positive
        tx_df = self.datasets.get("transactions")
        if tx_df is not None:
            negative_tx = tx_df[tx_df["amount"] <= 0]
            if len(negative_tx) > 0:
                logger.warning(f"RULE WARNING: Found {len(negative_tx)} transactions with non-positive amounts.")
                self.warnings_found += 1
            
            # 2. Outlier detection (extremely high amounts > $1M)
            outliers = tx_df[tx_df["amount"] > 1_000_000]
            if len(outliers) > 0:
                logger.info(f"RULE INFO: Found {len(outliers)} high-value transactions (> $1M) for compliance review.")

        # 3. Account status consistency (Transactions on Closed accounts)
        if tx_df is not None and "account_status_at_tx" in tx_df.columns:
            closed_tx = tx_df[tx_df["account_status_at_tx"] == "Closed"]
            if len(closed_tx) > 0:
                logger.warning(f"COMPLIANCE WARNING: {len(closed_tx)} transactions occurred on 'Closed' accounts.")
                self.warnings_found += 1

    def run_all_checks(self):
        """Execute the full validation pipeline."""
        logger.info("=== Starting Data Validation Pipeline ===")
        start_time = datetime.now()
        
        self.load_data()
        self.check_nulls()
        self.check_duplicates()
        self.check_referential_integrity()
        self.validate_business_rules()
        
        duration = datetime.now() - start_time
        logger.info(f"=== Validation Summary ===")
        logger.info(f"Duration: {duration.total_seconds():.2f} seconds")
        logger.info(f"Errors Found: {self.errors_found}")
        logger.info(f"Warnings Found: {self.warnings_found}")
        
        if self.errors_found == 0:
            logger.info("✅ Result: DATA QUALITY PASSED. Ready for ETL.")
        else:
            logger.error("❌ Result: DATA QUALITY FAILED. Fix errors before loading.")
        
        return self.errors_found == 0

if __name__ == "__main__":
    validator = DataValidator()
    validator.run_all_checks()
