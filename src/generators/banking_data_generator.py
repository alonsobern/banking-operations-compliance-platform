import pandas as pd
import numpy as np
from faker import Faker
import random
import os
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Configuration
NUM_BRANCHES = 10
NUM_CUSTOMERS = 1000
NUM_ACCOUNTS = 1200  # Some customers have multiple accounts
NUM_TRANSACTIONS = 5000
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2026, 5, 1)

# Get the directory where the script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Calculate root dir (up two levels from src/generators)
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
# Set data dir
DATA_RAW_DIR = os.path.join(ROOT_DIR, "data", "raw")

def ensure_dirs():
    if not os.path.exists(DATA_RAW_DIR):
        os.makedirs(DATA_RAW_DIR)

def generate_branches():
    print("Generating branches...")
    branches = []
    for i in range(1, NUM_BRANCHES + 1):
        branches.append({
            "branch_id": f"BR{i:03d}",
            "branch_name": f"{fake.city()} Main Branch",
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
            "branch_type": random.choice(["Retail", "Corporate", "Wealth Management"])
        })
    df = pd.DataFrame(branches)
    df.to_csv(os.path.join(DATA_RAW_DIR, "branches.csv"), index=False)
    return df

def generate_customers():
    print("Generating customers...")
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        customers.append({
            "customer_id": f"C{i:06d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "address": fake.address().replace('\n', ', '),
            "registration_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
            "customer_segment": random.choice(["Standard", "Silver", "Gold", "Platinum"])
        })
    df = pd.DataFrame(customers)
    df.to_csv(os.path.join(DATA_RAW_DIR, "customers.csv"), index=False)
    return df

def generate_accounts(customers_df, branches_df):
    print("Generating accounts...")
    accounts = []
    account_types = ["Savings", "Checking", "Fixed Deposit", "Current"]
    
    for i in range(1, NUM_ACCOUNTS + 1):
        customer = customers_df.iloc[random.randint(0, len(customers_df) - 1)]
        branch = branches_df.iloc[random.randint(0, len(branches_df) - 1)]
        
        accounts.append({
            "account_id": f"ACC{i:08d}",
            "customer_id": customer["customer_id"],
            "branch_id": branch["branch_id"],
            "account_type": random.choice(account_types),
            "account_status": random.choice(["Active", "Inactive", "Frozen"]),
            "opening_balance": round(random.uniform(100.0, 50000.0), 2),
            "current_balance": 0.0, # Will be updated by transactions
            "opened_at": fake.date_between(start_date=customer["registration_date"], end_date=END_DATE)
        })
    
    df = pd.DataFrame(accounts)
    df["current_balance"] = df["opening_balance"]
    df.to_csv(os.path.join(DATA_RAW_DIR, "accounts.csv"), index=False)
    return df

def generate_transactions(accounts_df):
    print("Generating transactions...")
    transactions = []
    transaction_types = ["Deposit", "Withdrawal", "Transfer", "Payment"]
    channels = ["ATM", "Online", "Branch", "POS"]
    
    for i in range(1, NUM_TRANSACTIONS + 1):
        account = accounts_df.iloc[random.randint(0, len(accounts_df) - 1)]
        tx_type = random.choice(transaction_types)
        amount = round(random.uniform(1.0, 15000.0), 2)
        
        # Adjust amount for withdrawals/payments/transfers
        if tx_type in ["Withdrawal", "Payment", "Transfer"]:
            amount = -amount
            
        transactions.append({
            "transaction_id": f"TX{i:010d}",
            "account_id": account["account_id"],
            "transaction_date": fake.date_time_between(start_date=account["opened_at"], end_date=END_DATE),
            "transaction_type": tx_type,
            "amount": abs(amount),
            "direction": "Credit" if amount > 0 else "Debit",
            "channel": random.choice(channels),
            "merchant_name": fake.company() if tx_type == "Payment" else "N/A",
            "is_suspicious": False # Default
        })
        
    df = pd.DataFrame(transactions)
    df.to_csv(os.path.join(DATA_RAW_DIR, "transactions.csv"), index=False)
    return df

def generate_compliance_flags(transactions_df):
    print("Generating compliance flags...")
    compliance_flags = []
    
    # Simple rules for suspicious activity
    # 1. Large transactions > 10,000
    large_tx = transactions_df[transactions_df["amount"] > 10000].copy()
    for idx, row in large_tx.iterrows():
        compliance_flags.append({
            "flag_id": f"FLG{len(compliance_flags)+1:08d}",
            "transaction_id": row["transaction_id"],
            "flag_reason": "Large Transaction (>10,000)",
            "risk_score": 75,
            "status": "Pending Review",
            "flagged_at": row["transaction_date"] + timedelta(hours=random.randint(1, 24))
        })
        transactions_df.at[idx, "is_suspicious"] = True

    # 2. Frequent transactions from same account (Simulated)
    # We'll just randomly flag some for diversity in the portfolio
    random_flags = transactions_df.sample(n=int(NUM_TRANSACTIONS * 0.02))
    for idx, row in random_flags.iterrows():
        if row["transaction_id"] not in [f["transaction_id"] for f in compliance_flags]:
            compliance_flags.append({
                "flag_id": f"FLG{len(compliance_flags)+1:08d}",
                "transaction_id": row["transaction_id"],
                "flag_reason": random.choice(["Structuring Pattern", "High Frequency Activity", "Unusual Merchant Location"]),
                "risk_score": random.randint(50, 95),
                "status": random.choice(["Pending Review", "Cleared", "Escalated"]),
                "flagged_at": row["transaction_date"] + timedelta(hours=random.randint(1, 48))
            })
            transactions_df.at[idx, "is_suspicious"] = True

    df_flags = pd.DataFrame(compliance_flags)
    df_flags.to_csv(os.path.join(DATA_RAW_DIR, "compliance_flags.csv"), index=False)
    
    # Save updated transactions with is_suspicious flag
    transactions_df.to_csv(os.path.join(DATA_RAW_DIR, "transactions.csv"), index=False)
    return df_flags

def main():
    ensure_dirs()
    branches_df = generate_branches()
    customers_df = generate_customers()
    accounts_df = generate_accounts(customers_df, branches_df)
    transactions_df = generate_transactions(accounts_df)
    generate_compliance_flags(transactions_df)
    print("\nData generation complete! Files saved to data/raw/")

if __name__ == "__main__":
    main()

