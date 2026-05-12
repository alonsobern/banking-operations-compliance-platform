import pandas as pd
import os

DATA_RAW_DIR = "data/raw"

def validate_files():
    files = ["branches.csv", "customers.csv", "accounts.csv", "transactions.csv", "compliance_flags.csv"]
    all_pass = True
    
    for f in files:
        path = os.path.join(DATA_RAW_DIR, f)
        if not os.path.exists(path):
            print(f"FAILED: {f} missing.")
            all_pass = False
            continue
            
        df = pd.DataFrame()
        try:
            df = pd.read_csv(path)
            print(f"PASSED: {f} loaded with {len(df)} rows.")
        except Exception as e:
            print(f"FAILED: Could not read {f}. Error: {e}")
            all_pass = False
            
    # Foreign Key Check (Basic)
    if all_pass:
        accounts = pd.read_csv(os.path.join(DATA_RAW_DIR, "accounts.csv"))
        customers = pd.read_csv(os.path.join(DATA_RAW_DIR, "customers.csv"))
        transactions = pd.read_csv(os.path.join(DATA_RAW_DIR, "transactions.csv"))
        
        # Check if all accounts have valid customers
        missing_customers = accounts[~accounts["customer_id"].isin(customers["customer_id"])]
        if len(missing_customers) == 0:
            print("PASSED: Referential integrity check (Accounts -> Customers).")
        else:
            print(f"FAILED: {len(missing_customers)} accounts have invalid customer IDs.")
            all_pass = False
            
        # Check if all transactions have valid accounts
        missing_accounts = transactions[~transactions["account_id"].isin(accounts["account_id"])]
        if len(missing_accounts) == 0:
            print("PASSED: Referential integrity check (Transactions -> Accounts).")
        else:
            print(f"FAILED: {len(missing_accounts)} transactions have invalid account IDs.")
            all_pass = False

    if all_pass:
        print("\nAll data validation checks PASSED!")
    else:
        print("\nSome data validation checks FAILED.")

if __name__ == "__main__":
    validate_files()
