"""
Main orchestrator â€” runs all data generators in dependency order.

Usage:
    cd src/generators
    python run_all.py
"""
import sys
import time

# Make sure local imports resolve
sys.path.insert(0, ".")

from config import ensure_dirs, DATA_RAW_DIR
from generate_branches         import generate_branches
from generate_customers        import generate_customers
from generate_accounts         import generate_accounts
from generate_transactions     import generate_transactions
from generate_compliance_flags import generate_compliance_flags


def main():
    print("=" * 60)
    print("  Banking Operations & Compliance Monitoring Platform")
    print("  Synthetic Data Generator â€” Full Run")
    print("=" * 60)
    ensure_dirs()
    start = time.time()

    print("\n[1/5] Branches")
    branches_df = generate_branches()

    print("\n[2/5] Customers")
    customers_df = generate_customers()

    print("\n[3/5] Accounts")
    accounts_df = generate_accounts(customers_df, branches_df)

    print("\n[4/5] Transactions")
    transactions_df = generate_transactions(accounts_df)

    print("\n[5/5] Compliance Flags")
    flags_df = generate_compliance_flags(transactions_df)

    elapsed = time.time() - start
    print("\n" + "=" * 60)
    print(f"  Generation complete in {elapsed:.1f}s")
    print(f"  Output directory: {DATA_RAW_DIR}")
    print("=" * 60)
    print("\n  Dataset Summary:")
    print(f"    Branches          : {len(branches_df):>8,}")
    print(f"    Customers         : {len(customers_df):>8,}")
    print(f"    Accounts          : {len(accounts_df):>8,}")
    print(f"    Transactions      : {len(transactions_df):>8,}")
    print(f"    Compliance Flags  : {len(flags_df):>8,}")
    print("=" * 60)


if __name__ == "__main__":
    main()

