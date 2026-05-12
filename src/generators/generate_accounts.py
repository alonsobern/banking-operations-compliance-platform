"""
Generate synthetic bank accounts.
Produces: data/raw/accounts.csv

ANALYTICS NOTE:
Account data is the core entity in banking analytics. It links customers to transactions
and drives key operational metrics:
  - Average balance by account type (measures product profitability)
  - Account churn rate (Closed/Inactive accounts = potential attrition signal)
  - Branch distribution analysis (which branches hold the most assets?)
  - Overdraft utilisation (early indicator of customer financial stress)
"""
import random
import numpy as np
import pandas as pd
from faker import Faker
from config import DATA_RAW_DIR, NUM_ACCOUNTS, RANDOM_SEED, ensure_dirs

fake = Faker()
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

ACCOUNT_TYPES = ["Checking", "Savings", "Fixed Deposit", "Current", "Money Market"]

# COMPLIANCE NOTE: Account status affects transaction monitoring rules.
# Frozen accounts that still process payments are a critical compliance alert —
# it indicates either a system control failure or deliberate policy bypass.
# Closed accounts with activity = potential fraud or data integrity issue.
ACCOUNT_STATUSES = ["Active", "Inactive", "Frozen", "Closed", "Pending Review"]

# ANALYTICS NOTE: ~75% active reflects a healthy retail bank portfolio.
# High Inactive/Closed rates signal poor customer retention or a legacy book.
# Frozen accounts are rare but important for compliance dashboards.
STATUS_WEIGHTS   = [75, 10, 5, 7, 3]

CURRENCIES       = ["USD", "EUR", "GBP", "CAD"]
# ANALYTICS NOTE: Multi-currency accounts are relevant for FX revenue tracking
# and cross-border payment compliance (SWIFT gpi monitoring).
CURRENCY_WEIGHTS = [85, 7, 5, 3]

# ANALYTICS NOTE: Interest rates reflect current market conditions per account type.
# Fixed Deposit rates are highest as the bank holds funds for a set term.
# Checking accounts offer near-zero rates — profit comes from transaction fees.
INTEREST_RATE_MAP = {
    "Checking"     : (0.01, 0.05),
    "Savings"      : (0.50, 2.50),
    "Fixed Deposit": (3.00, 5.50),
    "Current"      : (0.00, 0.02),
    "Money Market" : (1.50, 3.50),
}

# Opening balance distribution by customer segment.
# ANALYTICS NOTE: Segment-aware balances make analytics dashboards realistic.
# Business accounts hold significantly more capital than retail accounts.
BALANCE_PARAMS = {
    "Retail"  : (7.5, 1.0),    # median ~$1,800
    "Premium" : (9.5, 1.0),    # median ~$13,000
    "Business": (11.0, 1.2),   # median ~$60,000
}

# ANALYTICS NOTE: Realistic branch distribution — flagship city branches hold
# more accounts than rural or satellite branches. Skewed distribution simulates
# the organic growth pattern of a regional retail bank.
BRANCH_DISTRIBUTION_WEIGHTS = None  # Set dynamically from branches_df size


def generate_accounts(customers_df: pd.DataFrame, branches_df: pd.DataFrame) -> pd.DataFrame:
    print(f"  Generating {NUM_ACCOUNTS} accounts...")

    customer_ids       = customers_df["customer_id"].tolist()
    branch_ids         = branches_df["branch_id"].tolist()
    customer_reg_dates = dict(zip(customers_df["customer_id"], customers_df["registration_date"]))
    customer_segments  = dict(zip(customers_df["customer_id"], customers_df["customer_segment"]))

    # ANALYTICS NOTE: Branch load is skewed — larger urban branches handle more accounts.
    # We simulate this with exponentially decaying weights so the first few branches
    # in the list dominate, as a flagship HQ branch would in reality.
    n = len(branch_ids)
    branch_weights = [max(1, int(100 * (0.75 ** i))) for i in range(n)]

    records = []
    for i in range(1, NUM_ACCOUNTS + 1):
        cust_id   = random.choice(customer_ids)
        segment   = customer_segments.get(cust_id, "Retail")
        branch_id = random.choices(branch_ids, weights=branch_weights)[0]
        acc_type  = random.choice(ACCOUNT_TYPES)
        reg_date  = customer_reg_dates[cust_id]
        opened_at = fake.date_between(start_date=reg_date, end_date="today")
        low, high = INTEREST_RATE_MAP[acc_type]
        mean, sigma = BALANCE_PARAMS[segment]
        opening_bal = round(np.random.lognormal(mean=mean, sigma=sigma), 2)

        # COMPLIANCE NOTE: Some accounts are intentionally set to Inactive/Frozen/Closed
        # to simulate realistic account lifecycle. Monitoring tools should flag any
        # transaction activity on non-Active accounts for immediate investigation.
        # The higher weight for Active ensures most normal transactions have valid accounts.
        status = random.choices(ACCOUNT_STATUSES, weights=STATUS_WEIGHTS)[0]

        records.append({
            "account_id"      : f"ACC{i:09d}",
            "customer_id"     : cust_id,
            "branch_id"       : branch_id,
            "account_type"    : acc_type,
            "account_status"  : status,
            "customer_segment": segment,
            "currency"        : random.choices(CURRENCIES, weights=CURRENCY_WEIGHTS)[0],
            "opening_balance" : opening_bal,
            "current_balance" : opening_bal,
            "interest_rate"   : round(random.uniform(low, high), 2),
            "opened_at"       : opened_at,
            # ANALYTICS NOTE: Credit/overdraft limits are only relevant for certain account types.
            # These drive fee revenue analysis and credit risk dashboards.
            "credit_limit"    : round(random.uniform(500, 50_000), 2) if acc_type == "Current" else None,
            "overdraft_limit" : round(random.uniform(0, 5_000), 2) if acc_type in ("Checking", "Current") else None,
        })

    df = pd.DataFrame(records)
    out = f"{DATA_RAW_DIR}/accounts.csv"
    df.to_csv(out, index=False)
    print(f"    Saved -> {out}  ({len(df)} rows)")
    return df


if __name__ == "__main__":
    ensure_dirs()
    customers_df = pd.read_csv(f"{DATA_RAW_DIR}/customers.csv")
    branches_df  = pd.read_csv(f"{DATA_RAW_DIR}/branches.csv")
    generate_accounts(customers_df, branches_df)
