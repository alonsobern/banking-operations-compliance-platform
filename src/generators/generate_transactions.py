"""
Generate synthetic banking transactions (100,000 rows).
Produces: data/raw/transactions.csv

ANALYTICS NOTE:
Transactions are the heartbeat of banking analytics. This dataset simulates
realistic operational and compliance-relevant behaviors including:
  - Random payment failures (operational monitoring)
  - Rapid repeated transfers (AML structuring / velocity checks)
  - International transfers (cross-border payment compliance, SWIFT monitoring)
  - Transaction peak hours (capacity planning, fraud pattern detection)
  - Segment-aware transaction volumes (business vs retail behavioral differences)

Transaction mix:
  Deposit              18%
  Withdrawal           15%
  Transfer (Domestic)  12%
  Transfer (Intl)       5%   <- AML-relevant: cross-border flows
  Card Payment         27%
  Failed Payment        9%   <- Ops monitoring: failure rate KPI
  ATM Withdrawal        5%
  Rapid Transfer Burst  4%   <- AML: structuring / velocity pattern
  Suspicious           5%   <- High-value anomalous transactions
"""
import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
from config import DATA_RAW_DIR, NUM_TRANSACTIONS, START_DATE, END_DATE, RANDOM_SEED, ensure_dirs

fake = Faker()
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# ── Transaction type distribution ─────────────────────────────────────────────
TX_TYPES   = [
    "Deposit", "Withdrawal", "Transfer", "International Transfer",
    "Card Payment", "Failed Payment", "ATM Withdrawal",
    "Rapid Transfer Burst", "Suspicious"
]
TX_WEIGHTS = [18, 15, 12, 5, 27, 9, 5, 4, 5]

# ── Payment methods ───────────────────────────────────────────────────────────
PAYMENT_METHOD_MAP = {
    "Deposit"                : ["Bank Transfer", "Cash", "Direct Deposit", "Cheque", "ACH"],
    "Withdrawal"             : ["Cash", "Bank Transfer"],
    "Transfer"               : ["Bank Transfer", "ACH", "Wire Transfer"],
    "International Transfer" : ["SWIFT", "Wire Transfer", "SEPA"],
    "Card Payment"           : ["Visa Debit", "Mastercard Debit", "Visa Credit", "Mastercard Credit", "Amex"],
    "Failed Payment"         : ["Visa Debit", "Mastercard Debit", "Bank Transfer", "ACH", "Visa Credit"],
    "ATM Withdrawal"         : ["Visa Debit", "Mastercard Debit"],
    "Rapid Transfer Burst"   : ["Bank Transfer", "ACH", "Wire Transfer"],
    "Suspicious"             : ["Cash", "Wire Transfer", "SWIFT", "Crypto Exchange"],
}

# ── Merchant categories ───────────────────────────────────────────────────────
MERCHANT_CATEGORIES = [
    "Groceries", "Restaurant", "Fuel Station", "Online Retail", "Travel",
    "Entertainment", "Healthcare", "Utilities", "Electronics", "Clothing",
    "Real Estate", "Financial Services", "Education", "Gambling", "Crypto Exchange",
    "Charity", "Government", "Automotive", "Subscription Services", "ATM",
    "Jewellery", "Money Services Business", "Pawn Shop",
]

# COMPLIANCE NOTE: Money Services Businesses, Pawn Shops, and Jewellers are
# FATF-designated high-risk merchant categories. Transactions to these categories
# require enhanced scrutiny in a real AML monitoring system.
HIGH_RISK_MERCHANTS = {"Gambling", "Crypto Exchange", "Money Services Business", "Pawn Shop"}

# ── Channels ──────────────────────────────────────────────────────────────────
CHANNELS        = ["Online Banking", "Mobile App", "ATM", "Branch", "POS Terminal", "Phone Banking"]
CHANNEL_WEIGHTS = [30, 35, 12, 8, 12, 3]

# ── Failure reasons ───────────────────────────────────────────────────────────
# ANALYTICS NOTE: Failure reason distribution is key for operational dashboards.
# High "Insufficient Funds" rate → credit risk signal.
# High "Fraud Block" rate → potential fraud wave or false-positive alert tuning needed.
FAILURE_REASONS = [
    "Insufficient Funds", "Insufficient Funds", "Insufficient Funds",   # weighted 3x — most common
    "Card Expired", "Incorrect PIN", "Fraud Block",
    "Daily Limit Exceeded", "Inactive Account", "Network Error",
    "Invalid Account Number", "Beneficiary Bank Unreachable",
]

# ── Status map ────────────────────────────────────────────────────────────────
TX_STATUS_MAP = {
    "Deposit"                : "Completed",
    "Withdrawal"             : "Completed",
    "Transfer"               : "Completed",
    "International Transfer" : "Completed",
    "Card Payment"           : "Completed",
    "Failed Payment"         : "Failed",
    "ATM Withdrawal"         : "Completed",
    "Rapid Transfer Burst"   : "Completed",
    "Suspicious"             : "Under Review",
}

# ── Countries for international transfers ─────────────────────────────────────
# COMPLIANCE NOTE: Certain destination countries are FATF grey/blacklisted.
# Flagging transactions to high-risk jurisdictions is a core BSA/AML requirement.
INTL_COUNTRIES_NORMAL   = ["United Kingdom", "Canada", "Germany", "France", "Australia",
                            "Japan", "Singapore", "Netherlands", "Switzerland", "UAE"]
INTL_COUNTRIES_HIGH_RISK= ["Nigeria", "Pakistan", "Myanmar", "Iran", "Venezuela",
                            "Russia", "North Korea", "Syria", "Cuba", "Libya"]
# 85% to normal countries, 15% to high-risk — realistic split for a US retail bank
INTL_COUNTRY_WEIGHTS    = [85, 15]

# ── Peak hour model ───────────────────────────────────────────────────────────
# ANALYTICS NOTE: Transaction volume follows predictable daily patterns in retail banking:
#   - Morning spike (8-10am): bill payments, payroll credits hitting accounts
#   - Lunch spike (12-2pm): card payments, ATM withdrawals
#   - Evening peak (5-7pm): online shopping, transfers after work hours
#   - Overnight trough (12am-6am): automated batch payments only
# Peak hour data enables capacity planning and anomaly detection
# (unusual 3am transfer volumes can indicate fraud or system abuse).
HOUR_WEIGHTS = [
    1, 1, 1, 1, 1, 2,       # 0-5am  — low, mostly automated batch
    4, 6,                   # 6-7am  — early risers / batch jobs
    9, 10, 8,               # 8-10am — morning spike
    7, 9, 9,                # 11am-1pm — lunch peak
    8, 7,                   # 2-3pm  — afternoon steady
    6, 6,                   # 4-5pm  — pre-evening
    9, 10,                  # 6-7pm  — after-work peak
    8, 7,                   # 8-9pm  — evening wind-down
    5, 3,                   # 10-11pm — late evening
]  # 24 weights


def _peak_hour_timestamp(start: datetime, end: datetime) -> datetime:
    """Generate a timestamp biased towards realistic banking peak hours."""
    delta = end - start
    total_seconds = int(delta.total_seconds())
    if total_seconds <= 0:
        return start
    random_seconds = random.randint(0, total_seconds)
    ts   = start + timedelta(seconds=random_seconds)
    # Override just the hour using peak-hour weights
    hour = random.choices(range(24), weights=HOUR_WEIGHTS)[0]
    return ts.replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59))


def _sample_amount(tx_type: str, segment: str = "Retail") -> float:
    """
    Sample transaction amount based on type and customer segment.

    ANALYTICS NOTE: Amount distributions are log-normal to reflect the real-world
    long tail of banking transactions (most are small; a few are very large).
    Segment multipliers ensure Business customers have proportionally larger transactions.
    """
    segment_multiplier = {"Retail": 1.0, "Premium": 3.5, "Business": 15.0}.get(segment, 1.0)

    if tx_type == "Deposit":
        base = np.random.lognormal(mean=6.2, sigma=1.0)        # ~$490 median
    elif tx_type in ("Withdrawal", "ATM Withdrawal"):
        base = np.random.lognormal(mean=4.3, sigma=0.8)        # ~$74 median
    elif tx_type in ("Transfer", "Rapid Transfer Burst"):
        base = np.random.lognormal(mean=6.8, sigma=1.1)        # ~$898 median
    elif tx_type == "International Transfer":
        base = np.random.lognormal(mean=7.5, sigma=1.3)        # ~$1,808 median — intl transfers are larger
    elif tx_type == "Card Payment":
        base = np.random.lognormal(mean=4.0, sigma=1.0)        # ~$55 median
    elif tx_type == "Failed Payment":
        base = np.random.lognormal(mean=4.8, sigma=1.1)        # ~$121 median
    elif tx_type == "Suspicious":
        # COMPLIANCE NOTE: Suspicious transactions are intentionally near or above the
        # $10,000 BSA reporting threshold to simulate structuring and large-cash scenarios.
        base = random.uniform(9_000, 500_000)
    else:
        base = random.uniform(1, 500)

    return round(base * segment_multiplier, 2)


def _inject_rapid_burst(account_id: str, burst_start: datetime, all_accounts: list,
                        segment: str) -> list:
    """
    Generate 3-8 rapid successive transfers from the same account within 1-2 hours.

    COMPLIANCE NOTE: Rapid repeated transfers from a single account are a textbook
    'velocity' AML alert pattern. It can indicate:
      - Structuring: splitting large amounts into smaller ones to avoid reporting thresholds
      - Account takeover: attacker draining funds as quickly as possible
      - Mule account activity: funds being layered through intermediary accounts
    Real-time transaction monitoring systems (e.g., FICO TONBELLER, Actimize) flag
    accounts exceeding N transfers within T minutes as a high-priority alert.
    """
    burst_count = random.randint(3, 8)
    burst_records = []
    for j in range(burst_count):
        tx_time = burst_start + timedelta(minutes=random.randint(1, 90))
        counterparty = random.choice([a for a in all_accounts if a != account_id])
        burst_records.append({
            "transaction_type"     : "Rapid Transfer Burst",
            "account_id"           : account_id,
            "transaction_timestamp": tx_time.isoformat(),
            "transaction_date"     : tx_time.date(),
            "transaction_time"     : tx_time.strftime("%H:%M:%S"),
            "amount"               : _sample_amount("Rapid Transfer Burst", segment),
            "currency"             : "USD",
            "direction"            : "Debit",
            "payment_method"       : random.choice(PAYMENT_METHOD_MAP["Rapid Transfer Burst"]),
            "channel"              : "Online Banking",
            "merchant_name"        : "N/A",
            "merchant_category"    : "Financial Services",
            "counterparty_account" : counterparty,
            "destination_country"  : "USA",
            "status"               : "Completed",
            "failure_reason"       : None,
            "reference_number"     : fake.bothify(text="REF-########"),
            "is_flagged"           : True,  # All burst transfers are pre-flagged
            "notes"                : f"Rapid transfer burst — {burst_count} transfers within 90 minutes.",
        })
    return burst_records


def generate_transactions(accounts_df: pd.DataFrame) -> pd.DataFrame:
    print(f"  Generating {NUM_TRANSACTIONS:,} transactions...")

    # ANALYTICS NOTE: Only active accounts should drive the majority of transactions.
    # Non-active account transactions are injected deliberately as anomalies (see below).
    active_accounts  = accounts_df[accounts_df["account_status"] == "Active"]["account_id"].tolist()
    all_accounts     = accounts_df["account_id"].tolist()
    account_segments = dict(zip(accounts_df["account_id"],
                                accounts_df.get("customer_segment", pd.Series(["Retail"] * len(accounts_df)))))
    account_statuses = dict(zip(accounts_df["account_id"], accounts_df["account_status"]))
    account_open_dates = dict(zip(accounts_df["account_id"], pd.to_datetime(accounts_df["opened_at"])))

    # COMPLIANCE NOTE: A small fraction of transactions (~2%) are deliberately routed
    # through inactive/frozen/closed accounts to simulate control failures.
    # In production, these should be caught by pre-authorisation checks and flagged immediately.
    non_active_accounts = [a for a in all_accounts if a not in active_accounts]

    records = []
    tx_counter = 1

    for i in range(1, NUM_TRANSACTIONS + 1):
        tx_type = random.choices(TX_TYPES, weights=TX_WEIGHTS)[0]

        # COMPLIANCE NOTE: ~2% of transactions target inactive accounts to simulate
        # dormant account reactivation — a known money laundering typology.
        if tx_type in ("Failed Payment", "Suspicious") and non_active_accounts and random.random() < 0.15:
            account_id = random.choice(non_active_accounts)
        elif tx_type == "Rapid Transfer Burst":
            account_id = random.choice(active_accounts) if active_accounts else random.choice(all_accounts)
        else:
            account_id = random.choice(active_accounts) if active_accounts else random.choice(all_accounts)

        segment   = account_segments.get(account_id, "Retail")
        acc_status = account_statuses.get(account_id, "Active")
        amount    = _sample_amount(tx_type, segment)
        channel   = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]
        method    = random.choice(PAYMENT_METHOD_MAP[tx_type])
        status    = TX_STATUS_MAP[tx_type]
        direction = "Credit" if tx_type == "Deposit" else "Debit"

        # ANALYTICS NOTE: Merchant data only makes sense for card-based or point-of-sale transactions.
        # Setting it to N/A for bank transfers keeps the data clean and query-efficient.
        merchant     = fake.company() if tx_type in ("Card Payment", "Suspicious") else "N/A"
        merchant_cat = random.choice(MERCHANT_CATEGORIES) if tx_type in ("Card Payment", "Suspicious") else "N/A"

        # COMPLIANCE NOTE: International transfers require destination country capture.
        # This field drives geographic risk scoring — high-risk jurisdictions trigger
        # enhanced review workflows under OFAC and FATF screening requirements.
        if tx_type == "International Transfer":
            pool = random.choices(
                [INTL_COUNTRIES_NORMAL, INTL_COUNTRIES_HIGH_RISK],
                weights=INTL_COUNTRY_WEIGHTS
            )[0]
            dest_country = random.choice(pool)
        elif tx_type == "Suspicious":
            # Suspicious transactions occasionally go international to simulate layering
            dest_country = random.choice(INTL_COUNTRIES_HIGH_RISK) if random.random() < 0.4 else "USA"
        else:
            dest_country = "USA"

        # COMPLIANCE NOTE: Random payment failures simulate real operational conditions.
        # A healthy payment system has a failure rate of <2% for normal card payments.
        # Spikes in failure rates (e.g., 10%+ on a given hour) indicate fraud waves,
        # system outages, or coordinated card testing attacks.
        failure_reason = random.choice(FAILURE_REASONS) if tx_type == "Failed Payment" else None

        # COMPLIANCE NOTE: is_flagged is pre-set for known suspicious types.
        # Additional flags are raised by compliance_flags generator post-hoc.
        is_flagged = (
            tx_type == "Suspicious"
            or amount > 10_000
            or dest_country in INTL_COUNTRIES_HIGH_RISK
            or acc_status in ("Frozen", "Closed", "Inactive")
            or merchant_cat in HIGH_RISK_MERCHANTS
        )

        # Generate timestamp with peak-hour bias
        acc_opened = account_open_dates.get(account_id, START_DATE)
        if hasattr(acc_opened, "to_pydatetime"):
            acc_opened = acc_opened.to_pydatetime()
        elif not isinstance(acc_opened, datetime):
            acc_opened = datetime(acc_opened.year, acc_opened.month, acc_opened.day)
        effective_start = max(acc_opened, START_DATE)
        if effective_start >= END_DATE:
            effective_start = START_DATE

        # ANALYTICS NOTE: Rapid burst transactions get their own burst injector
        # which creates multiple linked records with tight timestamp spacing.
        if tx_type == "Rapid Transfer Burst":
            burst_ts = _peak_hour_timestamp(effective_start, END_DATE)
            burst_records = _inject_rapid_burst(account_id, burst_ts, all_accounts, segment)
            for rec in burst_records:
                rec["transaction_id"] = f"TX{tx_counter:011d}"
                tx_counter += 1
                records.append(rec)
            continue

        tx_timestamp = _peak_hour_timestamp(effective_start, END_DATE)
        counterparty = (random.choice([a for a in all_accounts if a != account_id])
                        if tx_type in ("Transfer", "International Transfer") else None)

        records.append({
            "transaction_id"       : f"TX{tx_counter:011d}",
            "account_id"           : account_id,
            "transaction_type"     : tx_type,
            "transaction_date"     : tx_timestamp.date(),
            "transaction_time"     : tx_timestamp.strftime("%H:%M:%S"),
            "transaction_timestamp": tx_timestamp.isoformat(),
            "amount"               : amount,
            "currency"             : "USD",
            "direction"            : direction,
            "payment_method"       : method,
            "channel"              : channel,
            "merchant_name"        : merchant,
            "merchant_category"    : merchant_cat,
            "counterparty_account" : counterparty,
            "destination_country"  : dest_country,
            "customer_segment"     : segment,
            "account_status_at_tx" : acc_status,   # snapshot of account status at transaction time
            "status"               : status,
            "failure_reason"       : failure_reason,
            "reference_number"     : fake.bothify(text="REF-########"),
            "is_flagged"           : is_flagged,
            "notes"                : (
                "Transaction on non-active account — requires immediate review."
                if acc_status in ("Frozen", "Closed", "Inactive") else
                "High-value transaction — AML review required."
                if tx_type == "Suspicious" else
                f"International transfer to high-risk jurisdiction: {dest_country}."
                if dest_country in INTL_COUNTRIES_HIGH_RISK else None
            ),
        })
        tx_counter += 1

        if i % 10_000 == 0:
            print(f"    ... {i:,} / {NUM_TRANSACTIONS:,} processed")

    df = pd.DataFrame(records)
    df = df.sort_values("transaction_timestamp").reset_index(drop=True)
    out = f"{DATA_RAW_DIR}/transactions.csv"
    df.to_csv(out, index=False)
    print(f"    Saved -> {out}  ({len(df):,} rows)")
    return df


if __name__ == "__main__":
    ensure_dirs()
    accounts_df = pd.read_csv(f"{DATA_RAW_DIR}/accounts.csv")
    generate_transactions(accounts_df)
