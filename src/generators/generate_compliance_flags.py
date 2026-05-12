"""
Generate compliance flag events from the enhanced transactions dataset.
Produces: data/raw/compliance_flags.csv

COMPLIANCE NOTE:
This module simulates a rules-based AML/Compliance monitoring engine.
Real banks use systems like FICO TONBELLER, Oracle Mantas, or Actimize to run
similar rules continuously. Each rule maps to a real regulatory requirement:

  RULE-001  Large Cash Transaction        BSA Section 5313 — CTR filing required >$10k
  RULE-002  Structuring Pattern           BSA Section 5324 — illegal to structure to avoid CTR
  RULE-003  High Frequency Activity       FATF Recommendation 20 — velocity monitoring
  RULE-004  Suspicious Transaction Type   Internal SAR (Suspicious Activity Report) trigger
  RULE-005  High-Risk Merchant Category   FATF typologies — MSBs, Gambling, Crypto
  RULE-006  Failed Payment Burst          Card testing / account takeover pattern
  RULE-007  International High-Risk       OFAC/FATF — transfers to sanctioned/grey-list countries
  RULE-008  Non-Active Account Activity   Internal control — frozen/closed account transactions
"""
import random
import pandas as pd
from datetime import timedelta
from faker import Faker
from config import DATA_RAW_DIR, RANDOM_SEED, ensure_dirs

fake = Faker()
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

FLAG_STATUSES  = ["Pending Review", "Under Investigation", "Cleared", "Escalated", "Reported to FinCEN"]
STATUS_WEIGHTS = [30, 25, 20, 15, 10]

RULE_META = {
    "RULE-001": {"description": "Large Cash Transaction (>$10,000 — BSA CTR threshold)",          "base_risk": "High"},
    "RULE-002": {"description": "Structuring Pattern — near-threshold deposits within 24h",        "base_risk": "Critical"},
    "RULE-003": {"description": "High Frequency Activity (10+ txns/day on same account)",          "base_risk": "Medium"},
    "RULE-004": {"description": "Suspicious Transaction Type flagged by system",                    "base_risk": "High"},
    "RULE-005": {"description": "High-Risk Merchant Category (Gambling/Crypto/MSB/Pawn)",          "base_risk": "Medium"},
    "RULE-006": {"description": "Failed Payment Burst (3+ failures within 1 hour)",                "base_risk": "Low"},
    "RULE-007": {"description": "International Transfer to High-Risk Jurisdiction (FATF/OFAC)",    "base_risk": "Critical"},
    "RULE-008": {"description": "Transaction on Non-Active Account (Frozen/Closed/Inactive)",      "base_risk": "High"},
}

HIGH_RISK_COUNTRIES = {
    "Nigeria", "Pakistan", "Myanmar", "Iran", "Venezuela",
    "Russia", "North Korea", "Syria", "Cuba", "Libya"
}
HIGH_RISK_MERCHANTS = {"Gambling", "Crypto Exchange", "Money Services Business", "Pawn Shop"}


def _assign_risk_score(base_risk: str, amount: float, segment: str = "Retail") -> int:
    """
    Compute a 0-100 risk score.

    ANALYTICS NOTE: Risk scores enable prioritised alert queuing — compliance analysts
    review Critical/High scores first. Scores incorporate both the rule severity
    and transaction amount (a $500k suspicious transfer scores higher than a $10,001 one).
    """
    base = {"Low": 20, "Medium": 45, "High": 70, "Critical": 88}[base_risk]
    if amount > 50_000:  base = min(base + 8, 100)
    if amount > 100_000: base = min(base + 5, 100)
    if amount > 250_000: base = min(base + 5, 100)
    # Business segment gets a slight score reduction — larger transactions are expected
    if segment == "Business": base = max(base - 5, 10)
    return min(base + random.randint(-4, 4), 100)


def _build_flag(row, rule_id: str) -> dict:
    """Assemble a single flag record from a transaction row and rule ID."""
    meta   = RULE_META[rule_id]
    segment = row.get("customer_segment", "Retail")
    flagged_at = pd.to_datetime(row["transaction_timestamp"]) + timedelta(minutes=random.randint(1, 90))
    status = random.choices(FLAG_STATUSES, weights=STATUS_WEIGHTS)[0]
    return {
        "transaction_id"   : row["transaction_id"],
        "account_id"       : row["account_id"],
        "rule_id"          : rule_id,
        "flag_reason"      : meta["description"],
        "risk_level"       : meta["base_risk"],
        "risk_score"       : _assign_risk_score(meta["base_risk"], row["amount"], segment),
        "customer_segment" : segment,
        "status"           : status,
        "analyst_assigned" : fake.name() if random.random() > 0.25 else None,
        "amount"           : row["amount"],
        "destination_country": row.get("destination_country", "USA"),
        "flagged_at"       : flagged_at,
        "resolved_at"      : (flagged_at + timedelta(hours=random.randint(1, 240))
                              if status in ("Cleared", "Reported to FinCEN") else None),
        "notes"            : meta["description"],
    }


# ── Rule implementations ──────────────────────────────────────────────────────

def apply_rule_001(tx_df: pd.DataFrame) -> list:
    """Large cash transactions > $10,000 — BSA Currency Transaction Report threshold."""
    flagged = tx_df[tx_df["amount"] > 10_000]
    return [_build_flag(row, "RULE-001") for _, row in flagged.iterrows()]


def apply_rule_002(tx_df: pd.DataFrame) -> list:
    """
    Structuring: 3+ deposits between $8,000-$9,999 on same account within 24h.

    COMPLIANCE NOTE: Structuring is a federal crime under 31 USC 5324.
    Breaking large amounts into sub-$10k chunks to avoid CTR filing is a
    primary indicator of money laundering. Detection requires sliding window analysis.
    """
    deposits = tx_df[
        (tx_df["transaction_type"] == "Deposit") &
        (tx_df["amount"].between(8_000, 9_999))
    ].copy()
    deposits["tx_dt"] = pd.to_datetime(deposits["transaction_timestamp"])

    flags = []
    flagged_ids = set()
    for acct, grp in deposits.groupby("account_id"):
        grp = grp.sort_values("tx_dt")
        for idx, row in grp.iterrows():
            window = grp[
                (grp["tx_dt"] >= row["tx_dt"]) &
                (grp["tx_dt"] <= row["tx_dt"] + timedelta(hours=24))
            ]
            if len(window) >= 3:
                for w_idx, w_row in window.iterrows():
                    key = (w_row["transaction_id"], "RULE-002")
                    if key not in flagged_ids:
                        flags.append(_build_flag(w_row, "RULE-002"))
                        flagged_ids.add(key)
    return flags


def apply_rule_003(tx_df: pd.DataFrame) -> list:
    """High frequency: 10+ transactions per account per calendar day."""
    tx_df = tx_df.copy()
    tx_df["tx_date"] = pd.to_datetime(tx_df["transaction_date"])
    counts = tx_df.groupby(["account_id", "tx_date"]).size().reset_index(name="count")
    high_freq = counts[counts["count"] >= 10]
    if high_freq.empty:
        return []
    merged = tx_df.merge(high_freq[["account_id", "tx_date"]], on=["account_id", "tx_date"])
    return [_build_flag(row, "RULE-003") for _, row in merged.iterrows()]


def apply_rule_004(tx_df: pd.DataFrame) -> list:
    """System-flagged suspicious transactions and Rapid Transfer Bursts."""
    flagged = tx_df[tx_df["transaction_type"].isin(["Suspicious", "Rapid Transfer Burst"])]
    return [_build_flag(row, "RULE-004") for _, row in flagged.iterrows()]


def apply_rule_005(tx_df: pd.DataFrame) -> list:
    """High-risk merchant category transactions."""
    flagged = tx_df[tx_df["merchant_category"].isin(HIGH_RISK_MERCHANTS)]
    return [_build_flag(row, "RULE-005") for _, row in flagged.iterrows()]


def apply_rule_006(tx_df: pd.DataFrame) -> list:
    """
    Failed payment burst: 3+ failures within 1 hour per account.

    COMPLIANCE NOTE: Rapid card failures are a primary indicator of card testing —
    fraudsters validate stolen card numbers with small purchases, then use successful
    cards for high-value fraud. Real-time detection must block accounts after N failures.
    """
    failures = tx_df[tx_df["transaction_type"] == "Failed Payment"].copy()
    failures["tx_dt"] = pd.to_datetime(failures["transaction_timestamp"])
    flags = []
    flagged_ids = set()
    for acct, grp in failures.groupby("account_id"):
        grp = grp.sort_values("tx_dt")
        for idx, row in grp.iterrows():
            window = grp[
                (grp["tx_dt"] >= row["tx_dt"]) &
                (grp["tx_dt"] <= row["tx_dt"] + timedelta(hours=1))
            ]
            if len(window) >= 3:
                for w_idx, w_row in window.iterrows():
                    key = (w_row["transaction_id"], "RULE-006")
                    if key not in flagged_ids:
                        flags.append(_build_flag(w_row, "RULE-006"))
                        flagged_ids.add(key)
    return flags


def apply_rule_007(tx_df: pd.DataFrame) -> list:
    """
    International transfers to FATF grey/blacklisted or OFAC-sanctioned countries.

    COMPLIANCE NOTE: Transfers to sanctioned countries (Iran, North Korea, Syria, Cuba)
    are prohibited under OFAC regulations and require immediate blocking + SAR filing.
    Transfers to FATF grey-list countries require enhanced due diligence documentation.
    """
    flagged = tx_df[
        (tx_df["transaction_type"].isin(["International Transfer", "Suspicious"])) &
        (tx_df["destination_country"].isin(HIGH_RISK_COUNTRIES))
    ]
    return [_build_flag(row, "RULE-007") for _, row in flagged.iterrows()]


def apply_rule_008(tx_df: pd.DataFrame) -> list:
    """
    Any transaction activity on Frozen, Closed, or Inactive accounts.

    COMPLIANCE NOTE: Transactions on non-active accounts indicate either:
      - System control failure (payment engine bypassed account status check)
      - Deliberate fraud (account reactivated without proper authorisation)
      - Data integrity issue (status not updated after account closure)
    All three scenarios require immediate investigation and potential system audit.
    """
    if "account_status_at_tx" not in tx_df.columns:
        return []
    flagged = tx_df[tx_df["account_status_at_tx"].isin(["Frozen", "Closed", "Inactive"])]
    return [_build_flag(row, "RULE-008") for _, row in flagged.iterrows()]


def generate_compliance_flags(transactions_df: pd.DataFrame) -> pd.DataFrame:
    print("  Generating compliance flags...")

    rule_fns = [
        ("RULE-001", apply_rule_001),
        ("RULE-002", apply_rule_002),
        ("RULE-003", apply_rule_003),
        ("RULE-004", apply_rule_004),
        ("RULE-005", apply_rule_005),
        ("RULE-006", apply_rule_006),
        ("RULE-007", apply_rule_007),
        ("RULE-008", apply_rule_008),
    ]

    all_flags = []
    for rule_id, fn in rule_fns:
        result = fn(transactions_df)
        print(f"    {rule_id}: {len(result):,} flags")
        all_flags.extend(result)

    if not all_flags:
        print("    No flags generated.")
        return pd.DataFrame()

    df = pd.DataFrame(all_flags)
    # Deduplicate on (transaction_id, rule_id) — same transaction can fire multiple rules
    df = df.drop_duplicates(subset=["transaction_id", "rule_id"]).reset_index(drop=True)
    df.insert(0, "flag_id", [f"FLG{i+1:08d}" for i in range(len(df))])

    cols = ["flag_id", "transaction_id", "account_id", "rule_id", "flag_reason",
            "risk_level", "risk_score", "customer_segment", "status", "analyst_assigned",
            "amount", "destination_country", "flagged_at", "resolved_at", "notes"]
    df = df[[c for c in cols if c in df.columns]]

    out = f"{DATA_RAW_DIR}/compliance_flags.csv"
    df.to_csv(out, index=False)
    print(f"    Saved -> {out}  ({len(df):,} rows)")
    return df


if __name__ == "__main__":
    ensure_dirs()
    transactions_df = pd.read_csv(f"{DATA_RAW_DIR}/transactions.csv")
    generate_compliance_flags(transactions_df)
