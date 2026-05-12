"""
Generate synthetic bank customers.
Produces: data/raw/customers.csv

ANALYTICS NOTE:
Customer segmentation is fundamental to retail banking. It drives:
  - Risk profiling (premium/business customers carry higher transaction volumes)
  - Product eligibility (e.g., business accounts allow higher overdraft limits)
  - Compliance thresholds (business customers have different AML monitoring rules)
  - Dashboard KPIs (revenue per segment, churn rate by segment)
"""
import random
import numpy as np
import pandas as pd
from faker import Faker
from config import DATA_RAW_DIR, NUM_CUSTOMERS, START_DATE, RANDOM_SEED, ensure_dirs

fake = Faker()
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# ANALYTICS NOTE: Three-tier segmentation mirrors real retail banking models.
# Retail = mass market, Premium = high net worth individuals, Business = SME/corporate.
# Each segment has different income distributions, risk profiles, and transaction behaviors.
CUSTOMER_SEGMENTS = ["Retail", "Premium", "Business"]

# 70% retail, 20% premium, 10% business — matches typical bank portfolio distribution.
# Skewing too many customers to Business inflates average transaction values unrealistically.
SEGMENT_WEIGHTS   = [70, 20, 10]

KYC_STATUSES = ["Verified", "Pending", "Failed", "Expired"]
# COMPLIANCE NOTE: KYC (Know Your Customer) status gates transaction permissions.
# Pending/Failed/Expired customers should generate compliance flags if they transact —
# this is a core AML (Anti-Money Laundering) monitoring requirement under FATF guidelines.
KYC_WEIGHTS  = [80, 10, 5, 5]

# ANALYTICS NOTE: Annual income follows a log-normal distribution in real populations.
# Mean and sigma are tuned per segment to reflect realistic income differences.
INCOME_PARAMS = {
    "Retail"  : (10.5, 0.5),   # median ~$36k
    "Premium" : (11.5, 0.6),   # median ~$98k
    "Business": (12.5, 0.8),   # median ~$268k (business revenues, not salary)
}

# ANALYTICS NOTE: Occupation data enriches transaction pattern analysis.
# e.g., a 'Financial Analyst' making daily $50k crypto transfers is less suspicious
# than a 'Retired Teacher' doing the same — occupation is a soft risk signal.
RETAIL_OCCUPATIONS  = ["Teacher", "Nurse", "Driver", "Sales Associate", "Retail Worker",
                        "Administrative Assistant", "Student", "Retired", "Warehouse Worker"]
PREMIUM_OCCUPATIONS = ["Financial Analyst", "Physician", "Attorney", "Software Engineer",
                        "Architect", "Business Owner", "Investment Manager", "Consultant"]
BUSINESS_OCCUPATIONS= ["CEO", "CFO", "Director", "Managing Partner", "Business Owner",
                        "Sole Trader", "Company Secretary", "Operations Manager"]

OCCUPATION_MAP = {
    "Retail"  : RETAIL_OCCUPATIONS,
    "Premium" : PREMIUM_OCCUPATIONS,
    "Business": BUSINESS_OCCUPATIONS,
}


def generate_customers() -> pd.DataFrame:
    print(f"  Generating {NUM_CUSTOMERS} customers...")
    records = []
    for i in range(1, NUM_CUSTOMERS + 1):
        segment = random.choices(CUSTOMER_SEGMENTS, weights=SEGMENT_WEIGHTS)[0]
        mean, sigma = INCOME_PARAMS[segment]
        dob = fake.date_of_birth(minimum_age=18, maximum_age=85)

        records.append({
            "customer_id"      : f"C{i:07d}",
            "first_name"       : fake.first_name(),
            "last_name"        : fake.last_name(),
            "email"            : fake.unique.email(),
            "phone_number"     : fake.phone_number(),
            "date_of_birth"    : dob,
            "address"          : fake.street_address(),
            "city"             : fake.city(),
            "state"            : fake.state_abbr(),
            "zip_code"         : fake.zipcode(),
            "country"          : "USA",
            "registration_date": fake.date_between(start_date=START_DATE, end_date="today"),
            "customer_segment" : segment,
            "kyc_status"       : random.choices(KYC_STATUSES, weights=KYC_WEIGHTS)[0],
            # COMPLIANCE NOTE: is_active=False customers who still transact are an immediate red flag.
            # Dormant account reactivation with large transactions is an AML pattern.
            "is_active"        : random.choices([True, False], weights=[92, 8])[0],
            "annual_income"    : round(np.random.lognormal(mean=mean, sigma=sigma), 2),
            "occupation"       : random.choice(OCCUPATION_MAP[segment]),
            # ANALYTICS NOTE: Nationality supports geographic risk profiling.
            # Customers from high-risk jurisdictions (FATF grey/blacklist) require enhanced due diligence.
            "nationality"      : fake.country(),
        })

    df = pd.DataFrame(records)
    out = f"{DATA_RAW_DIR}/customers.csv"
    df.to_csv(out, index=False)
    print(f"    Saved -> {out}  ({len(df)} rows)")
    return df


if __name__ == "__main__":
    ensure_dirs()
    generate_customers()
