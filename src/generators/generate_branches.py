"""
Generate synthetic bank branches.
Produces: data/raw/branches.csv
"""
import random
import pandas as pd
from faker import Faker
from config import DATA_RAW_DIR, NUM_BRANCHES, RANDOM_SEED, ensure_dirs

fake = Faker()
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

BRANCH_TYPES = ["Retail", "Corporate", "Wealth Management", "Digital Hub", "Community"]


def generate_branches() -> pd.DataFrame:
    print(f"  Generating {NUM_BRANCHES} branches...")
    records = []
    for i in range(1, NUM_BRANCHES + 1):
        city = fake.city()
        records.append({
            "branch_id"   : f"BR{i:03d}",
            "branch_name" : f"{city} {random.choice(['Main', 'North', 'South', 'East', 'West', 'Central'])} Branch",
            "city"        : city,
            "state"       : fake.state_abbr(),
            "zip_code"    : fake.zipcode(),
            "phone"       : fake.phone_number(),
            "branch_type" : random.choice(BRANCH_TYPES),
            "opened_date" : fake.date_between(start_date="-15y", end_date="-1y"),
            "is_active"   : random.choices([True, False], weights=[95, 5])[0],
        })
    df = pd.DataFrame(records)
    out = f"{DATA_RAW_DIR}/branches.csv"
    df.to_csv(out, index=False)
    print(f"    Saved -> {out}  ({len(df)} rows)")
    return df


if __name__ == "__main__":
    ensure_dirs()
    generate_branches()

