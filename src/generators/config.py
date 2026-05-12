"""
Shared configuration and constants for all data generators.
"""
import os
from datetime import datetime

# Paths 
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR    = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
DATA_RAW_DIR = os.path.join(ROOT_DIR, "data", "raw")

# Volume 
NUM_BRANCHES     = 20
NUM_CUSTOMERS    = 2_000
NUM_ACCOUNTS     = 3_000
NUM_TRANSACTIONS = 100_000

# Date window 
START_DATE = datetime(2024, 1, 1)
END_DATE   = datetime(2026, 5, 1)

# Seed for reproducibility 
RANDOM_SEED = 42

def ensure_dirs():
    os.makedirs(DATA_RAW_DIR, exist_ok=True)

