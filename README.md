# Banking Operations & Compliance Monitoring Platform

This project simulates a retail banking analytics and compliance monitoring solution. It includes synthetic data generation, ETL pipelines, PostgreSQL integration, and Power BI dashboards.

## Project Structure

- `data/`: Raw and processed synthetic data.
- `src/`: Platform source code.
  - `generators/`: Synthetic data generation logic.
  - `etl/`: ETL pipelines (Future).
  - `database/`: Database schema and connection logic (Future).
  - `compliance/`: Monitoring and flagging logic (Future).
- `dashboards/`: Power BI assets.
- `notebooks/`: Analysis and EDA.
- `tests/`: Data validation and unit tests.

## Getting Started

### Prerequisites

- Python 3.8+
- PostgreSQL
- Power BI Desktop

### Installation

1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Generate synthetic data:
   ```bash
   python src/generators/banking_data_generator.py
   ```
4. Validate data:
   ```bash
   python tests/validate_data.py
   ```

## Datasets

The platform currently generates the following core datasets:
- **Branches**: Bank locations and metadata.
- **Customers**: Synthetic customer demographics.
- **Accounts**: Retail banking accounts linked to customers.
- **Transactions**: Daily banking activities (Deposits, Withdrawals, Transfers, Payments).
- **Compliance Flags**: Transactions flagged for manual review based on AML (Anti-Money Laundering) rules.

## Future Roadmap

- **Day 2**: Database Schema design and ETL process.
- **Day 3**: Compliance monitoring logic and risk scoring.
- **Day 4**: Power BI Dashboard integration.
