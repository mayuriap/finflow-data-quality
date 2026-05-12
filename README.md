# FinFlow Data Quality Platform

[![FinFlow CI](https://github.com/mayuriap/finflow-data-quality/actions/workflows/ci.yml/badge.svg)](https://github.com/mayuriap/finflow-data-quality/actions/workflows/ci.yml)

Enterprise-grade data pipeline testing framework built on synthetic 
financial transaction data — demonstrating production-level data quality 
practices across Bronze, Silver and Gold layers.

## Results

| Layer | Status | Detail |
|---|---|---|
| Bronze ETL | ✅ | 9,990 rows ingested, DQ flags applied |
| GE Validation | ✅ | 18/18 expectations passing |
| dbt Silver | ✅ | 9,169 clean rows, 10/10 tests passing |
| dbt Gold | ✅ | Daily aggregates across 8 currencies |
| pytest | ✅ | 26/26 unit tests passing |
| CI/CD | ✅ | GitHub Actions — all jobs green |

## ArchitectureRaw CSV / JSONL (Kafka simulated)
↓
PySpark Bronze ETL
(DQ flag columns + audit metadata + partitioned output)
↓
Great Expectations Validation
(18 expectations across Bronze transactions + customers)
↓
Snowflake RAW tables
↓
dbt Silver (incremental merge, dedup, FX conversion)
↓
dbt Gold (daily aggregates, success rates, USD totals)
↓
pytest unit tests (26 tests — DQ, Silver filter, FX, dedup, Gold)
↓
GitHub Actions CI/CD (auto-runs on every push)

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | PySpark 3.5 |
| Data Platform | Snowflake |
| Transformation | dbt Core 1.7 |
| Data Quality | Great Expectations |
| Testing | pytest 7.4 |
| CI/CD | GitHub Actions |
| Language | Python 3.11 |

## Quick Start

### Prerequisites
- Python 3.11
- Java 11
- Git
- Snowflake account (free trial at snowflake.com)

### Setup
```bash
git clone https://github.com/mayuriap/finflow-data-quality.git
cd finflow-data-quality

py -3.11 -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### Run the full pipeline
```bash
# 1. Generate synthetic data
python scripts/generate_data.py --records 10000

# 2. Run Bronze ETL
python ingestion/etl_pipeline.py

# 3. Validate with Great Expectations
python scripts/run_ge_validation.py

# 4. Load to Snowflake
python scripts/load_to_snowflake.py

# 5. Run dbt models
cd dbt_project
dbt run --profiles-dir ~/.dbt
dbt test --profiles-dir ~/.dbt

# 6. Run pytest
cd ..
pytest tests/unit/ -v
```

## Project Structure
finflow-data-quality/
├── ingestion/
│   └── etl_pipeline.py        # PySpark Bronze ETL
├── scripts/
│   ├── generate_data.py       # Synthetic data generator
│   ├── run_ge_validation.py   # Great Expectations suites
│   └── load_to_snowflake.py   # Snowflake loader
├── dbt_project/
│   ├── models/
│   │   ├── bronze/            # Staging views
│   │   ├── silver/            # Incremental cleaned tables
│   │   └── gold/              # Aggregates
│   ├── macros/                # Reusable Jinja macros
│   └── tests/                 # Custom singular tests
├── tests/
│   └── unit/                  # 26 pytest unit tests
├── .github/workflows/
│   └── ci.yml                 # GitHub Actions pipeline
└── requirements.txt

## Data Quality Summary

Bronze layer intentionally contains dirty data:
- ~3% null amounts
- ~2% negative amounts  
- ~1% invalid currency codes
- ~2% null customer IDs

Silver layer filters all critical issues:
- 9,169 / 9,990 rows retained (91.8% coverage)
- 100% valid currencies, statuses, amounts
- Zero duplicates — unique transaction_id enforced