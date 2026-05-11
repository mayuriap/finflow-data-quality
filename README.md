# FinFlow Data Quality Platform

Enterprise-grade data pipeline testing framework built on synthetic 
financial transaction data.

## Architecture
Raw CSV/JSONL (Kafka simulated)
↓
PySpark Bronze ETL (DQ flags + audit columns)
↓
Great Expectations Validation (Bronze layer)
↓
dbt Silver + Gold models (Snowflake)
↓
pytest unit + integration tests
↓
GitHub Actions CI/CD



## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | PySpark 3.5 |
| Data Platform | Snowflake |
| Transformation | dbt Core |
| Data Quality | Great Expectations |
| Testing | pytest |
| CI/CD | GitHub Actions |

## Quick Start

### 1. Prerequisites
- Python 3.11
- Java 11
- Git

### 2. Setup
```bash
git clone https://github.com/mayuriap/finflow-data-quality.git
cd finflow-data-quality
py -3.11 -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 3. Generate data
```bash
python scripts/generate_data.py --records 10000
```

### 4. Run Bronze ETL
```bash
python ingestion/etl_pipeline.py
```

### 5. Run GE Validation
```bash
python scripts/run_ge_validation.py
```

## Results

Bronze validation: **18/18 checks passing**
- 9,990 transactions across 4 monthly partitions
- DQ flags correctly identifying ~3% null amounts, 
  ~2% negative amounts, ~1% invalid currencies

## Project Structure
finflow-data-quality/
├── ingestion/          # PySpark Bronze ETL
├── scripts/            # Data generation + GE validation
├── dbt_project/        # dbt Bronze/Silver/Gold models
├── tests/              # pytest unit + integration tests
└── .github/workflows/  # GitHub Actions CI/CD