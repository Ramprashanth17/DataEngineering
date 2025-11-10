# NYC 311 Data Pipeline

A production-grade data pipeline processing NYC 311 service requests using modern data engineering tools.

## Architecture
```
NYC Open Data API → Python → Snowflake (RAW) → dbt (STAGING/MARTS) → Tableau/PowerBI
                              ↑
                         Orchestrated by Airflow
```

## Tech Stack
- **Data Warehouse:** Snowflake
- **Transformation:** dbt
- **Orchestration:** Airflow
- **Language:** Python 3.x

## Project Status
- [x] Initial setup and Snowflake connection
- [ ] Data ingestion from NYC 311 API
- [ ] dbt staging models
- [ ] Kimball star schema (fact & dimensions)
- [ ] Airflow DAG orchestration

## Quick Start
```bash
# Setup
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure credentials
cp .env.example .env  # Edit with your Snowflake credentials

# Test connection
python scripts/test_snowflake.py
```

## Contact
Part of my Data Engineering learning journey. Connect with me on LinkedIn!
