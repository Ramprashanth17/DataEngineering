# NYC 311 Service Requests Data Pipeline

## Project Overview
End-to-end data pipeline processing NYC 311 service requests from ingestion to analytics-ready data models.

## Architecture
```
NYC Open Data API → Python Ingestion → Snowflake RAW 
                                          ↓
                                     dbt Transformations
                                          ↓
                              STAGING → MARTS (Star Schema)
                                          ↓
                                    Tableau/Power BI
```

## Tech Stack
- **Source:** NYC Open Data API
- **Ingestion:** Python (pandas, requests)
- **Storage:** Snowflake
- **Transformation:** dbt
- **Orchestration:** Airflow (coming)
- **Visualization:** Tableau/Power BI

## Database Structure
```sql
DE_LEARNING.RAW.NYC_311          -- Raw data from API
DE_LEARNING.STAGING.stg_nyc_311  -- Cleaned & standardized
DE_LEARNING.MARTS.fact_311_requests  -- Fact table
DE_LEARNING.MARTS.dim_*          -- Dimension tables
```

## Project Status
- [x] Snowflake database setup (RAW, STAGING, MARTS schemas)
- [x] NYC_311 table created in RAW layer
- [ ] Data ingestion script
- [ ] dbt staging models
- [ ] Kimball star schema
- [ ] Airflow orchestration

## Data Model (Kimball)
Coming: Fact table with service requests, dimensions for date, agency, location, complaint type.

## Next Steps
1. Build Python ingestion script for NYC 311 API
2. Create dbt staging models for data cleaning
3. Implement Kimball dimensional model
4. Add Airflow DAG for orchestration
