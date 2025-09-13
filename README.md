# Data Platform: Cleaning + EDA + Postgres + Streamlit BI

Production-ready scaffold to clean CSVs, explore data, load to Postgres, and visualize interactive dashboards.

## Components
- Pandas cleaning pipeline (config-driven)
- EDA (script + notebook)
- Postgres analytics schema + loaders
- Streamlit multipage BI app

## Requirements
- Python 3.10+
- Postgres 13+

## Install
- Create/activate a virtual env, then install:
  - `pip install -e .`
  - Installs: pandas, seaborn, matplotlib, psycopg2-binary, streamlit, plotly, statsmodels

## Environment
- Set Postgres DSN (example PowerShell):
  - `$env:POSTGRES_DSN = "host=localhost dbname=analytics user=postgres password=yourpass"`

## Clean Data
- Single file:
  - `python scripts\run_cleaning.py --input data\your.csv --output data\your.cleaned.csv --config configs\cleaning_transactions_amazon_india.json --report data\your.report.json`
- Batch all CSVs in `data\` to `data\cleaned\`:
  - `python scripts\batch_clean.py`

## EDA
- Notebook only (auto-locates project root):
  - Open `notebooks/eda_analysis.ipynb` and run cells

## Postgres Warehouse
- Initialize schema + time dimension (schema `analytics`):
  - `python scripts\init_db_pg.py`
- (Optional) Add `category`/`brand` columns to fact (for fallback segmentation):
  - `python scripts\migrate_add_category_brand_pg.py`
- Load products (from catalog):
  - `python scripts\load_products_pg.py`
- Load cleaned CSVs into transactions:
  - `python scripts\load_to_db_pg.py`

## Dashboards (Streamlit)
- Run app:
  - `streamlit run apps\streamlit_app.py`
- Pages include:
  - 01 Executive Summary
  - 02 Real‑time Monitor
  - 03 Strategic Overview
  - 04 Financial Performance
  - 05 Growth Analytics
  - 06 Revenue Trend Analysis
  - 07 Category Performance
  - 08 Geographic Revenue
  - 09 Festival Sales Analytics
  - 10 Price Optimization
  - 11 Customer Segmentation (RFM)
  - 12 Customer Journey
  - 13 Prime Analytics
  - 14 Customer Retention (Cohorts)
  - 15 Demographics & Behavior
  - 16 Product Performance
  - 17 Brand Analytics
  - 18 Inventory Optimization
  - 19 Product Rating & Review
  - 20 New Product Launch
  - 21 Delivery Performance
  - 22 Payment Analytics
  - 23 Return & Cancellation
  - 24 Customer Service (placeholder)
  - 25 Supply Chain (placeholder)
  - 26 Predictive Analytics (forecast)
  - 27 Market Intelligence
  - 28 Cross‑sell & Upsell
  - 29 Seasonal Planning
  - 30 Command Center

## Configuration
- Cleaning configs: `configs/cleaning_*.json`
  - `configs/cleaning_transactions_amazon_india.json` covers dates, prices, ratings, booleans, delivery, dedup, outliers, payment
- Pipelines autodetect common columns; adjust config where needed

## Repo Structure
```
src/
  data_pipeline/
    pd_pipeline.py      # Pandas cleaners + orchestration
    config.py           # Config dataclasses + loader
    db_pg_utils.py      # Postgres helpers
    bi/db.py            # Streamlit DB access wrapper
scripts/
  run_cleaning.py      # Clean single file
  batch_clean.py       # Clean all CSVs in data/ → data/cleaned/
  init_db_pg.py        # Create analytics schema + time dimension
  load_products_pg.py  # Upsert products from catalog
  migrate_add_category_brand_pg.py # Add category/brand to fact
  load_to_db_pg.py     # Load cleaned CSVs to transactions
apps/
  streamlit_app.py     # Streamlit entry
  pages/               # Multipage dashboards (01…30)
db/
  schema.postgres.sql  # DDL for analytics schema
configs/
  cleaning_*.json      # Cleaning configurations
data/
  cleaned/             # Batch output target
notebooks/
  eda_analysis.ipynb   # Interactive EDA
```

## Troubleshooting
- “Unknown” categories/brands:
  - Run `migrate_add_category_brand_pg.py`, reload with `load_to_db_pg.py`, and/or load products via `load_products_pg.py`
- COPY errors on integers (e.g., delivery_days):
  - Loader normalizes types; ensure cleaned CSVs have consistent numeric columns
- Discount bucket error:
  - Ensure “Max discount %” and “Discount buckets” sliders are > 0 in Price Optimization
