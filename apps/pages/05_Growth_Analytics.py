import os, sys
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Growth Analytics", layout="wide")
st.title("Growth Analytics")

@st.cache_data(ttl=300)
def new_customers_by_year():
    sql = """
      WITH first_orders AS (
        SELECT customer_id, MIN(d.date) AS first_date
        FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
        WHERE customer_id IS NOT NULL
        GROUP BY customer_id
      )
      SELECT EXTRACT(YEAR FROM first_date)::int AS year, COUNT(*) AS new_customers
      FROM first_orders GROUP BY 1 ORDER BY 1
    """
    return read_sql(sql)

@st.cache_data(ttl=300)
def product_portfolio_growth():
    sql = "SELECT launch_year AS year, COUNT(*) AS products FROM products WHERE launch_year IS NOT NULL GROUP BY launch_year ORDER BY launch_year"
    return read_sql(sql)

nc = new_customers_by_year()
if not nc.empty:
    st.plotly_chart(px.bar(nc, x='year', y='new_customers', title='New Customers by Year'), use_container_width=True)

pp = product_portfolio_growth()
if not pp.empty:
    st.plotly_chart(px.line(pp, x='year', y='products', markers=True, title='Product Portfolio Expansion'), use_container_width=True)

st.info("Add predictive models (e.g., ARIMA/Prophet) for deeper growth forecasts once data is stable.")

