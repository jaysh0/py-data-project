import os, sys, datetime as dt
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Inventory Optimization", layout="wide")
st.title("Inventory Optimization Dashboard")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-1), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def demand_monthly(start: str, end: str):
    sql = """
      SELECT d.year, d.month, SUM(t.quantity) AS units
      FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY d.year, d.month ORDER BY d.year, d.month
    """
    return read_sql(sql, [start, end])

dm = demand_monthly(start_s, end_s)
if not dm.empty:
    dm['period'] = dm['year'].astype(str) + '-' + dm['month'].astype(str)
    st.plotly_chart(px.line(dm, x='period', y='units', title='Monthly Demand (Units)'), use_container_width=True)

st.info("For full optimization (turnover, stockouts), add an inventory table (on-hand, receipts, supplier lead times). This page shows demand patterns as a first step.")

