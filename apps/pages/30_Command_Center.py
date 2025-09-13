import os, sys, datetime as dt
import streamlit as st
import pandas as pd

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Command Center", layout="wide")
st.title("Business Intelligence Command Center")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-1), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=60)
def summary(start: str, end: str):
    sql = """
      SELECT SUM(revenue) AS revenue,
             COUNT(*) AS orders,
             COUNT(DISTINCT customer_id) AS customers
      FROM transactions t JOIN time_dimension d ON d.date_key=t.date_key
      WHERE d.date BETWEEN %s AND %s
    """
    return read_sql(sql, [start, end]).iloc[0]

s = summary(start_s, end_s)
col1, col2, col3 = st.columns(3)
col1.metric("Revenue", f"₹{(s['revenue'] or 0):,.0f}")
col2.metric("Orders", f"{int(s['orders'] or 0):,}")
col3.metric("Customers", f"{int(s['customers'] or 0):,}")

st.markdown("""
### Shortcuts
- Executive Summary
- Real-time Monitor
- Revenue Trends
- Category Performance
- Prime Analytics
- Price Optimization
""")

st.info("Add auto-alerts (email/Slack) by monitoring KPIs vs thresholds and triggering notifications.")

