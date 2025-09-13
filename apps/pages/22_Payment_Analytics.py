import os, sys, datetime as dt
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Payment Analytics", layout="wide")
st.title("Payment Analytics Dashboard")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-1), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def payment_mix(start: str, end: str):
    sql = """
      SELECT d.year, payment_method, SUM(revenue) AS revenue
      FROM transactions t JOIN time_dimension d ON d.date_key=t.date_key
      WHERE d.date BETWEEN %s AND %s AND payment_method IS NOT NULL
      GROUP BY d.year, payment_method ORDER BY d.year
    """
    return read_sql(sql, [start, end])

pm = payment_mix(start_s, end_s)
if pm.empty:
    st.warning("No payment data available.")
else:
    total = pm.groupby('year')['revenue'].transform('sum')
    pm['share'] = pm['revenue']/total
    st.plotly_chart(px.area(pm, x='year', y='share', color='payment_method', title='Payment Method Share by Year'), use_container_width=True)

st.info("If you capture success/failure per payment, we can add authorization success rates and retry funnels.")

