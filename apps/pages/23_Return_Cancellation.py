import os, sys, datetime as dt
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Returns & Cancellations", layout="wide")
st.title("Return & Cancellation Dashboard")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-1), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def return_rates(start: str, end: str):
    sql = """
      SELECT COALESCE(p.category, t.category, 'Unknown') AS category,
             AVG(CASE WHEN t.is_returned THEN 1.0 ELSE 0.0 END) AS return_rate,
             SUM(t.revenue) AS revenue,
             SUM(t.quantity) AS units
      FROM transactions t LEFT JOIN products p ON p.product_id=t.product_id
      JOIN time_dimension d ON d.date_key=t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY 1 ORDER BY 2 DESC
    """
    return read_sql(sql, [start, end])

rr = return_rates(start_s, end_s)
if not rr.empty:
    st.plotly_chart(px.bar(rr, x='category', y='return_rate', title='Return Rate by Category'), use_container_width=True)
    st.dataframe(rr.sort_values('return_rate', ascending=False).head(50))

st.info("Cancellation rates and reasons require additional fields. If you collect them, we can include reason codes and cost impact.")

