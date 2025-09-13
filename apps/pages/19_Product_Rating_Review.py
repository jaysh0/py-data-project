import os, sys, datetime as dt
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Product Rating & Reviews", layout="wide")
st.title("Product Rating & Review Dashboard")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-1), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def rating_distribution(start: str, end: str):
    sql = """
      SELECT COALESCE(p.category, t.category, 'Unknown') AS category,
             AVG(t.customer_rating) AS avg_rating,
             COUNT(*) AS cnt
      FROM transactions t LEFT JOIN products p ON p.product_id=t.product_id
      JOIN time_dimension d ON d.date_key=t.date_key
      WHERE d.date BETWEEN %s AND %s AND t.customer_rating IS NOT NULL
      GROUP BY 1 ORDER BY 2 DESC
    """
    return read_sql(sql, [start, end])

rd = rating_distribution(start_s, end_s)
if not rd.empty:
    st.plotly_chart(px.bar(rd, x='category', y='avg_rating', hover_data=['cnt'], title='Average Rating by Category'), use_container_width=True)

st.info("Review sentiment requires a reviews table with text. If you provide that, we can add NLP sentiment and correlate with ratings and sales.")

