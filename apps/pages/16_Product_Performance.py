import os, sys, datetime as dt
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Product Performance", layout="wide")
st.title("Product Performance Dashboard")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-1), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()
top_n = st.sidebar.slider("Top N", 5, 100, 20)

@st.cache_data(ttl=300)
def product_kpis(start: str, end: str):
    sql = """
      SELECT t.product_id,
             COALESCE(p.product_name, t.product_id) AS product_name,
             COALESCE(p.category, t.category, 'Unknown') AS category,
             SUM(t.revenue) AS revenue,
             SUM(t.quantity) AS units,
             AVG(t.customer_rating) AS avg_rating,
             AVG(CASE WHEN t.is_returned THEN 1.0 ELSE 0.0 END) AS return_rate
      FROM transactions t
      LEFT JOIN products p ON p.product_id = t.product_id
      JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY t.product_id, product_name, category
      ORDER BY revenue DESC
      LIMIT %s
    """
    return read_sql(sql, [start, end, top_n])

df = product_kpis(start_s, end_s)
if df.empty:
    st.warning("No data for selected period.")
    st.stop()

col1, col2 = st.columns(2)
with col1:
    st.subheader("Top Products by Revenue")
    st.dataframe(df[["product_name","category","revenue","units","avg_rating","return_rate"]])
with col2:
    st.subheader("Revenue vs Units (bubble size by rating)")
    fig = px.scatter(df, x='units', y='revenue', size='avg_rating', color='category', hover_name='product_name')
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Return Rate vs Rating")
fig2 = px.scatter(df, x='avg_rating', y='return_rate', color='category', hover_name='product_name')
st.plotly_chart(fig2, use_container_width=True)

