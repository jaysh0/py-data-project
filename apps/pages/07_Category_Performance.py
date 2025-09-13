import os, sys, datetime as dt
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Category Performance", layout="wide")
st.title("Category Performance")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-1), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def revenue_share(start: str, end: str):
    sql = """
      SELECT COALESCE(p.category, t.category, 'Unknown') AS category, SUM(t.revenue) AS revenue
      FROM transactions t LEFT JOIN products p ON p.product_id = t.product_id
      JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY 1 ORDER BY 2 DESC
    """
    return read_sql(sql, [start, end])

@st.cache_data(ttl=300)
def category_trend(start: str, end: str):
    sql = """
      SELECT d.year, COALESCE(p.category, t.category, 'Unknown') AS category, SUM(t.revenue) AS revenue
      FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
      LEFT JOIN products p ON p.product_id = t.product_id
      WHERE d.date BETWEEN %s AND %s
      GROUP BY 1, 2 ORDER BY 1
    """
    return read_sql(sql, [start, end])

rs = revenue_share(start_s, end_s)
if not rs.empty:
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(px.pie(rs, values='revenue', names='category', title='Revenue Share'), use_container_width=True)
    with col2:
        st.dataframe(rs.head(20))

ct = category_trend(start_s, end_s)
if not ct.empty:
    st.subheader("Category Growth Trends")
    fig = px.line(ct, x="year", y="revenue", color="category")
    st.plotly_chart(fig, use_container_width=True)

    # Market share change by year
    total = ct.groupby('year')['revenue'].transform('sum')
    ct['share'] = ct['revenue']/total
    fig2 = px.area(ct, x='year', y='share', color='category', groupnorm='fraction', title='Category Market Share Over Time')
    st.plotly_chart(fig2, use_container_width=True)

st.info("Profitability requires cost/margin data; add a costs table to compute category-wise margins.")
