import os, sys, datetime as dt
import pandas as pd
import streamlit as st
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Demographics & Behavior", layout="wide")
st.title("Demographics & Behavioral Analytics")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-2), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def age_group_revenue(start: str, end: str):
    sql = """
      SELECT COALESCE(c.age_group, 'Unknown') AS age_group, SUM(t.revenue) AS revenue
      FROM transactions t
      LEFT JOIN customers c ON c.customer_id = t.customer_id
      JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY 1 ORDER BY 2 DESC
    """
    return read_sql(sql, [start, end])

@st.cache_data(ttl=300)
def age_category_pref(start: str, end: str):
    sql = """
      SELECT COALESCE(c.age_group,'Unknown') AS age_group,
             COALESCE(p.category, t.category, 'Unknown') AS category,
             SUM(t.revenue) AS revenue
      FROM transactions t
      LEFT JOIN customers c ON c.customer_id=t.customer_id
      LEFT JOIN products p ON p.product_id=t.product_id
      JOIN time_dimension d ON d.date_key=t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY 1,2
    """
    return read_sql(sql, [start, end])

ar = age_group_revenue(start_s, end_s)
if not ar.empty:
    st.subheader("Revenue by Age Group")
    st.plotly_chart(px.bar(ar, x='age_group', y='revenue'), use_container_width=True)

ac = age_category_pref(start_s, end_s)
if not ac.empty:
    st.subheader("Category Preference by Age Group")
    fig = px.area(ac, x='category', y='revenue', color='age_group', groupnorm='fraction')
    st.plotly_chart(fig, use_container_width=True)

st.info("Populate customers.age_group to unlock full demographic insights. If you have gender or income segments, we can add them similarly.")

