import os, sys, datetime as dt
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Geographic Revenue", layout="wide")
st.title("Geographic Revenue Analysis")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-1), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def state_revenue(start: str, end: str):
    sql = """
      SELECT COALESCE(state,'Unknown') AS state, SUM(revenue) AS revenue
      FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY 1 ORDER BY 2 DESC
    """
    return read_sql(sql, [start, end])

@st.cache_data(ttl=300)
def city_revenue(start: str, end: str):
    sql = """
      SELECT COALESCE(city,'Unknown') AS city, SUM(revenue) AS revenue
      FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY 1 ORDER BY 2 DESC
      LIMIT 50
    """
    return read_sql(sql, [start, end])

@st.cache_data(ttl=300)
def tier_growth():
    # basic city-to-tier mapping for common metros; extend as needed
    return {
        'Bengaluru': 'Metro', 'Bangalore': 'Metro', 'Mumbai': 'Metro', 'Delhi': 'Metro', 'Hyderabad': 'Metro', 'Chennai': 'Metro', 'Kolkata': 'Metro', 'Pune': 'Tier1'
    }

sr = state_revenue(start_s, end_s)
cr = city_revenue(start_s, end_s)

if not sr.empty:
    st.subheader("State-wise Revenue")
    st.plotly_chart(px.bar(sr.head(25), x='revenue', y='state', orientation='h'), use_container_width=True)

if not cr.empty:
    st.subheader("Top Cities by Revenue")
    st.plotly_chart(px.bar(cr, x='revenue', y='city', orientation='h'), use_container_width=True)

st.info("To enable interactive maps, provide an India states GeoJSON or use a mapping API; current view shows ranked bars.")

