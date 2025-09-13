import os, sys
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Strategic Overview", layout="wide")
st.title("Strategic Overview")

st.caption("Market share, competitive positioning, geographic expansion, and health indicators")

@st.cache_data(ttl=300)
def brand_share():
    sql = """
      SELECT COALESCE(p.brand,'Unknown') AS brand, SUM(t.revenue) AS revenue
      FROM transactions t LEFT JOIN products p ON p.product_id = t.product_id
      GROUP BY 1 ORDER BY 2 DESC
    """
    return read_sql(sql)

@st.cache_data(ttl=300)
def geo_expansion():
    sql = """
      SELECT d.year, t.state, COUNT(DISTINCT t.city) AS cities, SUM(t.revenue) AS revenue
      FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
      GROUP BY d.year, t.state
    """
    return read_sql(sql)

bs = brand_share()
if not bs.empty:
    fig = px.pie(bs.head(10), values='revenue', names='brand', title='Top 10 Brand Revenue Share')
    st.plotly_chart(fig, use_container_width=True)

geo = geo_expansion()
if not geo.empty:
    st.plotly_chart(px.line(geo.groupby('year', as_index=False)['cities'].sum(), x='year', y='cities', title='Cities Covered by Year'), use_container_width=True)

st.info("Add competitor mappings and tiers to enhance this page (e.g., metro vs tier-2).")

