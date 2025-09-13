import os, sys
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Seasonal Planning", layout="wide")
st.title("Seasonal Planning Dashboard")

@st.cache_data(ttl=300)
def monthly_pivot():
    sql = """
      SELECT d.year, d.month, SUM(t.revenue) AS revenue
      FROM transactions t JOIN time_dimension d ON d.date_key=t.date_key
      GROUP BY d.year, d.month ORDER BY d.year, d.month
    """
    return read_sql(sql)

mp = monthly_pivot()
if not mp.empty:
    piv = mp.pivot_table(index='year', columns='month', values='revenue', aggfunc='sum', fill_value=0)
    st.subheader("Revenue Heatmap")
    st.dataframe(piv)
    st.plotly_chart(px.imshow(piv, color_continuous_scale='YlGnBu'), use_container_width=True)

st.info("Use this view to plan promo windows and resource allocation based on peaks. We can add a calendar and export plan templates.")

