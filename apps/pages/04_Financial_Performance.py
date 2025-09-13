import os, sys
import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Financial Performance", layout="wide")
st.title("Financial Performance")

@st.cache_data(ttl=300)
def revenue_by_category():
    sql = """
      SELECT COALESCE(p.category,'Unknown') AS category, SUM(t.revenue) AS revenue
      FROM transactions t LEFT JOIN products p ON p.product_id = t.product_id
      GROUP BY 1 ORDER BY 2 DESC
    """
    return read_sql(sql)

@st.cache_data(ttl=300)
def monthly_revenue():
    sql = """
      SELECT d.year, d.month, SUM(t.revenue) AS revenue
      FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
      GROUP BY d.year, d.month
      ORDER BY d.year, d.month
    """
    return read_sql(sql)

rc = revenue_by_category()
if not rc.empty:
    st.plotly_chart(px.bar(rc.head(20), x='category', y='revenue', title='Revenue by Category'), use_container_width=True)

mr = monthly_revenue()
if not mr.empty:
    # simple forecast: linear trend on last 24 months
    mr['period'] = np.arange(len(mr))
    if len(mr) >= 6:
        z = np.polyfit(mr['period'], mr['revenue'], 1)
        mr['trend'] = np.poly1d(z)(mr['period'])
    st.plotly_chart(px.line(mr, x=mr.index, y=['revenue','trend'] if 'trend' in mr else ['revenue'], title='Monthly Revenue & Trend'), use_container_width=True)

st.info("For margins and costs, add a costs table or margin % by category and join into transactions.")

