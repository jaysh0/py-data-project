import os, sys
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Market Intelligence", layout="wide")
st.title("Market Intelligence")

@st.cache_data(ttl=300)
def brand_price_positioning():
    sql = """
      SELECT COALESCE(p.brand, t.brand, 'Unknown') AS brand,
             COALESCE(p.category, t.category, 'Unknown') AS category,
             PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price) AS median_price,
             SUM(t.quantity) AS units
      FROM transactions t LEFT JOIN products p ON p.product_id=t.product_id
      WHERE t.unit_price IS NOT NULL
      GROUP BY 1,2
    """
    return read_sql(sql)

bp = brand_price_positioning()
if not bp.empty:
    st.subheader("Brand Price vs Units by Category")
    cat = st.selectbox("Category", sorted(bp['category'].unique()))
    sub = bp[bp['category']==cat]
    fig = px.scatter(sub, x='median_price', y='units', hover_name='brand', title=f'Brand Positioning - {cat}')
    st.plotly_chart(fig, use_container_width=True)

st.info("Add competitor labels and external price feeds to deepen market intelligence.")

