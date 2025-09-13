import os, sys, datetime as dt
import pandas as pd
import streamlit as st
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Customer Journey", layout="wide")
st.title("Customer Journey Analytics")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-2), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def transitions(start: str, end: str) -> pd.DataFrame:
    sql = """
      WITH base AS (
        SELECT t.customer_id,
               d.date AS order_date,
               COALESCE(p.category, t.category, 'Unknown') AS category,
               ROW_NUMBER() OVER (PARTITION BY t.customer_id ORDER BY d.date, t.tx_id) AS rn
        FROM transactions t
        JOIN time_dimension d ON d.date_key = t.date_key
        LEFT JOIN products p ON p.product_id = t.product_id
        WHERE d.date BETWEEN %s AND %s AND t.customer_id IS NOT NULL
      ), pairs AS (
        SELECT b1.customer_id, b1.category AS cat_from, b2.category AS cat_to
        FROM base b1
        JOIN base b2 ON b1.customer_id=b2.customer_id AND b2.rn=b1.rn+1
      )
      SELECT cat_from, cat_to, COUNT(*) AS transitions
      FROM pairs GROUP BY cat_from, cat_to ORDER BY transitions DESC
    """
    return read_sql(sql, [start, end])

trans = transitions(start_s, end_s)
if trans.empty:
    st.warning("No transitions available for the selected range.")
    st.stop()

st.subheader("Category-to-Category Transitions")
fig = px.imshow(trans.pivot_table(index='cat_from', columns='cat_to', values='transitions', aggfunc='sum', fill_value=0),
                color_continuous_scale='Blues', aspect='auto')
st.plotly_chart(fig, use_container_width=True)

st.subheader("Top Transition Pairs")
st.dataframe(trans.head(50))

