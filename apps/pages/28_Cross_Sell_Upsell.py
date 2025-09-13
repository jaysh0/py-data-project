import os, sys, datetime as dt
import streamlit as st
import pandas as pd

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Cross-sell & Upsell", layout="wide")
st.title("Cross-selling & Upselling")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-1), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def associations(start: str, end: str):
    sql = """
      WITH items AS (
        SELECT order_id, product_id
        FROM transactions t JOIN time_dimension d ON d.date_key=t.date_key
        WHERE d.date BETWEEN %s AND %s AND order_id IS NOT NULL AND product_id IS NOT NULL
        GROUP BY order_id, product_id
      )
      SELECT a.product_id AS prod_a, b.product_id AS prod_b, COUNT(*) AS co_occurs
      FROM items a JOIN items b ON a.order_id=b.order_id AND a.product_id<b.product_id
      GROUP BY a.product_id, b.product_id
      ORDER BY co_occurs DESC
      LIMIT 100
    """
    return read_sql(sql, [start, end])

assoc = associations(start_s, end_s)
if assoc.empty:
    st.info("Not enough order-level data to compute associations.")
else:
    st.subheader("Top Product Associations")
    st.dataframe(assoc)

