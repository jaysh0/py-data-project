import os, sys, datetime as dt
import pandas as pd
import streamlit as st
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Customer Retention", layout="wide")
st.title("Customer Retention & Cohort Analysis")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-3), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def cohorts(start: str, end: str) -> pd.DataFrame:
    sql = """
      WITH orders AS (
        SELECT t.customer_id, d.date::date AS order_date, make_date(d.year, d.month, 1) AS ym
        FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
        WHERE d.date BETWEEN %s AND %s AND t.customer_id IS NOT NULL
      ), firsts AS (
        SELECT customer_id, MIN(ym) AS cohort FROM orders GROUP BY customer_id
      ), labeled AS (
        SELECT o.customer_id, o.ym, f.cohort,
               EXTRACT(YEAR FROM age(o.ym, f.cohort)) * 12 + EXTRACT(MONTH FROM age(o.ym, f.cohort)) AS months_since
        FROM orders o JOIN firsts f USING (customer_id)
      )
      SELECT cohort, months_since::int AS m, COUNT(DISTINCT customer_id) AS active
      FROM labeled
      GROUP BY cohort, m
      ORDER BY cohort, m
    """
    return read_sql(sql, [start, end])

co = cohorts(start_s, end_s)
if co.empty:
    st.warning("No cohort data available.")
    st.stop()

st.subheader("Cohort Retention (Active Customers)")
piv = co.pivot_table(index='cohort', columns='m', values='active', aggfunc='sum', fill_value=0)
st.dataframe(piv)

st.subheader("Normalized Retention (%)")
base = piv.iloc[:,0].replace(0, pd.NA)
ret = (piv.divide(base, axis=0) * 100).fillna(0).round(1)
fig = px.imshow(ret, color_continuous_scale='Greens', aspect='auto', labels=dict(color='Retention%'))
st.plotly_chart(fig, use_container_width=True)

