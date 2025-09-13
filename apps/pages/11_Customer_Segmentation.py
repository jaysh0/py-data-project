import os, sys, datetime as dt
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Customer Segmentation (RFM)", layout="wide")
st.title("Customer Segmentation (RFM & Behavior)")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-2), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def rfm_table(start: str, end: str) -> pd.DataFrame:
    sql = """
      WITH orders AS (
        SELECT t.customer_id, d.date::date AS order_date, t.revenue
        FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
        WHERE d.date BETWEEN %s AND %s AND t.customer_id IS NOT NULL
      ),
      last_ref AS (SELECT MAX(order_date) AS ref FROM orders)
      SELECT o.customer_id,
             (SELECT ref FROM last_ref) AS ref_date,
             MAX(o.order_date) AS last_order,
             COUNT(*) AS frequency,
             SUM(o.revenue) AS monetary
      FROM orders o
      GROUP BY o.customer_id
    """
    df = read_sql(sql, [start, end])
    if df.empty:
        return df
    df["R"] = (pd.to_datetime(df["ref_date"]) - pd.to_datetime(df["last_order"]) ).dt.days
    df = df.rename(columns={"frequency":"F", "monetary":"M"})
    # Percentile ranks for scores
    def qs(s, buckets=5, reverse=False):
        ranks = s.rank(pct=True, method='first')
        if reverse: ranks = 1 - ranks
        return np.ceil(ranks*buckets).astype(int).clip(1,buckets)
    df["R_score"] = qs(df["R"], reverse=True)
    df["F_score"] = qs(df["F"]) 
    df["M_score"] = qs(df["M"]) 
    df["Segment"] = df["R_score"].astype(str) + df["F_score"].astype(str) + df["M_score"].astype(str)
    return df

rfm = rfm_table(start_s, end_s)
if rfm.empty:
    st.warning("No customer data for selected period.")
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("Customers", f"{rfm.shape[0]:,}")
col2.metric("Median Monetary", f"₹{rfm['M'].median():,.0f}")
col3.metric("Median Frequency", f"{rfm['F'].median():,.1f}")

seg_counts = rfm.groupby('Segment').size().reset_index(name='count').sort_values('count', ascending=False)
st.subheader("Top RFM Segments")
st.dataframe(seg_counts.head(20))

st.subheader("RFM Scatter (F vs M, colored by Recency days)")
fig = px.scatter(rfm, x='F', y='M', color='R', color_continuous_scale='viridis', hover_data=['Segment','customer_id'])
st.plotly_chart(fig, use_container_width=True)

st.subheader("Segment Drill-down")
seg_choice = st.selectbox("Select segment", seg_counts['Segment'].tolist())
st.dataframe(rfm[rfm['Segment']==seg_choice].sort_values('M', ascending=False).head(200))

