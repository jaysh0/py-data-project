import os, sys, datetime as dt
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Festival Sales Analytics", layout="wide")
st.title("Festival Sales Analytics")

today = dt.date.today()
year = st.sidebar.number_input("Year", min_value=2015, max_value=today.year, value=today.year)
festival = st.sidebar.selectbox("Festival", ["Diwali", "Prime Day", "Custom"])

if festival == "Custom":
    start_date = st.sidebar.date_input("Start", value=dt.date(year, 1, 1))
    end_date = st.sidebar.date_input("End", value=dt.date(year, 1, 7))
else:
    # simple approximations; adjust as needed
    if festival == "Diwali":
        # approximate Diwali window: Nov 1-14 (varies by year; customize if exact dates known)
        start_date = dt.date(year, 11, 1)
        end_date = dt.date(year, 11, 14)
    elif festival == "Prime Day":
        start_date = dt.date(year, 7, 15)
        end_date = dt.date(year, 7, 16)

window = st.sidebar.slider("Before/After window (days)", 3, 21, 7)

def revenue_range(a: str, b: str):
    sql = """
      SELECT d.date, SUM(t.revenue) AS revenue
      FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY d.date ORDER BY d.date
    """
    return read_sql(sql, [a, b])

start_s, end_s = start_date.isoformat(), end_date.isoformat()
before_s = (start_date - dt.timedelta(days=window)).isoformat()
before_e = (start_date - dt.timedelta(days=1)).isoformat()
after_s = (end_date + dt.timedelta(days=1)).isoformat()
after_e = (end_date + dt.timedelta(days=window)).isoformat()

tab1, tab2, tab3 = st.tabs(["Before", "During", "After"])

df_before = revenue_range(before_s, before_e)
df_during = revenue_range(start_s, end_s)
df_after = revenue_range(after_s, after_e)

with tab1:
    st.plotly_chart(px.bar(df_before, x='date', y='revenue', title=f"Before ({before_s} to {before_e})"), use_container_width=True)
with tab2:
    st.plotly_chart(px.bar(df_during, x='date', y='revenue', title=f"During ({start_s} to {end_s})"), use_container_width=True)
with tab3:
    st.plotly_chart(px.bar(df_after, x='date', y='revenue', title=f"After ({after_s} to {after_e})"), use_container_width=True)

st.subheader("Campaign Effectiveness Summary")
summary = pd.DataFrame({
    'period': ['before','during','after'],
    'revenue': [df_before['revenue'].sum() if not df_before.empty else 0,
                df_during['revenue'].sum() if not df_during.empty else 0,
                df_after['revenue'].sum() if not df_after.empty else 0]
})
st.dataframe(summary)

st.info("If your data includes campaign flags (e.g., is_festival_sale, campaign_id), we can attribute impact precisely and add lift metrics.")
