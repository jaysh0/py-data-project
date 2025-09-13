import os, sys, datetime as dt, calendar
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Real-time Monitor", layout="wide")
st.title("Real-time Business Performance Monitor")

today = dt.date.today()
month_start = today.replace(day=1)
days_in_month = calendar.monthrange(today.year, today.month)[1]

target_rev = st.sidebar.number_input("Monthly Revenue Target (₹)", value=10000000, step=100000)
target_cust = st.sidebar.number_input("Monthly Active Customers Target", value=10000, step=100)
refresh_ttl = st.sidebar.slider("Data refresh (seconds)", 10, 600, 60)

@st.cache_data(ttl=refresh_ttl)
def mtd_metrics(start: str, end: str):
    sql = """
        SELECT SUM(t.revenue) AS revenue,
               COUNT(DISTINCT t.customer_id) AS customers,
               COUNT(*) AS orders
        FROM transactions t
        JOIN time_dimension d ON d.date_key = t.date_key
        WHERE d.date BETWEEN %s AND %s
    """
    row = read_sql(sql, [start, end]).iloc[0]
    return float(row["revenue"] or 0), int(row["customers"] or 0), int(row["orders"] or 0)

rev, cust, orders = mtd_metrics(month_start.isoformat(), today.isoformat())
run_rate = rev / max(1, today.day) * days_in_month

col1, col2, col3, col4 = st.columns(4)
col1.metric("Revenue MTD", f"₹{rev:,.0f}", delta=f"Run-rate ₹{run_rate:,.0f}")
col2.metric("Active Customers MTD", f"{cust:,}")
col3.metric("Orders MTD", f"{orders:,}")
ach = 0 if target_rev == 0 else (rev/target_rev*100)
col4.metric("Target Achieved", f"{ach:.1f}%")

if rev < target_rev * (today.day/days_in_month):
    st.warning("Revenue is behind linear target trajectory.")
else:
    st.success("Revenue track on or above target trajectory.")

@st.cache_data(ttl=refresh_ttl)
def daily_revenue_month(year: int, month: int):
    sql = """
      SELECT d.date, SUM(t.revenue) AS revenue
      FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.year=%s AND d.month=%s
      GROUP BY d.date ORDER BY d.date
    """
    return read_sql(sql, [year, month])

dr = daily_revenue_month(today.year, today.month)
if not dr.empty:
    st.plotly_chart(px.bar(dr, x="date", y="revenue", title="Daily Revenue (Current Month)"), use_container_width=True)

