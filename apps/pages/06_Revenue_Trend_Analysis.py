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

st.set_page_config(page_title="Revenue Trend Analysis", layout="wide")
st.title("Revenue Trend Analysis")

@st.cache_data(ttl=300)
def categories():
    try:
        df = read_sql("""
           SELECT DISTINCT category FROM products WHERE category IS NOT NULL
           UNION
           SELECT DISTINCT category FROM transactions WHERE category IS NOT NULL
           ORDER BY 1
        """)
        return df["category"].dropna().tolist()
    except Exception:
        return []

freq = st.sidebar.radio("Frequency", ["Monthly", "Quarterly", "Yearly"], index=0)
today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-3), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
cat_opts = ["All"] + categories()
cat_sel = st.sidebar.multiselect("Categories", cat_opts, default=["All"]) 
cats = None if ("All" in cat_sel or not cat_sel) else cat_sel

def freq_cols():
    if freq == "Monthly":
        return "d.year, d.month", ["year", "month"], "month"
    if freq == "Quarterly":
        return "d.year, d.quarter", ["year", "quarter"], "quarter"
    return "d.year", ["year"], None

keys, cols, label = freq_cols()
where_cat = ""
params = [start_date.isoformat(), end_date.isoformat()]
if cats:
    where_cat = " AND COALESCE(p.category, t.category) = ANY(%s)"
    params.append(cats)

sql = f"""
  SELECT {keys}, SUM(t.revenue) AS revenue
  FROM transactions t
  JOIN time_dimension d ON d.date_key = t.date_key
  LEFT JOIN products p ON p.product_id = t.product_id
  WHERE d.date BETWEEN %s AND %s {where_cat}
  GROUP BY {keys}
  ORDER BY {keys}
"""
df = read_sql(sql, params)

if df.empty:
    st.warning("No data for selected filters.")
    st.stop()

df["period_idx"] = np.arange(len(df))
if len(df) >= 2:
    z = np.polyfit(df["period_idx"], df["revenue"], 1)
    df["trend"] = np.poly1d(z)(df["period_idx"])

if label:
    title = f"Revenue by {label.title()}"
else:
    title = "Revenue by Year"

fig = px.line(df, x=cols, y="revenue", markers=True, title=title)
if "trend" in df.columns:
    fig2 = px.line(df, x=cols, y="trend")
    for tr in fig2.data:
        fig.add_trace(tr)
st.plotly_chart(fig, use_container_width=True)

# Growth rates
df["prev"] = df["revenue"].shift(1)
df["growth_pct"] = (df["revenue"] - df["prev"]) / df["prev"] * 100
st.subheader("Growth Rates")
st.dataframe(df[cols + ["revenue", "growth_pct"]].round(2))

# Seasonal variation (monthly average across years)
if freq == "Monthly":
    sql2 = f"""
      SELECT d.month, AVG(x.rev) AS avg_month_rev
      FROM (
        SELECT d.year, d.month, SUM(t.revenue) AS rev
        FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
        LEFT JOIN products p ON p.product_id = t.product_id
        WHERE d.date BETWEEN %s AND %s {where_cat}
        GROUP BY d.year, d.month
      ) x JOIN time_dimension d ON TRUE
      GROUP BY d.month ORDER BY d.month
    """
    params2 = [start_date.isoformat(), end_date.isoformat()]
    if cats:
        params2.append(cats)
    mon = read_sql(sql2, params2)
    if not mon.empty:
        st.subheader("Seasonal Pattern (Avg by Month)")
        st.plotly_chart(px.bar(mon, x="month", y="avg_month_rev"), use_container_width=True)
