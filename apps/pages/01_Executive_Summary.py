import os, sys, datetime as dt
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Executive Summary", layout="wide")
st.title("Executive Summary")

@st.cache_data(ttl=300)
def get_categories():
    sql = """
      SELECT DISTINCT category FROM products WHERE category IS NOT NULL
      UNION
      SELECT DISTINCT category FROM transactions WHERE category IS NOT NULL
      ORDER BY 1
    """
    df = read_sql(sql)
    return df["category"].dropna().tolist()

@st.cache_data(ttl=60)
def kpi_query(start: str, end: str, categories: list[str] | None):
    where_cat = "" if not categories else " AND p.category = ANY(%s)"
    params = [start, end]
    if categories:
        params.append(categories)
    sql = f"""
        SELECT
          SUM(t.revenue) AS revenue,
          COUNT(DISTINCT t.customer_id) AS active_customers,
          CASE WHEN COUNT(*)>0 THEN SUM(t.revenue)/NULLIF(COUNT(*),0) END AS aov
        FROM transactions t
        LEFT JOIN products p ON p.product_id = t.product_id
        LEFT JOIN time_dimension d ON d.date_key = t.date_key
        WHERE d.date BETWEEN %s AND %s {where_cat}
    """
    df = read_sql(sql, params)
    return df.iloc[0].to_dict()

@st.cache_data(ttl=300)
def revenue_by_year(start: str, end: str, categories: list[str] | None):
    where_cat = "" if not categories else " AND p.category = ANY(%s)"
    params = [start, end]
    if categories:
        params.append(categories)
    sql = f"""
        SELECT d.year, SUM(t.revenue) AS revenue
        FROM transactions t
        LEFT JOIN time_dimension d ON d.date_key = t.date_key
        LEFT JOIN products p ON p.product_id = t.product_id
        WHERE d.date BETWEEN %s AND %s {where_cat}
          AND (%s IS NULL OR COALESCE(p.category, t.category) = ANY(%s))
        GROUP BY d.year
        ORDER BY d.year
    """
    # duplicate cats param to satisfy placeholders when cats is not None
    p2 = params + ([None, None] if not categories else [None, categories])
    return read_sql(sql, p2)

@st.cache_data(ttl=300)
def top_categories(start: str, end: str, limit: int = 10):
    sql = """
        SELECT COALESCE(p.category, t.category, 'Unknown') AS category, SUM(t.revenue) AS revenue
        FROM transactions t
        LEFT JOIN products p ON p.product_id = t.product_id
        LEFT JOIN time_dimension d ON d.date_key = t.date_key
        WHERE d.date BETWEEN %s AND %s
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT %s
    """
    return read_sql(sql, [start, end, limit])

# Sidebar filters
today = dt.date.today()
default_start = today.replace(year=today.year-1)
start_date = st.sidebar.date_input("Start date", value=default_start)
end_date = st.sidebar.date_input("End date", value=today)
c_opts = ["All"] + get_categories()
cat_sel = st.sidebar.multiselect("Categories", c_opts, default=["All"]) 
cats = None if ("All" in cat_sel or not cat_sel) else cat_sel

start_s, end_s = start_date.isoformat(), end_date.isoformat()
kpi = kpi_query(start_s, end_s, cats)

col1, col2, col3 = st.columns(3)
col1.metric("Total Revenue", f"₹{(kpi['revenue'] or 0):,.0f}")
col2.metric("Active Customers", f"{int(kpi['active_customers'] or 0):,}")
col3.metric("Avg Order Value", f"₹{(kpi['aov'] or 0):,.0f}")

# YoY comparison
prev_start = (start_date.replace(year=start_date.year-1)).isoformat()
prev_end = (end_date.replace(year=end_date.year-1)).isoformat()
rev_now = kpi["revenue"] or 0
rev_prev = kpi_query(prev_start, prev_end, cats)["revenue"] or 0
delta = None if rev_prev == 0 else (rev_now - rev_prev)/rev_prev*100
st.metric("Revenue YoY", f"₹{rev_now:,.0f}", f"{(delta or 0):.1f}%")

# Trend line
rev = revenue_by_year("2015-01-01", end_s, cats)
if not rev.empty:
    fig = px.line(rev, x="year", y="revenue", markers=True, title="Revenue by Year")
    st.plotly_chart(fig, use_container_width=True)

# Top categories
tc = top_categories(start_s, end_s, 10)
if not tc.empty:
    fig2 = px.bar(tc, x="revenue", y="category", orientation="h", title="Top Categories")
    st.plotly_chart(fig2, use_container_width=True)
