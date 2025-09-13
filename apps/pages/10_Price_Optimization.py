import os, sys, datetime as dt
import numpy as np
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Price Optimization", layout="wide")
st.title("Price Optimization")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-1), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()
bucket_max = st.sidebar.slider("Max discount %", min_value=10, max_value=90, value=50, step=5)
bucket_count = st.sidebar.slider("Discount buckets", min_value=4, max_value=12, value=6)
min_obs = st.sidebar.slider("Min points for elasticity", min_value=10, max_value=200, value=30)

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

@st.cache_data(ttl=300)
def brands_for_categories(cats: list[str] | None):
    params = []
    cond = ""
    if cats:
        cond = " WHERE category = ANY(%s)"
        params.append(cats)
    # Prefer products, fallback to transactions union
    df = read_sql(f"""
        SELECT DISTINCT brand FROM products{cond}
        UNION
        SELECT DISTINCT brand FROM transactions{cond}
        ORDER BY 1
    """, params if params else None)
    return df["brand"].dropna().tolist()

@st.cache_data(ttl=300)
def price_demand(start: str, end: str, cats: list[str] | None, brands: list[str] | None):
    cond_cat = "" if not cats else " AND p.category = ANY(%s)"
    cond_brand = "" if not brands else " AND p.brand = ANY(%s)"
    params = [start, end]
    if cats:
        params.append(cats)
    if brands:
        params.append(brands)
    sql = f"""
      SELECT COALESCE(p.category, t.category, 'Unknown') AS category,
             COALESCE(p.brand, t.brand, 'Unknown') AS brand,
             t.unit_price, t.quantity
      FROM transactions t
      LEFT JOIN products p ON p.product_id = t.product_id
      JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s AND t.unit_price IS NOT NULL AND t.quantity IS NOT NULL
        {(' AND COALESCE(p.category, t.category) = ANY(%s)') if cats else ''}
        {(' AND COALESCE(p.brand, t.brand) = ANY(%s)') if brands else ''}
    """
    return read_sql(sql, params)

@st.cache_data(ttl=300)
def discount_effect(start: str, end: str, cats: list[str] | None, max_pct: int, buckets: int):
    cond_cat = "" if not cats else " AND p.category = ANY(%s)"
    params = [start, end]
    if cats:
        params.append(cats)
    sql = f"""
      SELECT COALESCE(p.category, t.category, 'Unknown') AS category,
             width_bucket(COALESCE(t.discount_pct,0), 0, %s, %s) AS disc_bucket,
             AVG(t.discount_pct) AS avg_disc,
             SUM(t.quantity) AS units,
             SUM(t.revenue) AS revenue
      FROM transactions t
      LEFT JOIN products p ON p.product_id = t.product_id
      JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s {cond_cat}
      GROUP BY 1,2
      ORDER BY 1,2
    """
    if max_pct <= 0 or buckets <= 0:
        return pd.DataFrame(columns=["category","disc_bucket","avg_disc","units","revenue"])
    # prepend bucket params in order: max, buckets
    return read_sql(sql, [max_pct, buckets] + params)

cat_opts = ["All"] + categories()
cat_sel = st.sidebar.multiselect("Categories", cat_opts, default=["All"]) 
cats = None if ("All" in cat_sel or not cat_sel) else cat_sel
brand_opts = ["All"] + brands_for_categories(cats)
brand_sel = st.sidebar.multiselect("Brands", brand_opts, default=["All"]) 
brands = None if ("All" in brand_sel or not brand_sel) else brand_sel

pd_df = price_demand(start_s, end_s, cats, brands)
if not pd_df.empty:
    st.subheader("Price vs Demand (Units)")
    cat_list = sorted(pd_df['category'].unique())
    cat_choice = st.selectbox("Category", cat_list)
    sub = pd_df[pd_df['category'] == cat_choice]
    # optional brand filter within chart
    brand_in_cat = ["All"] + sorted(sub['brand'].dropna().unique().tolist())
    brand_choice = st.selectbox("Brand (optional)", brand_in_cat)
    if brand_choice != "All":
        sub = sub[sub['brand'] == brand_choice]
    # scatter with trendline if statsmodels available
    try:
        fig_scatter = px.scatter(sub, x='unit_price', y='quantity', trendline='ols', title=f'Price vs Units - {cat_choice}')
    except Exception:
        fig_scatter = px.scatter(sub, x='unit_price', y='quantity', title=f'Price vs Units - {cat_choice}')
    st.plotly_chart(fig_scatter, use_container_width=True)
    # elasticity via log-log slope + R^2
    if len(sub) >= min_obs:
        x = np.log(sub['unit_price'].astype(float) + 1e-6)
        y = np.log(sub['quantity'].astype(float) + 1e-6)
        b1, b0 = np.polyfit(x, y, 1)
        y_pred = b1*x + b0
        ss_res = np.sum((y - y_pred)**2)
        ss_tot = np.sum((y - np.mean(y))**2)
        r2 = 1 - ss_res/ss_tot if ss_tot != 0 else np.nan
        st.info(f"Estimated elasticity (log-log slope) for {cat_choice}{' - ' + brand_choice if brand_choice!='All' else ''}: {b1:.2f} (R²={r2:.2f})")
else:
    st.warning("Insufficient price/quantity data for elasticity analysis.")

de = discount_effect(start_s, end_s, cats, bucket_max, bucket_count)
if not de.empty:
    st.subheader("Discount Effectiveness")
    # translate bucket to percent label
    de['disc_label'] = (de['disc_bucket'] * (bucket_max / bucket_count)).astype(float)
    st.plotly_chart(px.line(de, x='disc_label', y='units', color='category', markers=True, title='Units vs Discount %'), use_container_width=True)
    st.plotly_chart(px.line(de, x='disc_label', y='revenue', color='category', markers=True, title='Revenue vs Discount %'), use_container_width=True)

    # Heatmap of revenue by category vs discount bucket
    piv = de.pivot_table(index='category', columns='disc_label', values='revenue', aggfunc='sum', fill_value=0)
    st.dataframe(piv)

st.subheader("Price Bands Analysis")
if not pd_df.empty:
    cat_choice2 = st.selectbox("Category for bands", sorted(pd_df['category'].unique()), key='bands_cat')
    sub2 = pd_df[pd_df['category'] == cat_choice2].copy()
    if len(sub2) >= min_obs:
        sub2['band'] = pd.qcut(sub2['unit_price'], q=10, duplicates='drop')
        sub2['line_rev'] = sub2['unit_price'].astype(float) * sub2['quantity'].astype(float)
        agg = sub2.groupby('band').agg(units=('quantity','sum'), revenue=('line_rev','sum'))
        st.bar_chart(agg['units'])
        st.bar_chart(agg['revenue'])

st.subheader("Brand Positioning (within category)")
if not pd_df.empty:
    cat_choice3 = st.selectbox("Category for brand positioning", sorted(pd_df['category'].unique()), key='brand_pos_cat')
    sub3 = pd_df[pd_df['category'] == cat_choice3].copy()
    if not sub3.empty:
        brand_stats = sub3.groupby('brand').agg(median_price=('unit_price','median'), units=('quantity','sum')).reset_index()
        brand_stats['revenue'] = brand_stats['units'] * brand_stats['median_price']
        fig_bp = px.scatter(brand_stats, x='median_price', y='units', size='revenue', hover_name='brand', title=f'Brand Positioning - {cat_choice3}')
        st.plotly_chart(fig_bp, use_container_width=True)

st.info("Competitive pricing analysis can be added using brand/competitor mappings and external price feeds.")
