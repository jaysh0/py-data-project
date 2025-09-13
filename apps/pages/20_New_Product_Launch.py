import os, sys, datetime as dt
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="New Product Launch", layout="wide")
st.title("New Product Launch Dashboard")

year_window = st.sidebar.slider("Launch year window", 1, 10, 3)

@st.cache_data(ttl=300)
def launches(since_year: int):
    sql = """
      SELECT product_id, product_name, brand, category, launch_year
      FROM products WHERE launch_year >= %s ORDER BY launch_year DESC
    """
    return read_sql(sql, [since_year])

this_year = dt.date.today().year
lp = launches(this_year - year_window)
if lp.empty:
    st.info("No products with launch_year in the selected window.")
else:
    st.subheader("Recent Launches")
    st.dataframe(lp)

    # Adoption trend (revenue since launch)
    @st.cache_data(ttl=300)
    def adoption(product_ids: list[str]):
        if not product_ids:
            return pd.DataFrame()
        sql = """
          SELECT t.product_id, d.year, d.month, SUM(t.revenue) AS revenue
          FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
          WHERE t.product_id = ANY(%s)
          GROUP BY t.product_id, d.year, d.month
          ORDER BY d.year, d.month
        """
        return read_sql(sql, [product_ids])

    ad = adoption(lp['product_id'].dropna().tolist())
    if not ad.empty:
        ad['period'] = ad['year'].astype(str) + '-' + ad['month'].astype(str)
        st.plotly_chart(px.line(ad, x='period', y='revenue', color='product_id', title='Post-Launch Revenue Trend'), use_container_width=True)

