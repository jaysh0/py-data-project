import os, sys, datetime as dt
import numpy as np
import streamlit as st
import pandas as pd
import plotly.express as px
from statsmodels.tsa.api import ExponentialSmoothing

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Predictive Analytics", layout="wide")
st.title("Predictive Analytics")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-3), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()
horizon = st.sidebar.slider("Forecast horizon (months)", 3, 24, 12)

@st.cache_data(ttl=300)
def monthly_revenue(start: str, end: str):
    sql = """
      SELECT make_date(d.year, d.month, 1) AS ym, SUM(t.revenue) AS revenue
      FROM transactions t JOIN time_dimension d ON d.date_key=t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY ym ORDER BY ym
    """
    return read_sql(sql, [start, end])

mr = monthly_revenue(start_s, end_s)
if mr.empty:
    st.warning("Not enough data to forecast.")
    st.stop()

ts = mr.set_index('ym')['revenue']
model = ExponentialSmoothing(ts, trend='add', seasonal='add', seasonal_periods=12)
fit = model.fit(optimized=True)
fcast = fit.forecast(horizon)
fc = fcast.reset_index().rename(columns={'index':'ym', 0:'forecast'})
mr['type'] = 'actual'; fc['type'] = 'forecast'; fc = fc.rename(columns={'forecast':'revenue'})
allp = pd.concat([mr[['ym','revenue','type']], fc[['ym','revenue','type']]])
st.plotly_chart(px.line(allp, x='ym', y='revenue', color='type', title='Monthly Revenue Forecast'), use_container_width=True)

st.info("For churn prediction and scenario planning, share labeled churn data or specify drivers; we can add a classifier and slider-driven scenarios.")

