import os
import sys
import streamlit as st

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

st.set_page_config(page_title="Business Intelligence", layout="wide")

st.title("Business Intelligence Portal")
st.markdown(
    """
    Use the sidebar to navigate dashboards:
    - Executive Summary
    - Real-time Monitor
    - Strategic Overview
    - Financial Performance
    - Growth Analytics
    """
)

st.info("Ensure POSTGRES_DSN is set and scripts/init_db_pg.py + scripts/load_to_db_pg.py were run.")

