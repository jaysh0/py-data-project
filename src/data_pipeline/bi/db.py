import os
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Optional

import pandas as pd
import psycopg2


def get_dsn() -> str:
    return os.environ.get("POSTGRES_DSN", "dbname=postgres user=postgres host=localhost password=postgres")


@contextmanager
def get_conn():
    dsn = get_dsn()
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("SET search_path = analytics, public;")
        yield conn
    finally:
        conn.close()


def read_sql(sql: str, params: Optional[Iterable[Any]] = None) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query(sql, conn, params=params)

