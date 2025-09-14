"""Utilities for initializing and populating the Postgres analytics schema."""

import os
from datetime import date, timedelta
from typing import Optional

import psycopg2
from psycopg2.extensions import connection as PGConnection


def connect_postgres(dsn: Optional[str] = None) -> PGConnection:
    """Create a Postgres connection and set ``search_path`` to ``analytics``.

    Reads DSN from the parameter or ``POSTGRES_DSN`` environment variable.
    """
    dsn = dsn or os.environ.get("POSTGRES_DSN") or "dbname=postgres user=postgres host=localhost password=postgres"
    conn = psycopg2.connect(dsn)
    with conn.cursor() as cur:
        cur.execute("SET search_path = analytics, public;")
    conn.commit()
    return conn


def execute_sql_file(conn: PGConnection, path: str):
    """Execute a .sql file against an open Postgres connection."""
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def yyyymmdd(d: date) -> int:
    """Convert a ``date`` to an integer key in YYYYMMDD format."""
    return d.year * 10000 + d.month * 100 + d.day


def populate_time_dimension_pg(conn: PGConnection, start: date, end: date, fiscal_year_start_month: int = 4):
    """Populate or upsert rows in ``time_dimension`` for a date range.

    Includes calendar attributes and fiscal year/quarter given a fiscal year
    start month (default April for India).
    """
    rows = []
    d = start
    while d <= end:
        dk = yyyymmdd(d)
        day = d.day
        month = d.month
        year = d.year
        month_name = d.strftime("%B")
        quarter = (month - 1) // 3 + 1
        quarter_name = f"Q{quarter}"
        week_iso = int(d.strftime("%V"))
        day_name = d.strftime("%A")
        is_weekend = d.weekday() >= 5
        fy = year if month >= fiscal_year_start_month else (year - 1)
        fquarter = ((month - fiscal_year_start_month) % 12) // 3 + 1
        rows.append((dk, d, day, month, month_name, quarter, quarter_name, year, week_iso, day_name, is_weekend, fy, fquarter))
        d += timedelta(days=1)

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO time_dimension
            (date_key, date, day, month, month_name, quarter, quarter_name, year, week_iso, day_name, is_weekend, fiscal_year, fiscal_quarter)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key) DO UPDATE SET
              date = EXCLUDED.date,
              day = EXCLUDED.day,
              month = EXCLUDED.month,
              month_name = EXCLUDED.month_name,
              quarter = EXCLUDED.quarter,
              quarter_name = EXCLUDED.quarter_name,
              year = EXCLUDED.year,
              week_iso = EXCLUDED.week_iso,
              day_name = EXCLUDED.day_name,
              is_weekend = EXCLUDED.is_weekend,
              fiscal_year = EXCLUDED.fiscal_year,
              fiscal_quarter = EXCLUDED.fiscal_quarter
            """,
            rows,
        )
    conn.commit()
