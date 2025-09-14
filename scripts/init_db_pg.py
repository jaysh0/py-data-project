"""Initialize the Postgres analytics schema and populate the time dimension."""

import os
import sys
from datetime import date

# Ensure src/ importable
_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.db_pg_utils import connect_postgres, execute_sql_file, populate_time_dimension_pg


def main():
    """Create schema/tables and fill time_dimension for 2015–2025."""
    schema_path = os.path.join(_ROOT, "db", "schema.postgres.sql")
    conn = connect_postgres()
    execute_sql_file(conn, schema_path)
    populate_time_dimension_pg(conn, date(2015, 1, 1), date(2025, 12, 31), fiscal_year_start_month=4)
    print("Initialized Postgres schema (analytics) and populated time_dimension.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
