"""Add category and brand columns to the transactions fact table (if missing).

Useful when product dimension is sparse—allows dashboards to fall back on
fact-level segmentation.
"""

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.db_pg_utils import connect_postgres


def main():
    """Execute ALTER TABLEs and supporting indexes under the analytics schema."""
    conn = connect_postgres()
    with conn.cursor() as cur:
        cur.execute("SET search_path = analytics, public;")
        cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS category TEXT;")
        cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS brand TEXT;")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tx_category ON transactions(category);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tx_brand ON transactions(brand);")
    conn.commit()
    print("Migrated: added transactions.category and transactions.brand with indexes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
