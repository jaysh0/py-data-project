"""Load/Upsert products dimension from the catalog (cleaned or raw)."""

import os
import sys
import pandas as pd

# Ensure src/ importable
_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.db_pg_utils import connect_postgres


def pick_catalog_path() -> str | None:
    """Return path to the cleaned catalog if present, else the raw CSV.

    Looks in data/cleaned/ first, then data/.
    """
    # Prefer cleaned catalog if present
    cand_cleaned = os.path.join(_ROOT, "data", "cleaned", "amazon_india_products_catalog.cleaned.csv")
    cand_raw = os.path.join(_ROOT, "data", "amazon_india_products_catalog.csv")
    if os.path.exists(cand_cleaned):
        return cand_cleaned
    if os.path.exists(cand_raw):
        return cand_raw
    return None


def upsert_products(conn, df: pd.DataFrame):
    """Upsert product rows into analytics.products with conflict on product_id."""
    # Normalize expected columns
    rename_map = {
        'sub_category': 'subcategory',  # accept both names, normalize to 'subcategory'
    }
    df = df.rename(columns=rename_map)

    cols = {
        'product_id': 'product_id',
        'product_name': 'product_name',
        'brand': 'brand',
        'category': 'category',
        'subcategory': 'subcategory',
        'launch_year': 'launch_year',
        'base_price_2015': 'base_price_2015',
        'weight_kg': 'weight_kg',
    }
    present = [src for src in cols.keys() if src in df.columns]
    if 'product_id' not in present:
        raise ValueError("product_id column is required in the catalog file")

    sub = df[present].dropna(subset=['product_id']).drop_duplicates('product_id')

    # Build dynamic SQL
    tgt_cols = [cols[c] for c in present]
    placeholders = ",".join(["%s"] * len(tgt_cols))
    col_list = ",".join(tgt_cols)
    # Qualify target table columns to avoid ambiguity in DO UPDATE
    set_updates = ", ".join([f"{c}=COALESCE(EXCLUDED.{c}, products.{c})" for c in tgt_cols if c != 'product_id'])

    with conn.cursor() as cur:
        for _, row in sub.iterrows():
            values = [row.get(src) for src in present]
            cur.execute(
                f"""
                INSERT INTO products ({col_list}) VALUES ({placeholders})
                ON CONFLICT (product_id) DO UPDATE SET {set_updates}
                """,
                values,
            )
    conn.commit()


def main():
    """Entry point: locate catalog CSV and upsert into products table."""
    path = pick_catalog_path()
    if not path:
        print("Catalog file not found in data/ or data/cleaned/. Expected amazon_india_products_catalog.csv")
        return 1
    df = pd.read_csv(path)
    conn = connect_postgres()
    upsert_products(conn, df)
    print(f"Upserted {df.shape[0]} product records from {os.path.basename(path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
