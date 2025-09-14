"""Load cleaned CSVs from data/cleaned into Postgres transactions fact.

Also upserts minimal products/customers if identifiers are present, and
supports fact-level category/brand as fallbacks for segmentation.
"""

import os
import sys
from io import StringIO
import pandas as pd

# Ensure src/ importable
_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.db_pg_utils import connect_postgres


DATE_CANDS = ["order_date", "date"]
REV_CANDS = ["revenue", "total_amount", "original_price_inr", "amount", "total"]
QTY_CANDS = ["quantity", "qty", "units"]
UNIT_PRICE_CANDS = ["unit_price", "price", "base_price_2015"]
ORDER_ID_CANDS = ["order_id", "transaction_id"]
CUSTOMER_CANDS = ["customer_id", "cust_id", "user_id"]
PRODUCT_CANDS = ["product_id", "sku"]
CATEGORY_CANDS = ["category", "Category"]
BRAND_CANDS = ["brand", "Brand"]
PAYMENT_CANDS = ["payment_method", "payment"]
CITY_CANDS = ["city", "customer_city"]
STATE_CANDS = ["state", "customer_state"]
PRIME_CANDS = ["is_prime_member", "prime"]
DELIVERY_CANDS = ["delivery_days"]
RATING_CANDS = ["customer_rating", "rating"]
DISCOUNT_CANDS = ["discount_pct", "discount"]
RETURN_CANDS = ["is_returned", "returned"]


def pick(df: pd.DataFrame, cands):
    """Return the first matching column name present in ``df`` from ``cands``."""
    for c in cands:
        if c in df.columns:
            return c
    return None


def dataframe_to_copy_buffer(df: pd.DataFrame) -> StringIO:
    """Serialize a DataFrame to a CSV StringIO buffer for COPY."""
    buf = StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    return buf


def upsert_dimension(conn, table: str, key_col: str, cols: list[str], df: pd.DataFrame):
    """Idempotent upsert for small dimension slices using INSERT .. ON CONFLICT DO NOTHING."""
    if key_col not in df.columns:
        return
    sub = df[cols].dropna(subset=[key_col]).drop_duplicates()
    if sub.empty:
        return
    tmp_cols = ",".join(cols)
    placeholders = ",".join(["%s"] * len(cols))
    with conn.cursor() as cur:
        for _, row in sub.iterrows():
            values = [row.get(c) for c in cols]
            cur.execute(
                f"INSERT INTO {table} ({tmp_cols}) VALUES ({placeholders}) ON CONFLICT ({key_col}) DO NOTHING",
                values,
            )
    conn.commit()


def main():
    """Walk data/cleaned, load each CSV into analytics.transactions via COPY."""
    cleaned_dir = os.path.join(_ROOT, "data", "cleaned")
    conn = connect_postgres()
    cur = conn.cursor()

    for name in sorted(os.listdir(cleaned_dir)):
        if not name.lower().endswith(".csv"):
            continue
        path = os.path.join(cleaned_dir, name)
        df = pd.read_csv(path)
        if df.empty:
            continue

        date_col = pick(df, DATE_CANDS)
        prod_col = pick(df, PRODUCT_CANDS)
        cust_col = pick(df, CUSTOMER_CANDS)
        rev_col = pick(df, REV_CANDS)
        if not (date_col and rev_col):
            continue
        qty_col = pick(df, QTY_CANDS)
        unit_col = pick(df, UNIT_PRICE_CANDS)
        order_col = pick(df, ORDER_ID_CANDS)
        pay_col = pick(df, PAYMENT_CANDS)
        city_col = pick(df, CITY_CANDS)
        state_col = pick(df, STATE_CANDS)
        prime_col = pick(df, PRIME_CANDS)
        deliv_col = pick(df, DELIVERY_CANDS)
        rating_col = pick(df, RATING_CANDS)
        disc_col = pick(df, DISCOUNT_CANDS)
        ret_col = pick(df, RETURN_CANDS)
        cat_col = pick(df, CATEGORY_CANDS)
        brand_col = pick(df, BRAND_CANDS)

        # Upsert minimal dimensions if available
        if prod_col:
            upsert_dimension(conn, "products", "product_id", [prod_col], df.rename(columns={prod_col: "product_id"}))
        if cust_col:
            # build a small df with customer_id, city, state, is_prime_member
            small = pd.DataFrame({
                "customer_id": df[cust_col] if cust_col in df.columns else None,
                "city": df[city_col] if city_col in df.columns else None,
                "state": df[state_col] if state_col in df.columns else None,
                "is_prime_member": df[prime_col] if prime_col in df.columns else None,
            })
            upsert_dimension(conn, "customers", "customer_id", ["customer_id", "city", "state", "is_prime_member"], small)

        # Prepare fact rows
        dates = pd.to_datetime(df[date_col], errors='coerce')
        date_keys = (dates.dt.year*10000 + dates.dt.month*100 + dates.dt.day).astype('Int64')

        tx = pd.DataFrame({
            "order_id": df[order_col] if order_col in df.columns else None,
            "date_key": date_keys,
            "order_date": dates.dt.date.astype('string'),
            "customer_id": df[cust_col] if cust_col in df.columns else None,
            "product_id": df[prod_col] if prod_col in df.columns else None,
            "quantity": pd.to_numeric(df[qty_col], errors='coerce') if qty_col in df.columns else 1,
            "unit_price": pd.to_numeric(df[unit_col], errors='coerce') if unit_col in df.columns else None,
            "revenue": pd.to_numeric(df[rev_col], errors='coerce') if rev_col in df.columns else None,
            "category": df[cat_col] if cat_col in df.columns else None,
            "brand": df[brand_col] if brand_col in df.columns else None,
            "payment_method": df[pay_col] if pay_col in df.columns else None,
            "city": df[city_col] if city_col in df.columns else None,
            "state": df[state_col] if state_col in df.columns else None,
            "is_prime_member": df[prime_col] if prime_col in df.columns else None,
            "delivery_days": pd.to_numeric(df[deliv_col], errors='coerce') if deliv_col in df.columns else None,
            "customer_rating": pd.to_numeric(df[rating_col], errors='coerce') if rating_col in df.columns else None,
            "discount_pct": pd.to_numeric(df[disc_col], errors='coerce') if disc_col in df.columns else None,
            "is_returned": df[ret_col] if ret_col in df.columns else None,
            "source_file": name,
        })

        # Normalize dtypes for COPY (avoid 6.0 into INTEGER, avoid <NA> literals)
        tx["date_key"] = pd.to_numeric(tx["date_key"], errors='coerce').astype('Int64')
        tx["delivery_days"] = pd.to_numeric(tx["delivery_days"], errors='coerce').astype('Int64')

        # Build string-safe output with empty for NULLs
        cols = [
            "order_id","date_key","order_date","customer_id","product_id","quantity","unit_price","revenue",
            "category","brand",
            "payment_method","city","state","is_prime_member","delivery_days","customer_rating","discount_pct","is_returned","source_file"
        ]
        out = tx[cols].copy()

        # Integers as plain ints or empty
        for c in ["date_key", "delivery_days"]:
            if c in out.columns:
                out[c] = out[c].apply(lambda v: '' if pd.isna(v) else str(int(v)))

        # Booleans to 't'/'f' or empty
        for c in ["is_prime_member", "is_returned"]:
            if c in out.columns:
                out[c] = out[c].map(lambda v: '' if pd.isna(v) else ('t' if bool(v) else 'f'))

        # Dates: ensure empty for NA
        out["order_date"] = out["order_date"].map(lambda v: '' if pd.isna(v) or v in (None, 'NaT', '<NA>') else str(v))

        # Numerics keep as-is but empty for NA
        for c in ["quantity", "unit_price", "revenue", "customer_rating", "discount_pct"]:
            if c in out.columns:
                out[c] = out[c].apply(lambda v: '' if pd.isna(v) else str(v))

        # Texts: empty for NA
        for c in ["order_id","customer_id","product_id","category","brand","payment_method","city","state","source_file"]:
            if c in out.columns:
                out[c] = out[c].map(lambda v: '' if pd.isna(v) else str(v))

        buf = StringIO()
        out.to_csv(buf, index=False, header=False)
        buf.seek(0)
        copy_sql = f"COPY transactions ({', '.join(cols)}) FROM STDIN WITH CSV NULL ''"
        cur.copy_expert(copy_sql, buf)
        conn.commit()
        print(f"Loaded {len(tx)} rows from {name} into transactions")

    print("Done loading all files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
