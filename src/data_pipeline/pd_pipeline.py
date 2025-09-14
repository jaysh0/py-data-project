"""Pandas-powered data cleaning pipeline.

This module implements configurable, composable cleaning steps (missing value
imputation, date/price/category normalization, geo/boolean fixes, delivery
parsing, de-duplication, outlier correction, and payment normalization) and a
single `run_cleaning_df` orchestrator that runs them in sequence and returns a
cleaned DataFrame along with a step-by-step report.
"""

from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional
import math
import difflib
import unicodedata
import re
import pandas as pd
from .config import PipelineConfig


# ---------- Helpers ----------

def dq_report_df(before: pd.DataFrame, after: pd.DataFrame) -> Dict[str, Any]:
    """Summarize basic data-quality metrics before/after cleaning.

    Returns a dict with row counts and per-column missing counts for both
    the input and output frames.
    """
    def missing_counts(df: pd.DataFrame) -> Dict[str, int]:
        if df.empty:
            return {}
        return df.isna().sum().to_dict()
    return {
        "rows_before": int(len(before)),
        "rows_after": int(len(after)),
        "missing_before": missing_counts(before),
        "missing_after": missing_counts(after),
    }


# ---------- Cleaning steps (pandas) ----------

def impute_missing_pd(df: pd.DataFrame, cfg: PipelineConfig) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Impute missing values for numeric and categorical columns.

    Numeric columns use mean/median (from config); categoricals use mode.
    Returns the updated DataFrame and a report with imputed counts.
    """
    include = cfg.missing.include or list(df.columns)
    exclude = set(cfg.missing.exclude or [])
    cols = [c for c in include if c in df.columns and c not in exclude]
    rep: Dict[str, Any] = {"numeric": {}, "categorical": {}}
    if not cols:
        return df, rep

    # detect numeric columns among target cols
    num_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    cat_cols = [c for c in cols if c not in num_cols]

    # numeric
    if num_cols:
        if cfg.missing.numeric_strategy == "mean":
            values = df[num_cols].mean(numeric_only=True)
        else:  # median default
            values = df[num_cols].median(numeric_only=True)
        rep["numeric"] = {c: int(df[c].isna().sum()) for c in num_cols}
        df[num_cols] = df[num_cols].fillna(values)

    # categorical
    for c in cat_cols:
        mode = df[c].mode(dropna=True)
        if not mode.empty:
            rep["categorical"][c] = int(df[c].isna().sum())
            df[c] = df[c].fillna(mode.iloc[0])

    return df, rep


def standardize_dates_pd(df: pd.DataFrame, fields: List[str], invalid_to_null: bool, target_format: str,
                         input_formats: List[str]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Parse and normalize date columns to a target string format.

    Tries explicit formats first, then a catch-all parse. Optionally coerces
    invalid parses to nulls. Returns a per-field converted count.
    """
    changed = {f: 0 for f in fields if f in df.columns}
    for f in fields:
        if f not in df.columns:
            continue
        ser = df[f].astype("string")
        # first pass: parse unambiguous/ISO without dayfirst to avoid warnings
        parsed = pd.to_datetime(ser, errors="coerce")
        # secondary passes for explicit formats
        for fmt in input_formats or []:
            rem = ser[parsed.isna()]
            if rem.empty:
                break
            parsed2 = pd.to_datetime(rem, errors="coerce", format=fmt)
            parsed.loc[parsed.isna()] = parsed2
        
        before_nonnull = ser.notna() & (ser.str.len() > 0)
        changed[f] = int((parsed.notna() & before_nonnull).sum())
        if invalid_to_null:
            df.loc[:, f] = parsed.dt.strftime(target_format)
        else:
            # keep original where parsing failed
            df.loc[:, f] = df[f].where(parsed.isna(), parsed.dt.strftime(target_format))
    return df, {"dates_converted": changed}


_CURRENCY_RE = re.compile(r"[^\d\-\.\,()]")

def standardize_prices_pd(df: pd.DataFrame, fields: List[str], decimal_places: int,
                          coerce_invalid_to_null: bool) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Normalize currency-like text to numeric values with fixed decimals."""
    rep = {f: 0 for f in fields if f in df.columns}
    for f in fields:
        if f not in df.columns:
            continue
        s = df[f].astype("string")
        s2 = s.str.replace(_CURRENCY_RE, "", regex=True).str.replace(",", "", regex=False) # type: ignore
        # handle parentheses negative
        neg_mask = s2.str.match(r"^\(.*\)$", na=False)
        s2 = s2.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
        nums = pd.to_numeric(s2, errors="coerce")
        if not coerce_invalid_to_null:
            df.loc[:, f] = df[f].where(nums.isna(), nums.round(decimal_places))
        else:
            df.loc[:, f] = nums.round(decimal_places)
        rep[f] = int(nums.notna().sum())
    return df, {"prices_standardized": rep}


_STAR_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*stars?$", re.IGNORECASE)
_FRAC_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)$")

def _parse_rating_val(s: Optional[str], scale_max: float = 5.0) -> Optional[float]:
    """Parse a rating string (e.g., "4", "4 stars", "3/5") to a float."""
    if s is None:
        return None
    t = s.strip()
    if t == "":
        return None
    try:
        x = float(t)
        if x > 0:
            return max(1.0, min(scale_max, x))
    except Exception:
        pass
    m = _STAR_RE.match(t)
    if m:
        try:
            x = float(m.group(1))
            return max(1.0, min(scale_max, x))
        except Exception:
            return None
    m = _FRAC_RE.match(t)
    if m:
        try:
            num = float(m.group(1)); den = float(m.group(2))
            if den > 0:
                x = (num/den)*scale_max
                return max(1.0, min(scale_max, x))
        except Exception:
            return None
    return None


def standardize_ratings_pd(df: pd.DataFrame, column: str, decimal_places: int, impute: Any) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Normalize a ratings column to a bounded numeric scale with imputation."""
    if column not in df.columns:
        return df, {"ratings_changed": 0, "ratings_imputed": 0}
    s = df[column].astype("string")
    parsed = s.apply(lambda x: _parse_rating_val(x))
    changed = int(parsed.notna().sum())
    if isinstance(impute, (int, float)):
        fill_val = float(impute)
    elif impute == "mean":
        fill_val = parsed.mean()
    else:
        fill_val = parsed.median()
    parsed = parsed.fillna(fill_val)
    df.loc[:, column] = parsed.round(decimal_places)
    imputed = int(df[column].isna().sum()) if pd.isna(fill_val) else int(s.isna().sum())
    return df, {"ratings_changed": changed, "ratings_imputed": imputed}


def standardize_categories_pd(df: pd.DataFrame, fields: List[str], lowercase: bool, strip: bool,
                              collapse_spaces: bool, replace_ampersand: bool,
                              mappings: Dict[str, Dict[str, str]]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Clean categorical text and apply optional per-field mappings."""
    rep = {f: 0 for f in fields if f in df.columns}
    for f in fields:
        if f not in df.columns:
            continue
        s = df[f].astype("string")
        if strip:
            s = s.str.strip()
        if lowercase:
            s = s.str.lower()
        if replace_ampersand:
            s = s.str.replace("&", "and", regex=False)
        if collapse_spaces:
            s = s.str.replace(r"\s+", " ", regex=True)
        fmap = mappings.get(f, {})
        if fmap:
            s = s.map(lambda x: fmap.get(x, x))
        rep[f] = int((df[f] != s).fillna(False).sum())
        df.loc[:, f] = s
    return df, {"categories_standardized": rep}


def _normalize_city_name(s: str) -> str:
    t = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    t = " ".join(t.split())
    return t.strip().title()


def resolve_cities_pd(df: pd.DataFrame, column: Optional[str], canonical: List[str], mappings: Dict[str, str],
                      fuzzy_threshold: float) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Standardize city names via mappings, normalization, and fuzzy matching."""
    if not column or column not in df.columns:
        return df, {"geo_resolved": 0}
    canon_norm = { _normalize_city_name(c): c for c in canonical }
    canon_keys = list(canon_norm.keys())
    resolved = 0

    def fix(x: Any) -> Any:
        nonlocal resolved
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return x
        raw = str(x)
        if raw in mappings:
            resolved += 1
            return mappings[raw]
        norm = _normalize_city_name(raw)
        if norm in canon_norm:
            resolved += 1
            return canon_norm[norm]
        if canon_keys:
            m = difflib.get_close_matches(norm, canon_keys, n=1, cutoff=fuzzy_threshold)
            if m:
                resolved += 1
                return canon_norm[m[0]]
        return x

    df.loc[:, column] = df[column].apply(fix)
    return df, {"geo_resolved": resolved}


_TRUE_SET = {"true", "t", "yes", "y", "1"}
_FALSE_SET = {"false", "f", "no", "n", "0"}

def standardize_booleans_pd(df: pd.DataFrame, fields: List[str]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Convert mixed boolean-like values (Yes/No, 1/0, Y/N) to True/False."""
    rep = {f: 0 for f in fields if f in df.columns}
    for f in fields:
        if f not in df.columns:
            continue
        def pb(v: Any) -> Optional[bool]:
            if pd.isna(v):
                return None
            if isinstance(v, bool):
                return v
            s = str(v).strip().lower()
            if s in _TRUE_SET:
                return True
            if s in _FALSE_SET:
                return False
            return None
        new = df[f].apply(pb)
        rep[f] = int((new != df[f]).fillna(False).sum())
        df.loc[:, f] = new
    return df, {"booleans_standardized": rep}


_RANGE_RE = re.compile(r"^(\d+)\s*[-–]\s*(\d+)")
_NUM_RE = re.compile(r"^(\d+)")

def standardize_delivery_pd(df: pd.DataFrame, column: Optional[str], max_days: int, clip_max: bool) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Parse delivery SLA text/ranges to integer days and clamp outliers."""
    if not column or column not in df.columns:
        return df, {"delivery_changed": 0, "delivery_nullified": 0}
    def parse(v: Any) -> Optional[int]:
        if pd.isna(v):
            return None
        s = str(v).strip().lower()
        if s in {"same day", "sameday"}:
            return 0
        m = _RANGE_RE.match(s)
        if m:
            lo = int(m.group(1)); hi = int(m.group(2))
            return max(lo, hi)
        m = _NUM_RE.match(s)
        if m:
            return int(m.group(1))
        return None
    parsed = df[column].apply(parse)
    nullified = int(((parsed.isna()) | (parsed < 0)).sum())
    parsed = parsed.mask((parsed < 0), other=pd.NA)
    if clip_max:
        parsed = parsed.clip(upper=max_days)
    changed = int((parsed != df[column]).fillna(False).sum())
    df.loc[:, column] = parsed
    return df, {"delivery_changed": changed, "delivery_nullified": nullified}


def deduplicate_pd(df: pd.DataFrame, key_fields: List[str], quantity_field: Optional[str], strategy: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Drop or aggregate duplicates based on composite keys.

    If ``strategy`` is "aggregate" and ``quantity_field`` exists, sums quantities
    per key; otherwise keeps the first row and drops the rest.
    """
    if not key_fields:
        return df, {"dropped": 0, "kept": int(len(df))}
    existing_keys = [k for k in key_fields if k in df.columns]
    missing_keys = [k for k in key_fields if k not in df.columns]
    if not existing_keys:
        return df, {"dropped": 0, "kept": int(len(df)), "skipped_missing_keys": missing_keys}
    if strategy == "aggregate" and quantity_field and quantity_field in df.columns:
        grouped = df.groupby(existing_keys, dropna=False, as_index=False)
        # sum quantity if numeric, else first
        aggs = {c: "first" for c in df.columns if c not in existing_keys}
        aggs[quantity_field] = "sum"
        out = grouped.agg(aggs)
    else:
        before = len(df)
        out = df.drop_duplicates(subset=existing_keys, keep="first")
        return out, {"dropped": int(before - len(out)), "kept": int(len(out)), "missing_keys": missing_keys}
    return out, {"dropped": int(len(df) - len(out)), "kept": int(len(out)), "aggregated": True, "missing_keys": missing_keys}


def correct_outliers_pd(df: pd.DataFrame, column: Optional[str], high_factor: float,
                        downscale_candidates: List[int], decimal_places: int) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Identify extreme high values and attempt decimal downscale correction."""
    if not column or column not in df.columns:
        return df, {"corrected": 0, "flagged": 0}
    ser = pd.to_numeric(df[column], errors="coerce")
    med = ser.median()
    flagged_mask = ser > (med * high_factor)
    corrected = 0
    for f in downscale_candidates:
        cand = ser / f
        ok = flagged_mask & cand.between(med / 10, med * 10)
        ser = ser.where(~ok, cand)
        corrected += int(ok.sum())
        flagged_mask = flagged_mask & ~ok
    df.loc[:, column] = ser.round(decimal_places)
    return df, {"corrected": corrected, "flagged": int(flagged_mask.sum()), "median": med}


def normalize_payment_pd(df: pd.DataFrame, column: Optional[str], extra_mappings: Dict[str, str]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Map payment method variants (UPI/PhonePe/GPay, CC/CREDIT_CARD, COD) to canonical labels."""
    if not column or column not in df.columns:
        return df, {"payment_standardized": 0}
    def norm(s: Any) -> Any:
        if pd.isna(s):
            return s
        raw = str(s)
        if raw in extra_mappings:
            return extra_mappings[raw]
        t = raw.strip().lower().replace("_", " ")
        t = t.replace("c.o.d", "cod").replace("creditcard", "credit card")
        if any(x in t for x in ["upi", "phonepe", "google pay", "gpay", "googlepay"]):
            return "UPI"
        if any(x in t for x in ["cod", "cash on delivery", "cash-on-delivery"]):
            return "Cash on Delivery"
        if "debit" in t:
            return "Debit Card"
        if any(x in t for x in ["credit card", "cc"]):
            return "Credit Card"
        if "netbank" in t or "net bank" in t:
            return "Net Banking"
        if "wallet" in t:
            return "Wallet"
        return raw
    new = df[column].apply(norm)
    changed = int((new != df[column]).fillna(False).sum())
    df.loc[:, column] = new
    return df, {"payment_standardized": changed}


def run_cleaning_df(df: pd.DataFrame, cfg: PipelineConfig) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Run all configured cleaning steps and collect a step-by-step report."""
    # Work on a copy to avoid SettingWithCopy issues from upstream slices
    df = df.copy()
    report: Dict[str, Any] = {}

    # 1) Missing
    df, rep = impute_missing_pd(df, cfg)
    report["missing"] = rep

    # 2) Dates
    df, rep = standardize_dates_pd(df, cfg.dates.fields, cfg.dates.invalid_to_null, cfg.dates.target_format, cfg.dates.input_formats)
    report["dates"] = rep

    # 3) Prices
    df, rep = standardize_prices_pd(df, cfg.price.fields, cfg.price.decimal_places, cfg.price.coerce_invalid_to_null)
    report["price"] = rep

    # 4) Ratings
    if cfg.ratings.column:
        df, rep = standardize_ratings_pd(df, cfg.ratings.column, cfg.ratings.decimal_places, cfg.ratings.impute_strategy)
        report["ratings"] = rep

    # 5) Categories
    df, rep = standardize_categories_pd(df, cfg.categorical.fields, cfg.categorical.lowercase, cfg.categorical.strip,
                                        cfg.categorical.collapse_spaces, cfg.categorical.replace_ampersand,
                                        cfg.categorical.mappings)
    report["categorical"] = rep

    # 6) Geo
    df, rep = resolve_cities_pd(df, cfg.geo.city_field, cfg.geo.canonical_cities, cfg.geo.city_mappings, cfg.geo.fuzzy_threshold)
    report["geo"] = rep

    # 7) Booleans
    if cfg.booleans.fields:
        df, rep = standardize_booleans_pd(df, cfg.booleans.fields)
        report["booleans"] = rep

    # 8) Delivery
    if cfg.delivery.column:
        df, rep = standardize_delivery_pd(df, cfg.delivery.column, cfg.delivery.max_days, cfg.delivery.clip_max)
        report["delivery"] = rep

    # 9) Dedup
    if cfg.dedup.key_fields:
        df, rep = deduplicate_pd(df, cfg.dedup.key_fields, cfg.dedup.quantity_field, cfg.dedup.strategy)
        report["dedup"] = rep

    # 10) Outliers
    if cfg.outliers.column:
        df, rep = correct_outliers_pd(df, cfg.outliers.column, cfg.outliers.high_factor, cfg.outliers.downscale_candidates, cfg.outliers.decimal_places)
        report["outliers"] = rep

    # 11) Payment
    if cfg.payment.column:
        df, rep = normalize_payment_pd(df, cfg.payment.column, cfg.payment.extra_mappings)
        report["payment"] = rep

    return df, report
