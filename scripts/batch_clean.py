"""Batch runner for the pandas cleaning pipeline.

Discovers CSV files in a directory, selects a suitable cleaning config per
file (catalog vs. transactions), runs the pipeline, and writes cleaned
outputs plus JSON reports into a `cleaned/` subfolder.
"""

import argparse
import json
import os
import sys
from typing import List

# Make src/ importable when running directly
_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.config import load_config
from data_pipeline.pd_pipeline import run_cleaning_df, dq_report_df
import pandas as pd


def find_csv_files(data_dir: str) -> List[str]:
    """Return a sorted list of CSV file paths within ``data_dir``.

    Skips non-CSV files, does not recurse into subdirectories.
    """
    files = []
    for name in os.listdir(data_dir):
        if name.lower().endswith(".csv"):
            files.append(os.path.join(data_dir, name))
    return sorted(files)


def choose_config_for_file(path: str) -> str:
    """Select a cleaning config for a given CSV path.

    - Uses the catalog config if the filename contains "catalog".
    - Falls back to the transactions config otherwise.
    """
    name = os.path.basename(path).lower()
    base = os.path.dirname(os.path.dirname(__file__))  # repo root
    if "catalog" in name:
        return os.path.join(base, "configs", "cleaning_amazon_catalog.json")
    # default to transactions config for others
    return os.path.join(base, "configs", "cleaning_transactions_amazon_india.json")


def process_file(input_path: str, output_path: str, report_path: str, config_path: str):
    """Clean a single file and write outputs.

    Returns the final report dict with step summaries and DQ stats.
    """
    raw_df = pd.read_csv(input_path)
    cfg = load_config(config_path)
    cleaned_df, step_report = run_cleaning_df(raw_df, cfg)
    cleaned_df.to_csv(output_path, index=False)
    dq = dq_report_df(raw_df, cleaned_df)
    final_report = {"file": os.path.basename(input_path), "config": os.path.basename(config_path), "steps": step_report, "dq": dq}
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(final_report, f, indent=2, ensure_ascii=False)
    return final_report


def main():
    """CLI entrypoint: batch-clean all CSVs in a directory."""
    ap = argparse.ArgumentParser(description="Batch clean all CSVs in a data directory.")
    ap.add_argument("--data-dir", default=os.path.join(os.path.dirname(os.path.dirname(__file__)), "data"), help="Directory containing CSV files")
    ap.add_argument("--out-dir", help="Output directory for cleaned CSVs and reports (default: <data-dir>/cleaned)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    args = ap.parse_args()

    data_dir = args.data_dir
    out_dir = args.out_dir or os.path.join(data_dir, "cleaned")
    os.makedirs(out_dir, exist_ok=True)

    files = find_csv_files(data_dir)
    if not files:
        print(f"No CSV files found in {data_dir}")
        return 0

    summaries = []
    for f in files:
        cfg = choose_config_for_file(f)
        base = os.path.splitext(os.path.basename(f))[0]
        out_csv = os.path.join(out_dir, f"{base}.cleaned.csv")
        out_rep = os.path.join(out_dir, f"{base}.report.json")
        if not args.overwrite and os.path.exists(out_csv) and os.path.exists(out_rep):
            print(f"Skip existing: {f}")
            continue
        print(f"Cleaning: {f} -> {out_csv} (config: {os.path.basename(cfg)})")
        rep = process_file(f, out_csv, out_rep, cfg)
        summaries.append(rep)

    print("\nBatch complete. Summary:")
    for s in summaries:
        print(f"- {s['file']} (config={s['config']}), rows_before={s['dq']['rows_before']}, rows_after={s['dq']['rows_after']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
