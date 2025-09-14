"""CLI to run the pandas-based cleaning pipeline on a single CSV.

It reads the raw CSV, loads a JSON cleaning configuration, executes the
orchestrated cleaning steps, writes the cleaned CSV, and optionally emits
an on-disk JSON report with per-step summaries and data-quality stats.
"""

import argparse, json, os, sys

# Make src/ importable when running directly
_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.config import load_config
from data_pipeline.pd_pipeline import run_cleaning_df, dq_report_df
import pandas as pd

def main():
    """Parse CLI arguments and run the cleaning pipeline.

    Arguments are taken from the command line; see --help for details.
    Writes a cleaned CSV and prints a JSON report to stdout. If --report
    is provided, it also saves the report to the given file path.
    """
    ap = argparse.ArgumentParser(description="Run pandas-based data cleaning pipeline.")
    ap.add_argument("--input", required=True, help="Input CSV")
    ap.add_argument("--output", required=True, help="Output cleaned CSV")
    ap.add_argument("--config", required=True, help="Cleaning config JSON")
    ap.add_argument("--report", required=False, help="Write JSON report to this path")
    args = ap.parse_args()

    # Read raw CSV into a DataFrame
    raw_df = pd.read_csv(args.input)
    cfg = load_config(args.config)

    # Execute all configured cleaning steps, collecting a per-step report
    cleaned_df, step_report = run_cleaning_df(raw_df, cfg)
    cleaned_df.to_csv(args.output, index=False)

    # Build an overall DQ report before/after cleaning
    dq = dq_report_df(raw_df, cleaned_df)
    final_report = {"steps": step_report, "dq": dq}

    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(final_report, f, indent=2, ensure_ascii=False)

    print("Cleaning completed.")
    print(json.dumps(final_report, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
