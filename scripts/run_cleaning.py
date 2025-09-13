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
    ap = argparse.ArgumentParser(description="Run stdlib-only data cleaning pipeline.")
    ap.add_argument("--input", required=True, help="Input CSV")
    ap.add_argument("--output", required=True, help="Output cleaned CSV")
    ap.add_argument("--config", required=True, help="Cleaning config JSON")
    ap.add_argument("--report", required=False, help="Write JSON report to this path")
    args = ap.parse_args()

    raw_df = pd.read_csv(args.input)
    cfg = load_config(args.config)

    cleaned_df, step_report = run_cleaning_df(raw_df, cfg)
    cleaned_df.to_csv(args.output, index=False)

    dq = dq_report_df(raw_df, cleaned_df)
    final_report = {"steps": step_report, "dq": dq}

    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(final_report, f, indent=2, ensure_ascii=False)

    print("Cleaning completed.")
    print(json.dumps(final_report, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
