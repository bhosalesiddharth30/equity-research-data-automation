"""
Step 1: Data Ingestion

For this demo project, "ingestion" means loading CSVs from sample_data/
and writing them into output/01_raw_*.csv so later steps have a stable input.
In a real project, this script would call APIs, databases, etc.
"""

from pathlib import Path

from utils import DATA_DIR, OUTPUT_DIR, load_csv, save_df, logger


def main() -> None:
    logger.info("=== STEP 1: DATA INGESTION STARTED ===")

    # Source files (provided as synthetic samples)
    equity_src = DATA_DIR / "equity_prices_sample.csv"
    macro_src = DATA_DIR / "macro_data_sample.csv"
    meta_src = DATA_DIR / "company_metadata.csv"

    # Load
    equity_df = load_csv(equity_src)
    macro_df = load_csv(macro_src)
    meta_df = load_csv(meta_src)

    # Destination files
    raw_dir = OUTPUT_DIR / "01_raw"
    equity_raw_path = raw_dir / "equity_raw.csv"
    macro_raw_path = raw_dir / "macro_raw.csv"
    meta_raw_path = raw_dir / "company_metadata_raw.csv"

    # Save
    save_df(equity_df, equity_raw_path)
    save_df(macro_df, macro_raw_path)
    save_df(meta_df, meta_raw_path)

    logger.info("=== STEP 1: DATA INGESTION COMPLETED ===")


if __name__ == "__main__":
    main()
