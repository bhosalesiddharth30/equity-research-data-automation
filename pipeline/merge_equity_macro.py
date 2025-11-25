"""
Step 3: Merge Equity + Macro + Metadata and create final research-ready dataset.

Outputs:
- output/03_merged/equity_macro_merged.csv
with anomaly flags and sector info.
"""

from pathlib import Path

import pandas as pd

from utils import OUTPUT_DIR, load_csv, save_df, detect_price_anomalies, logger


def main() -> None:
    logger.info("=== STEP 3: MERGE + ANOMALY DETECTION STARTED ===")

    clean_dir = OUTPUT_DIR / "02_clean"
    merged_dir = OUTPUT_DIR / "03_merged"

    # Load cleaned datasets
    equity = load_csv(clean_dir / "equity_clean.csv")
    macro = load_csv(clean_dir / "macro_clean.csv")
    meta = load_csv(clean_dir / "company_metadata_clean.csv")

    # Merge equity + metadata (to attach sector or other info)
    equity_meta = equity.merge(meta, on="ticker", how="left")

    # Merge with macro data on date
    merged = equity_meta.merge(macro, on="date", how="left", suffixes=("", "_macro"))

    # Detect price anomalies
    merged = detect_price_anomalies(
        merged,
        price_col="price",
        window=20,
        z_threshold=3.0,
        group_col="ticker",
    )

    # Save merged dataset
    merged_path = merged_dir / "equity_macro_merged.csv"
    save_df(merged, merged_path)

    logger.info("=== STEP 3: MERGE + ANOMALY DETECTION COMPLETED ===")


if __name__ == "__main__":
    main()
