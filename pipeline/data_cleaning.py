"""
Step 2: Data Cleaning

Reads raw CSVs from output/01_raw and produces cleaned versions in
output/02_clean. Tasks include:
- date parsing
- sorting
- dropping duplicates
- handling missing values
"""

from pathlib import Path

import pandas as pd

from utils import OUTPUT_DIR, parse_date_column, save_df, load_csv, logger


def clean_equity(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Expected columns: date, ticker, price, volume (you can adjust)
    df = parse_date_column(df, "date")
    df = df.dropna(subset=["date", "ticker", "price"])
    df = df.drop_duplicates(subset=["date", "ticker"])

    # Sort for time-series operations
    df = df.sort_values(["ticker", "date"])

    # Simple forward-fill for missing volume/price if any
    df["price"] = df["price"].astype(float)
    if "volume" in df.columns:
        df["volume"] = df["volume"].astype(float)
        df["volume"] = df.groupby("ticker")["volume"].ffill()

    return df


def clean_macro(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = parse_date_column(df, "date")
    df = df.dropna(subset=["date"])
    df = df.drop_duplicates(subset=["date"])
    df = df.sort_values("date")

    # Example: forward-fill macro indicators
    for col in df.columns:
        if col != "date":
            df[col] = df[col].astype(float)
            df[col] = df[col].ffill()

    return df


def clean_metadata(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.dropna(subset=["ticker"])
    df = df.drop_duplicates(subset=["ticker"])
    df["ticker"] = df["ticker"].str.upper().str.strip()
    if "sector" in df.columns:
        df["sector"] = df["sector"].str.strip()

    return df


def main() -> None:
    logger.info("=== STEP 2: DATA CLEANING STARTED ===")

    raw_dir = OUTPUT_DIR / "01_raw"
    clean_dir = OUTPUT_DIR / "02_clean"

    # Load raw
    equity_raw = load_csv(raw_dir / "equity_raw.csv")
    macro_raw = load_csv(raw_dir / "macro_raw.csv")
    meta_raw = load_csv(raw_dir / "company_metadata_raw.csv")

    # Clean
    equity_clean = clean_equity(equity_raw)
    macro_clean = clean_macro(macro_raw)
    meta_clean = clean_metadata(meta_raw)

    # Save
    save_df(equity_clean, clean_dir / "equity_clean.csv")
    save_df(macro_clean, clean_dir / "macro_clean.csv")
    save_df(meta_clean, clean_dir / "company_metadata_clean.csv")

    logger.info("=== STEP 2: DATA CLEANING COMPLETED ===")


if __name__ == "__main__":
    main()
