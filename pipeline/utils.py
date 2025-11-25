"""
Common utilities for the Equity Research Data Automation project.
"""

from pathlib import Path
import logging
from typing import Optional

import pandas as pd

# --- Paths -------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "sample_data"
OUTPUT_DIR = PROJECT_ROOT / "output"
LOG_DIR = PROJECT_ROOT / "logs"

for d in (OUTPUT_DIR, LOG_DIR):
    d.mkdir(exist_ok=True)

# --- Logging -----------------------------------------------------------------

LOG_FILE = LOG_DIR / "pipeline.log"

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("equity_pipeline")


# --- Helper functions --------------------------------------------------------


def load_csv(path: Path) -> pd.DataFrame:
    """Load a CSV file into a DataFrame with basic logging."""
    logger.info("Loading CSV: %s", path)
    df = pd.read_csv(path)
    logger.info("Loaded %s rows x %s cols from %s", len(df), len(df.columns), path.name)
    return df


def save_df(df: pd.DataFrame, path: Path, index: bool = False) -> None:
    """Save a DataFrame to CSV with logging."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=index)
    logger.info(
        "Saved DataFrame (%s rows x %s cols) to %s",
        len(df),
        len(df.columns),
        path,
    )


def parse_date_column(
    df: pd.DataFrame,
    column: str = "date",
    dayfirst: bool = False,
    errors: str = "coerce",
) -> pd.DataFrame:
    """Convert a date column to pandas datetime."""
    df[column] = pd.to_datetime(df[column], dayfirst=dayfirst, errors=errors)
    return df


def detect_price_anomalies(
    df: pd.DataFrame,
    price_col: str = "price",
    window: int = 20,
    z_threshold: float = 3.0,
    group_col: Optional[str] = "ticker",
) -> pd.DataFrame:
    """
    Add a boolean 'price_anomaly' column based on rolling Z-score of prices.

    - window: rolling window size
    - z_threshold: abs(z) above which we flag an anomaly
    """
    logger.info(
        "Running anomaly detection on %s (window=%s, z_threshold=%s)",
        price_col,
        window,
        z_threshold,
    )

    df = df.copy()

    if group_col and group_col in df.columns:
        grouped = df.groupby(group_col)
    else:
        grouped = [(None, df)]

    anomaly_flags = []

    for _, g in grouped:
        # Work on a copy to avoid SettingWithCopy issues
        s = g[price_col].astype(float)
        rolling_mean = s.rolling(window=window, min_periods=window // 2).mean()
        rolling_std = s.rolling(window=window, min_periods=window // 2).std()

        z_scores = (s - rolling_mean) / rolling_std
        flags = z_scores.abs() > z_threshold
        anomaly_flags.append(flags)

    # Concatenate all boolean Series back in original order
    df["price_anomaly"] = pd.concat(anomaly_flags).sort_index()
    return df