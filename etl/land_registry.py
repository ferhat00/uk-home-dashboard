"""
HM Land Registry Price Paid Data ETL.

Downloads and processes the Price Paid dataset, filtering to areas of interest.
Source: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
"""
import logging
import sqlite3
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

import config

logger = logging.getLogger(__name__)

# Column names for the Price Paid CSV (no header in file)
PP_COLUMNS = [
    "transaction_id", "price", "date", "postcode", "property_type",
    "old_new", "duration", "paon", "saon", "street", "locality",
    "town", "district", "county", "ppd_category", "record_status",
]

# URL for the complete dataset (CSV)
PP_COMPLETE_URL = (
    "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/"
    "pp-complete.csv"
)
PP_MONTHLY_URL = (
    "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/"
    "pp-monthly-update-new-version.csv"
)


def _postcode_districts() -> list[str]:
    """Return list of postcode district prefixes from config."""
    return [a["code"].upper() for a in config.AREAS_OF_INTEREST]


def download_data(use_monthly: bool = True) -> Path:
    """Download Price Paid CSV. Use monthly update by default (smaller)."""
    url = PP_MONTHLY_URL if use_monthly else PP_COMPLETE_URL
    filename = "pp-monthly-update.csv" if use_monthly else "pp-complete.csv"
    dest = config.RAW_DIR / filename

    if dest.exists():
        logger.info("Price Paid file already exists: %s", dest)
        return dest

    logger.info("Downloading Price Paid data from %s ...", url)
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    with open(dest, "wb") as f:
        with tqdm(total=total, unit="B", unit_scale=True, desc="Land Registry") as pbar:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                pbar.update(len(chunk))

    logger.info("Downloaded to %s", dest)
    return dest


def load_and_filter(csv_path: Path) -> pd.DataFrame:
    """Load CSV and filter to areas of interest."""
    logger.info("Loading Price Paid CSV: %s", csv_path)

    df = pd.read_csv(
        csv_path,
        header=None,
        names=PP_COLUMNS,
        parse_dates=["date"],
        low_memory=False,
    )

    # Clean postcode
    df["postcode"] = df["postcode"].astype(str).str.strip()
    df["postcode_district"] = df["postcode"].str.extract(r"^([A-Z]+\d+)", expand=False)

    districts = _postcode_districts()
    mask = df["postcode_district"].isin(districts)
    filtered = df[mask].copy()

    logger.info(
        "Filtered %d → %d rows for districts: %s",
        len(df), len(filtered), districts,
    )

    # Derived columns
    filtered["postcode_sector"] = filtered["postcode"].str.extract(
        r"^([A-Z]+\d+\s?\d)", expand=False
    )
    filtered["year"] = filtered["date"].dt.year
    filtered["month"] = filtered["date"].dt.to_period("M").astype(str)

    return filtered


def compute_summaries(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Compute summary statistics per postcode district."""
    summaries = {}

    # Median price by district and year
    summaries["yearly_prices"] = (
        df.groupby(["postcode_district", "year"])["price"]
        .agg(["median", "mean", "count"])
        .reset_index()
        .rename(columns={"median": "median_price", "mean": "mean_price", "count": "transactions"})
    )

    # Median price by district and property type
    summaries["prices_by_type"] = (
        df.groupby(["postcode_district", "property_type"])["price"]
        .agg(["median", "mean", "count"])
        .reset_index()
        .rename(columns={"median": "median_price", "mean": "mean_price", "count": "transactions"})
    )

    # Latest year stats per district
    latest_year = df["year"].max()
    recent = df[df["year"] >= latest_year - 1]
    summaries["recent_transactions"] = (
        recent.sort_values("date", ascending=False)
        .head(500)
        [["date", "price", "postcode", "property_type", "paon", "street", "town", "postcode_district"]]
    )

    # Price trends: calculate YoY change
    yearly = summaries["yearly_prices"].copy()
    yearly = yearly.sort_values(["postcode_district", "year"])
    yearly["prev_median"] = yearly.groupby("postcode_district")["median_price"].shift(1)
    yearly["yoy_change"] = (
        (yearly["median_price"] - yearly["prev_median"]) / yearly["prev_median"] * 100
    ).round(2)
    summaries["yearly_prices"] = yearly

    return summaries


def save_to_db(df: pd.DataFrame, summaries: dict[str, pd.DataFrame]) -> None:
    """Save processed data to SQLite."""
    db_path = config.DATABASE_PATH
    logger.info("Saving Land Registry data to %s", db_path)

    with sqlite3.connect(db_path) as conn:
        df.to_sql("land_registry_transactions", conn, if_exists="replace", index=False)
        for name, summary_df in summaries.items():
            summary_df.to_sql(f"land_registry_{name}", conn, if_exists="replace", index=False)

    logger.info("Saved Land Registry data to database.")


def run(use_monthly: bool = True) -> None:
    """Run the full Land Registry ETL pipeline."""
    logger.info("=== Land Registry ETL starting ===")
    try:
        csv_path = download_data(use_monthly=use_monthly)
        df = load_and_filter(csv_path)
        if df.empty:
            logger.warning("No data found for areas of interest.")
            _save_empty_tables()
            return
        summaries = compute_summaries(df)
        save_to_db(df, summaries)
        logger.info("=== Land Registry ETL complete ===")
    except Exception:
        logger.exception("Land Registry ETL failed")
        _save_empty_tables()


def _save_empty_tables() -> None:
    """Create empty tables so the app doesn't crash."""
    with sqlite3.connect(config.DATABASE_PATH) as conn:
        for table in [
            "land_registry_transactions",
            "land_registry_yearly_prices",
            "land_registry_prices_by_type",
            "land_registry_recent_transactions",
        ]:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS [{table}] (placeholder TEXT)"
            )
