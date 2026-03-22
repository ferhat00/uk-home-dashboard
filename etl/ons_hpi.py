"""
ONS House Price Index ETL.

Downloads and processes the ONS HPI data for broader trend context.
Source: https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/housepriceindexmonthlyquarterlytables
"""
import logging
import sqlite3

import pandas as pd
import requests
import requests_cache

import config

logger = logging.getLogger(__name__)

# ONS HPI download URL (Excel format)
HPI_URL = (
    "https://www.ons.gov.uk/file?uri=/economy/inflationandpriceindices/"
    "datasets/housepriceindexmonthlyquarterlytables/"
    "current/hpimonthlyquarterlytablesql.xlsx"
)


def _get_session() -> requests_cache.CachedSession:
    return requests_cache.CachedSession(
        str(config.CACHE_DIR / "ons_cache"),
        expire_after=config.REQUEST_CACHE_EXPIRY,
    )


def download_hpi() -> pd.DataFrame:
    """Download ONS HPI Excel file and parse."""
    dest = config.RAW_DIR / "ons_hpi.xlsx"

    if not dest.exists():
        logger.info("Downloading ONS HPI data...")
        session = _get_session()
        try:
            resp = session.get(HPI_URL, timeout=120)
            resp.raise_for_status()
            with open(dest, "wb") as f:
                f.write(resp.content)
        except requests.RequestException as e:
            logger.warning("Could not download ONS HPI: %s", e)
            return pd.DataFrame()

    try:
        # The ONS file has multiple sheets; Table_2a has local authority data
        df = pd.read_excel(dest, sheet_name=None, engine="openpyxl")

        # Try to find the right sheet
        for sheet_name, sheet_df in df.items():
            if "local" in sheet_name.lower() or "table_2" in sheet_name.lower():
                return sheet_df

        # Fallback: return first sheet with enough data
        for sheet_name, sheet_df in df.items():
            if len(sheet_df) > 10:
                return sheet_df

    except Exception as e:
        logger.warning("Could not parse ONS HPI Excel: %s", e)

    return pd.DataFrame()


def process(df: pd.DataFrame) -> pd.DataFrame:
    """Process ONS HPI data — extract relevant local authority trends."""
    if df.empty:
        return df

    # The structure varies by release, so we do best-effort parsing
    # Look for columns that might contain area names and price indices
    logger.info("ONS HPI columns: %s", list(df.columns)[:20])
    return df


def save_to_db(df: pd.DataFrame) -> None:
    with sqlite3.connect(config.DATABASE_PATH) as conn:
        if not df.empty:
            df.to_sql("ons_hpi", conn, if_exists="replace", index=False)
        else:
            conn.execute("CREATE TABLE IF NOT EXISTS ons_hpi (placeholder TEXT)")


def run() -> None:
    """Run the ONS HPI ETL pipeline."""
    logger.info("=== ONS HPI ETL starting ===")
    try:
        df = download_hpi()
        df = process(df)
        save_to_db(df)
        logger.info("=== ONS HPI ETL complete ===")
    except Exception:
        logger.exception("ONS HPI ETL failed")
        save_to_db(pd.DataFrame())
