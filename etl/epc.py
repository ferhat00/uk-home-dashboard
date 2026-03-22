"""
EPC (Energy Performance Certificate) Register ETL.

Fetches property energy ratings and floor area data from the EPC API.
Source: https://epc.opendatacommunities.org/
"""
import logging
import sqlite3
import time

import pandas as pd
import requests
import requests_cache

import config

logger = logging.getLogger(__name__)

API_BASE = "https://epc.opendatacommunities.org/api/v1/domestic/search"


def _get_session() -> requests.Session:
    """Create a cached session with auth headers."""
    session = requests_cache.CachedSession(
        str(config.CACHE_DIR / "epc_cache"),
        expire_after=config.REQUEST_CACHE_EXPIRY,
    )
    if config.EPC_API_KEY:
        session.headers.update({
            "Authorization": f"Basic {config.EPC_API_KEY}",
            "Accept": "application/json",
        })
    return session


def fetch_epc_for_postcode(session: requests.Session, postcode: str) -> list[dict]:
    """Fetch EPC records for a postcode district."""
    all_rows = []
    page_size = 100
    search_after = None

    while True:
        params = {
            "postcode": postcode,
            "size": page_size,
        }
        if search_after:
            params["search-after"] = search_after

        try:
            resp = session.get(API_BASE, params=params, timeout=30)
            if resp.status_code == 401:
                logger.warning("EPC API: unauthorized — check EPC_API_KEY")
                return all_rows
            if resp.status_code == 429:
                logger.warning("EPC API rate limited, waiting 60s...")
                time.sleep(60)
                continue
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("EPC API request failed for %s: %s", postcode, e)
            return all_rows

        data = resp.json()
        rows = data.get("rows", [])
        if not rows:
            break

        all_rows.extend(rows)
        search_after = data.get("search-after")
        if not search_after or len(rows) < page_size:
            break

        time.sleep(1.0 / config.EPC_API_RATE_LIMIT)

    return all_rows


def fetch_all() -> pd.DataFrame:
    """Fetch EPC data for all areas of interest."""
    session = _get_session()
    all_records = []

    for area in config.AREAS_OF_INTEREST:
        code = area["code"]
        logger.info("Fetching EPC data for %s (%s)...", code, area["name"])
        records = fetch_epc_for_postcode(session, code)
        logger.info("  Got %d EPC records for %s", len(records), code)
        all_records.extend(records)
        time.sleep(1.0)

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    return df


def process(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and process EPC data."""
    if df.empty:
        return df

    # Select key columns (handle missing gracefully)
    keep_cols = [
        "postcode", "address1", "address2", "local-authority",
        "current-energy-rating", "potential-energy-rating",
        "current-energy-efficiency", "potential-energy-efficiency",
        "property-type", "built-form", "total-floor-area",
        "number-habitable-rooms", "construction-age-band",
        "main-heating-description", "walls-description",
        "lodgement-date",
    ]
    existing = [c for c in keep_cols if c in df.columns]
    df = df[existing].copy()

    # Rename columns to underscore style
    df.columns = [c.replace("-", "_") for c in df.columns]

    # Parse types
    if "total_floor_area" in df.columns:
        df["total_floor_area"] = pd.to_numeric(df["total_floor_area"], errors="coerce")
    if "current_energy_efficiency" in df.columns:
        df["current_energy_efficiency"] = pd.to_numeric(
            df["current_energy_efficiency"], errors="coerce"
        )
    if "lodgement_date" in df.columns:
        df["lodgement_date"] = pd.to_datetime(df["lodgement_date"], errors="coerce")

    # Extract postcode district
    if "postcode" in df.columns:
        df["postcode"] = df["postcode"].astype(str).str.strip()
        df["postcode_district"] = df["postcode"].str.extract(r"^([A-Z]+\d+)", expand=False)

    return df


def compute_summaries(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Compute EPC summary statistics."""
    if df.empty:
        return {}

    summaries = {}

    if "postcode_district" in df.columns and "total_floor_area" in df.columns:
        summaries["floor_area_by_district"] = (
            df.groupby("postcode_district")["total_floor_area"]
            .agg(["median", "mean", "count"])
            .reset_index()
            .rename(columns={"median": "median_m2", "mean": "mean_m2", "count": "n_records"})
        )

    if "postcode_district" in df.columns and "current_energy_rating" in df.columns:
        rating_counts = (
            df.groupby(["postcode_district", "current_energy_rating"])
            .size()
            .reset_index(name="count")
        )
        summaries["energy_ratings"] = rating_counts

    return summaries


def save_to_db(df: pd.DataFrame, summaries: dict[str, pd.DataFrame]) -> None:
    """Save to SQLite."""
    with sqlite3.connect(config.DATABASE_PATH) as conn:
        if not df.empty:
            df.to_sql("epc_certificates", conn, if_exists="replace", index=False)
        else:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS epc_certificates (placeholder TEXT)"
            )
        for name, sdf in summaries.items():
            sdf.to_sql(f"epc_{name}", conn, if_exists="replace", index=False)


def run() -> None:
    """Run the EPC ETL pipeline."""
    logger.info("=== EPC ETL starting ===")
    try:
        if not config.EPC_API_KEY:
            logger.warning("EPC_API_KEY not set — skipping EPC ETL. Set it in .env")
            _save_empty()
            return
        df = fetch_all()
        df = process(df)
        summaries = compute_summaries(df)
        save_to_db(df, summaries)
        logger.info("=== EPC ETL complete: %d records ===", len(df))
    except Exception:
        logger.exception("EPC ETL failed")
        _save_empty()


def _save_empty() -> None:
    with sqlite3.connect(config.DATABASE_PATH) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS epc_certificates (placeholder TEXT)"
        )
