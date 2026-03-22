"""
Police.uk Crime Data ETL.

Fetches street-level crime data for areas of interest.
Source: https://data.police.uk/docs/
"""
import logging
import sqlite3
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
import requests_cache

import config

logger = logging.getLogger(__name__)

CRIMES_URL = "https://data.police.uk/api/crimes-street/all-crime"
CATEGORIES_URL = "https://data.police.uk/api/crime-categories"


def _get_session() -> requests_cache.CachedSession:
    return requests_cache.CachedSession(
        str(config.CACHE_DIR / "crime_cache"),
        expire_after=config.REQUEST_CACHE_EXPIRY,
    )


def _get_months(n: int = 12) -> list[str]:
    """Return last n months as YYYY-MM strings."""
    months = []
    now = datetime.now()
    for i in range(1, n + 1):
        dt = now - timedelta(days=30 * i)
        months.append(dt.strftime("%Y-%m"))
    return months


def fetch_crime_categories(session: requests.Session) -> dict[str, str]:
    """Fetch crime category labels."""
    try:
        resp = session.get(CATEGORIES_URL, timeout=15)
        resp.raise_for_status()
        return {c["url"]: c["name"] for c in resp.json()}
    except requests.RequestException:
        logger.warning("Could not fetch crime categories")
        return {}


def fetch_crimes_for_location(
    session: requests.Session, lat: float, lng: float, date: str
) -> list[dict]:
    """Fetch crimes near a lat/lng for a given month."""
    try:
        resp = session.get(
            CRIMES_URL,
            params={"lat": lat, "lng": lng, "date": date},
            timeout=15,
        )
        if resp.status_code == 503:
            logger.debug("Police API 503 for %s at %s/%s — skipping", date, lat, lng)
            return []
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.debug("Crime fetch failed for %s %s/%s: %s", date, lat, lng, e)
        return []


def fetch_all() -> pd.DataFrame:
    """Fetch 12 months of crime data for all areas."""
    session = _get_session()
    months = _get_months(12)
    categories = fetch_crime_categories(session)
    all_crimes = []

    for area in config.AREAS_OF_INTEREST:
        code = area["code"]
        lat, lng = area["lat"], area["lng"]
        logger.info("Fetching crime data for %s (%s)...", code, area["name"])

        for month in months:
            crimes = fetch_crimes_for_location(session, lat, lng, month)
            for c in crimes:
                all_crimes.append({
                    "area_code": code,
                    "area_name": area["name"],
                    "month": c.get("month", month),
                    "category": c.get("category", "unknown"),
                    "category_name": categories.get(c.get("category", ""), c.get("category", "")),
                    "location_lat": c.get("location", {}).get("latitude"),
                    "location_lng": c.get("location", {}).get("longitude"),
                    "street_name": c.get("location", {}).get("street", {}).get("name", ""),
                    "outcome": c.get("outcome_status", {}).get("category", "") if c.get("outcome_status") else "",
                })
            time.sleep(0.1)  # Stay under 15 req/s

    if not all_crimes:
        return pd.DataFrame()

    return pd.DataFrame(all_crimes)


def compute_summaries(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Compute crime summaries."""
    if df.empty:
        return {}

    summaries = {}

    # Total crimes by area and category
    summaries["by_category"] = (
        df.groupby(["area_code", "area_name", "category", "category_name"])
        .size()
        .reset_index(name="count")
        .sort_values(["area_code", "count"], ascending=[True, False])
    )

    # Monthly trend by area
    summaries["monthly_trend"] = (
        df.groupby(["area_code", "area_name", "month"])
        .size()
        .reset_index(name="count")
        .sort_values(["area_code", "month"])
    )

    # Total crimes per area (for scoring)
    summaries["totals"] = (
        df.groupby(["area_code", "area_name"])
        .size()
        .reset_index(name="total_crimes")
        .sort_values("total_crimes")
    )

    return summaries


def save_to_db(df: pd.DataFrame, summaries: dict[str, pd.DataFrame]) -> None:
    with sqlite3.connect(config.DATABASE_PATH) as conn:
        if not df.empty:
            df.to_sql("crime_data", conn, if_exists="replace", index=False)
        else:
            conn.execute("CREATE TABLE IF NOT EXISTS crime_data (placeholder TEXT)")
        for name, sdf in summaries.items():
            sdf.to_sql(f"crime_{name}", conn, if_exists="replace", index=False)


def run() -> None:
    """Run the Crime ETL pipeline."""
    logger.info("=== Crime ETL starting ===")
    try:
        df = fetch_all()
        summaries = compute_summaries(df)
        save_to_db(df, summaries)
        logger.info("=== Crime ETL complete: %d records ===", len(df))
    except Exception:
        logger.exception("Crime ETL failed")
        _save_empty()


def _save_empty() -> None:
    with sqlite3.connect(config.DATABASE_PATH) as conn:
        for t in ["crime_data", "crime_by_category", "crime_monthly_trend", "crime_totals"]:
            conn.execute(f"CREATE TABLE IF NOT EXISTS [{t}] (placeholder TEXT)")
