"""
Amenities ETL — Overpass API (OpenStreetMap).

Queries for shops, cafes, healthcare, parks, etc. near areas of interest.
Source: https://overpass-api.de/
"""
import logging
import math
import sqlite3
import time

import pandas as pd
import requests
import requests_cache

import config

logger = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Amenity query definitions: (label, overpass filter)
AMENITY_QUERIES = {
    "supermarket": [
        '["shop"="supermarket"]',
    ],
    "supermarket_premium": [
        '["shop"="supermarket"]["brand"~"Waitrose|Marks|M&S"]',
    ],
    "supermarket_budget": [
        '["shop"="supermarket"]["brand"~"Aldi|Lidl"]',
    ],
    "bookshop": [
        '["shop"="books"]',
    ],
    "waterstones": [
        '["shop"="books"]["brand"="Waterstones"]',
    ],
    "cafe": [
        '["amenity"="cafe"]',
    ],
    "gails_bakery": [
        '["amenity"="cafe"]["brand"~"GAIL|Gail"]',
        '["amenity"="cafe"]["name"~"GAIL|Gail"]',
    ],
    "pharmacy": [
        '["amenity"="pharmacy"]',
    ],
    "gp_surgery": [
        '["amenity"="doctors"]',
        '["healthcare"="doctor"]',
    ],
    "park": [
        '["leisure"="park"]',
    ],
    "gym": [
        '["leisure"="fitness_centre"]',
        '["leisure"="sports_centre"]',
    ],
    "library": [
        '["amenity"="library"]',
    ],
    "pub": [
        '["amenity"="pub"]',
    ],
    "restaurant": [
        '["amenity"="restaurant"]',
    ],
}


def _bbox_from_center(lat: float, lng: float, radius_km: float = 3.0) -> str:
    """Create bounding box string for Overpass from center + radius."""
    dlat = radius_km / 111.32
    dlng = radius_km / (111.32 * math.cos(math.radians(lat)))
    s = lat - dlat
    n = lat + dlat
    w = lng - dlng
    e = lng + dlng
    return f"{s},{w},{n},{e}"


def _get_session() -> requests_cache.CachedSession:
    return requests_cache.CachedSession(
        str(config.CACHE_DIR / "amenities_cache"),
        expire_after=config.REQUEST_CACHE_EXPIRY,
    )


def query_overpass(
    session: requests.Session,
    filters: list[str],
    bbox: str,
) -> list[dict]:
    """Run an Overpass query and return elements."""
    union_parts = []
    for filt in filters:
        union_parts.append(f'  node{filt}({bbox});')
        union_parts.append(f'  way{filt}({bbox});')

    query = (
        f"[out:json][timeout:{config.OVERPASS_TIMEOUT}];\n"
        f"(\n"
        + "\n".join(union_parts)
        + "\n);\nout center;"
    )

    try:
        resp = session.post(OVERPASS_URL, data={"data": query}, timeout=60)
        if resp.status_code == 429:
            logger.warning("Overpass rate limited, waiting 30s...")
            time.sleep(30)
            resp = session.post(OVERPASS_URL, data={"data": query}, timeout=60)
        resp.raise_for_status()
        return resp.json().get("elements", [])
    except requests.RequestException as e:
        logger.warning("Overpass query failed: %s", e)
        return []


def fetch_all() -> pd.DataFrame:
    """Fetch all amenity types for all areas."""
    session = _get_session()
    all_records = []

    for area in config.AREAS_OF_INTEREST:
        code = area["code"]
        bbox = _bbox_from_center(area["lat"], area["lng"])
        logger.info("Fetching amenities for %s (%s)...", code, area["name"])

        for category, filters in AMENITY_QUERIES.items():
            elements = query_overpass(session, filters, bbox)

            for el in elements:
                lat = el.get("lat") or el.get("center", {}).get("lat")
                lng = el.get("lon") or el.get("center", {}).get("lon")
                tags = el.get("tags", {})

                all_records.append({
                    "area_code": code,
                    "area_name": area["name"],
                    "category": category,
                    "name": tags.get("name", ""),
                    "brand": tags.get("brand", ""),
                    "lat": lat,
                    "lng": lng,
                    "osm_id": el.get("id"),
                })

            time.sleep(1.0)  # Be nice to Overpass

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    # Deduplicate by osm_id + category
    df = df.drop_duplicates(subset=["osm_id", "category"])
    return df


def compute_summaries(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Compute amenity counts and density scores."""
    if df.empty:
        return {}

    summaries = {}

    # Count by area and category
    summaries["counts"] = (
        df.groupby(["area_code", "area_name", "category"])
        .size()
        .reset_index(name="count")
    )

    # Pivot: one row per area, columns = categories
    pivot = summaries["counts"].pivot_table(
        index=["area_code", "area_name"],
        columns="category",
        values="count",
        fill_value=0,
    ).reset_index()
    pivot.columns.name = None
    summaries["density"] = pivot

    return summaries


def save_to_db(df: pd.DataFrame, summaries: dict[str, pd.DataFrame]) -> None:
    with sqlite3.connect(config.DATABASE_PATH) as conn:
        if not df.empty:
            df.to_sql("amenities", conn, if_exists="replace", index=False)
        else:
            conn.execute("CREATE TABLE IF NOT EXISTS amenities (placeholder TEXT)")
        for name, sdf in summaries.items():
            sdf.to_sql(f"amenities_{name}", conn, if_exists="replace", index=False)


def run() -> None:
    """Run the Amenities ETL pipeline."""
    logger.info("=== Amenities ETL starting ===")
    try:
        df = fetch_all()
        summaries = compute_summaries(df)
        save_to_db(df, summaries)
        logger.info("=== Amenities ETL complete: %d POIs ===", len(df))
    except Exception:
        logger.exception("Amenities ETL failed")
        _save_empty()


def _save_empty() -> None:
    with sqlite3.connect(config.DATABASE_PATH) as conn:
        for t in ["amenities", "amenities_counts", "amenities_density"]:
            conn.execute(f"CREATE TABLE IF NOT EXISTS [{t}] (placeholder TEXT)")
