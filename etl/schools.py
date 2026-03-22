"""
Schools ETL — GIAS (Get Information About Schools) + Performance data.

Sources:
- GIAS: https://get-information-schools.service.gov.uk/Downloads
- Performance: https://www.find-school-performance-data.service.gov.uk/download-data
"""
import logging
import sqlite3
import math

import pandas as pd
import requests
import requests_cache

import config

logger = logging.getLogger(__name__)

GIAS_URL = (
    "https://get-information-schools.service.gov.uk/Downloads/"
    "DownloadFile?fileType=csv&fileName=edubasealldata"
)


def _get_session() -> requests_cache.CachedSession:
    return requests_cache.CachedSession(
        str(config.CACHE_DIR / "schools_cache"),
        expire_after=config.REQUEST_CACHE_EXPIRY,
    )


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in km between two points."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def download_gias() -> pd.DataFrame:
    """Download GIAS school data."""
    dest = config.RAW_DIR / "gias_schools.csv"

    if not dest.exists():
        logger.info("Downloading GIAS data...")
        session = _get_session()
        try:
            resp = session.get(GIAS_URL, timeout=120)
            resp.raise_for_status()
            with open(dest, "wb") as f:
                f.write(resp.content)
            logger.info("GIAS data downloaded to %s", dest)
        except requests.RequestException as e:
            logger.warning("Could not download GIAS data: %s", e)
            return pd.DataFrame()

    try:
        df = pd.read_csv(dest, encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(dest, encoding="latin-1", low_memory=False)

    return df


def filter_schools(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to open schools near areas of interest."""
    if df.empty:
        return df

    # Identify relevant columns (GIAS has many)
    col_map = {}
    for col in df.columns:
        cl = col.lower().strip()
        if "establishmentname" in cl.replace(" ", ""):
            col_map["name"] = col
        elif cl in ("urn",):
            col_map["urn"] = col
        elif "typeofeducation" in cl.replace(" ", "") or cl == "typeofeducation (name)":
            col_map["education_type"] = col
        elif "phaseofeducation" in cl.replace(" ", "") and "name" in cl.lower():
            col_map["phase"] = col
        elif cl == "postcode":
            col_map["postcode"] = col
        elif "ofstedrating" in cl.replace(" ", "") and "name" in cl.lower():
            col_map["ofsted_rating"] = col
        elif cl == "numberofpupils":
            col_map["num_pupils"] = col
        elif "establishmentstatus" in cl.replace(" ", "") and "name" in cl.lower():
            col_map["status"] = col
        elif cl == "easting":
            col_map["easting"] = col
        elif cl == "northing":
            col_map["northing"] = col

    # Filter open schools only
    if "status" in col_map:
        df = df[df[col_map["status"]].astype(str).str.contains("Open", case=False, na=False)]

    # Filter by phases we care about
    if "phase" in col_map:
        phases = ["Primary", "Secondary", "All through", "Middle"]
        mask = df[col_map["phase"]].astype(str).str.contains("|".join(phases), case=False, na=False)
        df = df[mask]

    # Filter by proximity to areas of interest
    # Use postcode district matching as a first pass
    if "postcode" in col_map:
        df["_pc"] = df[col_map["postcode"]].astype(str).str.strip()
        df["_pc_district"] = df["_pc"].str.extract(r"^([A-Z]+\d+)", expand=False)
        area_codes = [a["code"].upper() for a in config.AREAS_OF_INTEREST]

        # Include schools in area districts + neighbouring ones
        # Expand search radius by including any district that starts with same letters
        prefixes = set()
        for code in area_codes:
            prefixes.add(code)
            # Also add codes that share the letter prefix
            letters = "".join(c for c in code if c.isalpha())
            for num in range(1, 30):
                prefixes.add(f"{letters}{num}")

        df = df[df["_pc_district"].isin(prefixes)]

    # Build clean output
    result = pd.DataFrame()
    if "urn" in col_map:
        result["urn"] = df[col_map["urn"]]
    if "name" in col_map:
        result["school_name"] = df[col_map["name"]]
    if "phase" in col_map:
        result["phase"] = df[col_map["phase"]]
    if "postcode" in col_map:
        result["postcode"] = df[col_map["postcode"]].astype(str).str.strip()
        result["postcode_district"] = result["postcode"].str.extract(r"^([A-Z]+\d+)", expand=False)
    if "ofsted_rating" in col_map:
        result["ofsted_rating"] = df[col_map["ofsted_rating"]]
    if "num_pupils" in col_map:
        result["num_pupils"] = pd.to_numeric(df[col_map["num_pupils"]], errors="coerce")

    # Assign lat/lng from area centroids (simplified — ideally geocode each)
    area_lookup = {a["code"]: a for a in config.AREAS_OF_INTEREST}
    if "postcode_district" in result.columns:
        result["lat"] = result["postcode_district"].map(
            lambda x: area_lookup.get(x, {}).get("lat")
        )
        result["lng"] = result["postcode_district"].map(
            lambda x: area_lookup.get(x, {}).get("lng")
        )

    return result.reset_index(drop=True)


def compute_summaries(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Compute school summaries per area."""
    if df.empty:
        return {}

    summaries = {}

    # Ofsted breakdown by area
    if "postcode_district" in df.columns and "ofsted_rating" in df.columns:
        summaries["ofsted_by_area"] = (
            df.groupby(["postcode_district", "ofsted_rating"])
            .size()
            .reset_index(name="count")
        )

    # Count by phase per area
    if "postcode_district" in df.columns and "phase" in df.columns:
        summaries["phase_counts"] = (
            df.groupby(["postcode_district", "phase"])
            .size()
            .reset_index(name="count")
        )

    # Summary stats per area for scoring
    if "postcode_district" in df.columns and "ofsted_rating" in df.columns:
        ofsted_score_map = {
            "Outstanding": 4,
            "Good": 3,
            "Requires improvement": 2,
            "Requires Improvement": 2,
            "Inadequate": 1,
            "Serious Weaknesses": 1,
            "Special Measures": 0,
        }
        df["ofsted_score"] = df["ofsted_rating"].map(ofsted_score_map)
        summaries["area_scores"] = (
            df.groupby("postcode_district")
            .agg(
                avg_ofsted=("ofsted_score", "mean"),
                total_schools=("school_name", "count"),
                outstanding_count=("ofsted_score", lambda x: (x == 4).sum()),
                good_count=("ofsted_score", lambda x: (x == 3).sum()),
            )
            .reset_index()
        )

    return summaries


def save_to_db(df: pd.DataFrame, summaries: dict[str, pd.DataFrame]) -> None:
    with sqlite3.connect(config.DATABASE_PATH) as conn:
        if not df.empty:
            df.to_sql("schools", conn, if_exists="replace", index=False)
        else:
            conn.execute("CREATE TABLE IF NOT EXISTS schools (placeholder TEXT)")
        for name, sdf in summaries.items():
            sdf.to_sql(f"schools_{name}", conn, if_exists="replace", index=False)


def run() -> None:
    """Run the Schools ETL pipeline."""
    logger.info("=== Schools ETL starting ===")
    try:
        df = download_gias()
        df = filter_schools(df)
        summaries = compute_summaries(df)
        save_to_db(df, summaries)
        logger.info("=== Schools ETL complete: %d schools ===", len(df))
    except Exception:
        logger.exception("Schools ETL failed")
        _save_empty()


def _save_empty() -> None:
    with sqlite3.connect(config.DATABASE_PATH) as conn:
        for t in ["schools", "schools_ofsted_by_area", "schools_phase_counts", "schools_area_scores"]:
            conn.execute(f"CREATE TABLE IF NOT EXISTS [{t}] (placeholder TEXT)")
