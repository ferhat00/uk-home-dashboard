"""
Transport ETL — TfL API + NaPTAN.

Fetches station locations and calculates commute times.
Sources:
- TfL: https://api.tfl.gov.uk/
- NaPTAN: https://www.data.gov.uk/dataset/naptan
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

TFL_BASE = "https://api.tfl.gov.uk"


def _get_session() -> requests_cache.CachedSession:
    session = requests_cache.CachedSession(
        str(config.CACHE_DIR / "transport_cache"),
        expire_after=config.REQUEST_CACHE_EXPIRY,
    )
    return session


def _tfl_params() -> dict:
    """Common TfL API parameters."""
    params = {}
    if config.TFL_APP_ID:
        params["app_id"] = config.TFL_APP_ID
    if config.TFL_APP_KEY:
        params["app_key"] = config.TFL_APP_KEY
    return params


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
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


def fetch_stations(session: requests.Session) -> pd.DataFrame:
    """Fetch tube/overground/rail stations from TfL."""
    modes = "tube,overground,elizabeth-line,national-rail"
    url = f"{TFL_BASE}/StopPoint/Mode/{modes}"
    params = _tfl_params()

    all_stations = []
    try:
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        stop_points = data.get("stopPoints", data) if isinstance(data, dict) else data
        if isinstance(stop_points, dict):
            stop_points = stop_points.get("stopPoints", [])

        for sp in stop_points:
            modes_list = [m.get("modeName", "") for m in sp.get("modes", [])] if isinstance(sp.get("modes"), list) else []
            lines_list = [ln.get("name", "") for ln in sp.get("lines", [])] if isinstance(sp.get("lines"), list) else []

            all_stations.append({
                "station_id": sp.get("naptanId", sp.get("id", "")),
                "name": sp.get("commonName", sp.get("name", "")),
                "lat": sp.get("lat"),
                "lng": sp.get("lon"),
                "modes": ", ".join(modes_list),
                "lines": ", ".join(lines_list),
            })

    except requests.RequestException as e:
        logger.warning("Could not fetch TfL stations: %s", e)

    return pd.DataFrame(all_stations) if all_stations else pd.DataFrame()


def filter_nearby_stations(stations: pd.DataFrame, max_km: float = 5.0) -> pd.DataFrame:
    """Filter stations to those near areas of interest."""
    if stations.empty:
        return stations

    nearby = []
    for area in config.AREAS_OF_INTEREST:
        for _, st in stations.iterrows():
            if pd.isna(st.get("lat")) or pd.isna(st.get("lng")):
                continue
            dist = _haversine_km(area["lat"], area["lng"], st["lat"], st["lng"])
            if dist <= max_km:
                row = st.to_dict()
                row["area_code"] = area["code"]
                row["area_name"] = area["name"]
                row["distance_km"] = round(dist, 2)
                nearby.append(row)

    return pd.DataFrame(nearby) if nearby else pd.DataFrame()


def fetch_journey_times(session: requests.Session, stations: pd.DataFrame) -> pd.DataFrame:
    """Fetch journey times from nearest stations to commute destinations."""
    if stations.empty:
        return pd.DataFrame()

    # Get the nearest station per area
    nearest = (
        stations.sort_values("distance_km")
        .groupby("area_code")
        .first()
        .reset_index()
    )

    results = []
    params = _tfl_params()

    for _, station in nearest.iterrows():
        for dest in config.COMMUTE_DESTINATIONS:
            from_id = station["station_id"]
            to_coords = f"{dest['lat']},{dest['lng']}"

            url = f"{TFL_BASE}/Journey/JourneyResults/{from_id}/to/{to_coords}"
            try:
                resp = session.get(url, params=params, timeout=15)
                if resp.status_code != 200:
                    logger.debug("Journey API %d for %s→%s", resp.status_code, from_id, dest["name"])
                    continue
                data = resp.json()

                journeys = data.get("journeys", [])
                if journeys:
                    durations = [j.get("duration", 0) for j in journeys]
                    results.append({
                        "area_code": station["area_code"],
                        "area_name": station["area_name"],
                        "from_station": station["name"],
                        "to_destination": dest["name"],
                        "min_duration_min": min(durations),
                        "avg_duration_min": round(sum(durations) / len(durations)),
                    })
            except requests.RequestException:
                pass

            time.sleep(0.5)

    return pd.DataFrame(results) if results else pd.DataFrame()


def compute_summaries(
    stations: pd.DataFrame, journeys: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    summaries = {}

    if not stations.empty:
        # Nearest station per area
        summaries["nearest_stations"] = (
            stations.sort_values("distance_km")
            .groupby("area_code")
            .head(5)
            .reset_index(drop=True)
        )

        # Station count within various radii
        counts = []
        for area in config.AREAS_OF_INTEREST:
            area_st = stations[stations["area_code"] == area["code"]]
            counts.append({
                "area_code": area["code"],
                "area_name": area["name"],
                "stations_1km": len(area_st[area_st["distance_km"] <= 1.0]),
                "stations_2km": len(area_st[area_st["distance_km"] <= 2.0]),
                "stations_5km": len(area_st[area_st["distance_km"] <= 5.0]),
            })
        summaries["station_counts"] = pd.DataFrame(counts)

    if not journeys.empty:
        summaries["journey_times"] = journeys

    return summaries


def save_to_db(
    stations: pd.DataFrame,
    journeys: pd.DataFrame,
    summaries: dict[str, pd.DataFrame],
) -> None:
    with sqlite3.connect(config.DATABASE_PATH) as conn:
        if not stations.empty:
            stations.to_sql("transport_stations", conn, if_exists="replace", index=False)
        else:
            conn.execute("CREATE TABLE IF NOT EXISTS transport_stations (placeholder TEXT)")
        if not journeys.empty:
            journeys.to_sql("transport_journeys", conn, if_exists="replace", index=False)
        else:
            conn.execute("CREATE TABLE IF NOT EXISTS transport_journeys (placeholder TEXT)")
        for name, sdf in summaries.items():
            sdf.to_sql(f"transport_{name}", conn, if_exists="replace", index=False)


def run() -> None:
    """Run the Transport ETL pipeline."""
    logger.info("=== Transport ETL starting ===")
    try:
        session = _get_session()
        stations = fetch_stations(session)
        nearby = filter_nearby_stations(stations)
        journeys = fetch_journey_times(session, nearby)
        summaries = compute_summaries(nearby, journeys)
        save_to_db(nearby, journeys, summaries)
        logger.info(
            "=== Transport ETL complete: %d nearby stations ===", len(nearby)
        )
    except Exception:
        logger.exception("Transport ETL failed")
        _save_empty()


def _save_empty() -> None:
    with sqlite3.connect(config.DATABASE_PATH) as conn:
        for t in [
            "transport_stations", "transport_journeys",
            "transport_nearest_stations", "transport_station_counts",
            "transport_journey_times",
        ]:
            conn.execute(f"CREATE TABLE IF NOT EXISTS [{t}] (placeholder TEXT)")
