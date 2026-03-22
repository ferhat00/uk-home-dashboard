"""
Database access layer.

Reads from the SQLite database populated by the ETL pipeline.
"""
import sqlite3
from contextlib import contextmanager

import pandas as pd

import config


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = sqlite3.connect(config.DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _safe_read_sql(query: str, conn: sqlite3.Connection) -> pd.DataFrame:
    """Read SQL query, returning empty DataFrame on error."""
    try:
        return pd.read_sql_query(query, conn)
    except Exception:
        return pd.DataFrame()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cursor.fetchone() is not None


# ── Property data ─────────────────────────────────────────────────────

def get_yearly_prices() -> pd.DataFrame:
    with get_db() as conn:
        return _safe_read_sql(
            "SELECT * FROM land_registry_yearly_prices ORDER BY postcode_district, year",
            conn,
        )


def get_prices_by_type() -> pd.DataFrame:
    with get_db() as conn:
        return _safe_read_sql(
            "SELECT * FROM land_registry_prices_by_type",
            conn,
        )


def get_recent_transactions(area_code: str | None = None, limit: int = 100) -> pd.DataFrame:
    with get_db() as conn:
        if area_code:
            return _safe_read_sql(
                f"SELECT * FROM land_registry_recent_transactions "
                f"WHERE postcode_district = '{area_code}' "
                f"ORDER BY date DESC LIMIT {limit}",
                conn,
            )
        return _safe_read_sql(
            f"SELECT * FROM land_registry_recent_transactions "
            f"ORDER BY date DESC LIMIT {limit}",
            conn,
        )


# ── EPC data ──────────────────────────────────────────────────────────

def get_epc_summary() -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "epc_floor_area_by_district"):
            return pd.DataFrame()
        return _safe_read_sql("SELECT * FROM epc_floor_area_by_district", conn)


def get_epc_ratings() -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "epc_energy_ratings"):
            return pd.DataFrame()
        return _safe_read_sql("SELECT * FROM epc_energy_ratings", conn)


# ── Crime data ────────────────────────────────────────────────────────

def get_crime_by_category(area_code: str | None = None) -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "crime_by_category"):
            return pd.DataFrame()
        if area_code:
            return _safe_read_sql(
                f"SELECT * FROM crime_by_category WHERE area_code = '{area_code}'",
                conn,
            )
        return _safe_read_sql("SELECT * FROM crime_by_category", conn)


def get_crime_trend(area_code: str | None = None) -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "crime_monthly_trend"):
            return pd.DataFrame()
        if area_code:
            return _safe_read_sql(
                f"SELECT * FROM crime_monthly_trend WHERE area_code = '{area_code}' ORDER BY month",
                conn,
            )
        return _safe_read_sql(
            "SELECT * FROM crime_monthly_trend ORDER BY area_code, month",
            conn,
        )


def get_crime_totals() -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "crime_totals"):
            return pd.DataFrame()
        return _safe_read_sql("SELECT * FROM crime_totals", conn)


# ── Schools data ──────────────────────────────────────────────────────

def get_schools(area_code: str | None = None) -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "schools"):
            return pd.DataFrame()
        if area_code:
            return _safe_read_sql(
                f"SELECT * FROM schools WHERE postcode_district = '{area_code}'",
                conn,
            )
        return _safe_read_sql("SELECT * FROM schools", conn)


def get_schools_area_scores() -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "schools_area_scores"):
            return pd.DataFrame()
        return _safe_read_sql("SELECT * FROM schools_area_scores", conn)


def get_ofsted_by_area() -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "schools_ofsted_by_area"):
            return pd.DataFrame()
        return _safe_read_sql("SELECT * FROM schools_ofsted_by_area", conn)


# ── Amenities data ────────────────────────────────────────────────────

def get_amenities(area_code: str | None = None) -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "amenities"):
            return pd.DataFrame()
        if area_code:
            return _safe_read_sql(
                f"SELECT * FROM amenities WHERE area_code = '{area_code}'",
                conn,
            )
        return _safe_read_sql("SELECT * FROM amenities", conn)


def get_amenity_counts() -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "amenities_counts"):
            return pd.DataFrame()
        return _safe_read_sql("SELECT * FROM amenities_counts", conn)


def get_amenity_density() -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "amenities_density"):
            return pd.DataFrame()
        return _safe_read_sql("SELECT * FROM amenities_density", conn)


# ── Transport data ────────────────────────────────────────────────────

def get_nearest_stations(area_code: str | None = None) -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "transport_nearest_stations"):
            return pd.DataFrame()
        if area_code:
            return _safe_read_sql(
                f"SELECT * FROM transport_nearest_stations WHERE area_code = '{area_code}'",
                conn,
            )
        return _safe_read_sql("SELECT * FROM transport_nearest_stations", conn)


def get_station_counts() -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "transport_station_counts"):
            return pd.DataFrame()
        return _safe_read_sql("SELECT * FROM transport_station_counts", conn)


def get_journey_times() -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "transport_journey_times"):
            return pd.DataFrame()
        return _safe_read_sql("SELECT * FROM transport_journey_times", conn)


# ── Noise data ────────────────────────────────────────────────────────

def get_noise_data() -> pd.DataFrame:
    with get_db() as conn:
        if not _table_exists(conn, "noise_data"):
            return pd.DataFrame()
        return _safe_read_sql("SELECT * FROM noise_data", conn)


# ── Aggregate helpers ─────────────────────────────────────────────────

def get_all_area_data(area_code: str) -> dict:
    """Get all data for a specific area."""
    return {
        "transactions": get_recent_transactions(area_code),
        "crime_categories": get_crime_by_category(area_code),
        "crime_trend": get_crime_trend(area_code),
        "schools": get_schools(area_code),
        "amenities": get_amenities(area_code),
        "stations": get_nearest_stations(area_code),
    }
