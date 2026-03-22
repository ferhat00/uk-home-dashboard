"""
Composite Location Scoring Engine.

Normalises metrics to 0-100 and produces weighted composite scores.
"""
import pandas as pd
import numpy as np

import config
from app import models


def _normalise(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    """Min-max normalise a series to 0-100."""
    if series.empty or series.nunique() <= 1:
        return pd.Series([50.0] * len(series), index=series.index)

    mn, mx = series.min(), series.max()
    if mn == mx:
        return pd.Series([50.0] * len(series), index=series.index)

    normalised = (series - mn) / (mx - mn) * 100
    if not higher_is_better:
        normalised = 100 - normalised

    return normalised.round(1)


def compute_scores(weights: dict | None = None) -> pd.DataFrame:
    """
    Compute composite scores for all areas of interest.

    Returns DataFrame with columns: area_code, area_name, composite_score,
    plus individual dimension scores.
    """
    if weights is None:
        weights = config.DEFAULT_WEIGHTS.copy()

    areas = pd.DataFrame(config.AREAS_OF_INTEREST)
    areas = areas.rename(columns={"code": "area_code", "name": "area_name"})

    # ── Property value score (lower price = more affordable = higher score) ──
    yearly = models.get_yearly_prices()
    if not yearly.empty and "median_price" in yearly.columns:
        latest = yearly.groupby("postcode_district")["median_price"].last().reset_index()
        latest = latest.rename(columns={"postcode_district": "area_code"})
        areas = areas.merge(latest, on="area_code", how="left")
        areas["property_value_score"] = _normalise(
            areas["median_price"].fillna(areas["median_price"].median()),
            higher_is_better=False,  # Lower price = better score
        )
    else:
        areas["median_price"] = np.nan
        areas["property_value_score"] = 50.0

    # ── School quality score ──
    school_scores = models.get_schools_area_scores()
    if not school_scores.empty and "avg_ofsted" in school_scores.columns:
        school_scores = school_scores.rename(columns={"postcode_district": "area_code"})
        areas = areas.merge(
            school_scores[["area_code", "avg_ofsted", "total_schools"]],
            on="area_code",
            how="left",
        )
        areas["school_quality_score"] = _normalise(
            areas["avg_ofsted"].fillna(areas["avg_ofsted"].median()),
            higher_is_better=True,
        )
    else:
        areas["avg_ofsted"] = np.nan
        areas["total_schools"] = 0
        areas["school_quality_score"] = 50.0

    # ── Crime safety score (fewer crimes = higher score) ──
    crime_totals = models.get_crime_totals()
    if not crime_totals.empty and "total_crimes" in crime_totals.columns:
        areas = areas.merge(
            crime_totals[["area_code", "total_crimes"]],
            on="area_code",
            how="left",
        )
        areas["crime_safety_score"] = _normalise(
            areas["total_crimes"].fillna(areas["total_crimes"].median()),
            higher_is_better=False,  # Fewer crimes = better
        )
    else:
        areas["total_crimes"] = np.nan
        areas["crime_safety_score"] = 50.0

    # ── Transport score ──
    station_counts = models.get_station_counts()
    if not station_counts.empty and "stations_2km" in station_counts.columns:
        areas = areas.merge(
            station_counts[["area_code", "stations_2km"]],
            on="area_code",
            how="left",
        )
        areas["transport_score"] = _normalise(
            areas["stations_2km"].fillna(0),
            higher_is_better=True,
        )
    else:
        areas["stations_2km"] = 0
        areas["transport_score"] = 50.0

    # Also factor in journey times if available
    journey_times = models.get_journey_times()
    if not journey_times.empty and "avg_duration_min" in journey_times.columns:
        avg_jt = journey_times.groupby("area_code")["avg_duration_min"].mean().reset_index()
        areas = areas.merge(avg_jt, on="area_code", how="left")
        jt_score = _normalise(
            areas["avg_duration_min"].fillna(areas["avg_duration_min"].median()),
            higher_is_better=False,
        )
        # Blend station count and journey time
        areas["transport_score"] = (areas["transport_score"] * 0.4 + jt_score * 0.6).round(1)
    else:
        areas["avg_duration_min"] = np.nan

    # ── Amenities score ──
    amenity_density = models.get_amenity_density()
    if not amenity_density.empty:
        # Sum all amenity columns (excluding area_code and area_name)
        num_cols = amenity_density.select_dtypes(include="number").columns
        amenity_density["total_amenities"] = amenity_density[num_cols].sum(axis=1)
        areas = areas.merge(
            amenity_density[["area_code", "total_amenities"]],
            on="area_code",
            how="left",
        )
        areas["amenities_score"] = _normalise(
            areas["total_amenities"].fillna(0),
            higher_is_better=True,
        )
    else:
        areas["total_amenities"] = 0
        areas["amenities_score"] = 50.0

    # ── Noise score (lower noise = higher score) ──
    noise = models.get_noise_data()
    if not noise.empty and "road_lden_db" in noise.columns:
        areas = areas.merge(
            noise[["area_code", "road_lden_db", "rail_lden_db", "air_lden_db"]],
            on="area_code",
            how="left",
        )
        # Combined noise metric
        areas["combined_noise"] = (
            areas["road_lden_db"].fillna(55) * 0.6
            + areas["rail_lden_db"].fillna(50) * 0.2
            + areas["air_lden_db"].fillna(48) * 0.2
        )
        areas["noise_score"] = _normalise(
            areas["combined_noise"],
            higher_is_better=False,
        )
    else:
        areas["combined_noise"] = np.nan
        areas["noise_score"] = 50.0

    # ── Green space score (use park amenity count as proxy) ──
    amenity_counts = models.get_amenity_counts()
    if not amenity_counts.empty:
        parks = amenity_counts[amenity_counts["category"] == "park"]
        if not parks.empty:
            parks_by_area = parks.groupby("area_code")["count"].sum().reset_index()
            parks_by_area = parks_by_area.rename(columns={"count": "park_count"})
            areas = areas.merge(parks_by_area, on="area_code", how="left")
            areas["green_space_score"] = _normalise(
                areas["park_count"].fillna(0),
                higher_is_better=True,
            )
        else:
            areas["park_count"] = 0
            areas["green_space_score"] = 50.0
    else:
        areas["park_count"] = 0
        areas["green_space_score"] = 50.0

    # ── Composite score ──
    score_cols = {
        "property_value": "property_value_score",
        "school_quality": "school_quality_score",
        "crime_safety": "crime_safety_score",
        "transport": "transport_score",
        "amenities": "amenities_score",
        "noise": "noise_score",
        "green_space": "green_space_score",
    }

    areas["composite_score"] = sum(
        areas[col] * weights.get(dim, 0)
        for dim, col in score_cols.items()
    ).round(1)

    areas = areas.sort_values("composite_score", ascending=False).reset_index(drop=True)
    areas["rank"] = range(1, len(areas) + 1)

    return areas


def get_radar_data(scores_df: pd.DataFrame) -> list[dict]:
    """Convert scores to radar chart format."""
    dimensions = [
        ("property_value_score", "Property Value"),
        ("school_quality_score", "Schools"),
        ("crime_safety_score", "Safety"),
        ("transport_score", "Transport"),
        ("amenities_score", "Amenities"),
        ("noise_score", "Noise"),
        ("green_space_score", "Green Space"),
    ]

    radar = []
    for _, row in scores_df.iterrows():
        entry = {
            "area_code": row["area_code"],
            "area_name": row["area_name"],
            "values": [float(row.get(col, 50)) for col, _ in dimensions],
            "labels": [label for _, label in dimensions],
        }
        radar.append(entry)

    return radar
