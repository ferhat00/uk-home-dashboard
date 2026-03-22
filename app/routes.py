"""Flask routes for the UK Home Dashboard."""
import json
import logging

import pandas as pd
from flask import Blueprint, render_template, request, jsonify

import config
from app import models, scoring

logger = logging.getLogger(__name__)
bp = Blueprint("main", __name__)


def _area_lookup() -> dict:
    """Map area codes to area info."""
    return {a["code"]: a for a in config.AREAS_OF_INTEREST}


@bp.route("/")
def dashboard():
    """Overview dashboard."""
    areas = config.AREAS_OF_INTEREST
    scores = scoring.compute_scores()
    radar_data = scoring.get_radar_data(scores)

    # Get summary stats for KPI cards
    yearly_prices = models.get_yearly_prices()
    crime_totals = models.get_crime_totals()
    school_scores = models.get_schools_area_scores()
    noise_data = models.get_noise_data()

    # Best area per category
    kpis = {}
    if not scores.empty:
        kpis["best_overall"] = scores.iloc[0].to_dict() if len(scores) > 0 else {}
        if "property_value_score" in scores.columns:
            best_value = scores.loc[scores["property_value_score"].idxmax()]
            kpis["best_value"] = best_value.to_dict()
        if "school_quality_score" in scores.columns:
            best_school = scores.loc[scores["school_quality_score"].idxmax()]
            kpis["best_schools"] = best_school.to_dict()
        if "crime_safety_score" in scores.columns:
            safest = scores.loc[scores["crime_safety_score"].idxmax()]
            kpis["safest"] = safest.to_dict()
        if "transport_score" in scores.columns:
            best_transport = scores.loc[scores["transport_score"].idxmax()]
            kpis["best_transport"] = best_transport.to_dict()

    return render_template(
        "dashboard.html",
        areas=areas,
        scores=scores.to_dict("records") if not scores.empty else [],
        scores_json=scores.to_json(orient="records") if not scores.empty else "[]",
        radar_data=json.dumps(radar_data),
        kpis=kpis,
        weights=config.DEFAULT_WEIGHTS,
    )


@bp.route("/area/<area_code>")
def area_detail(area_code: str):
    """Detailed view for a single area."""
    area_code = area_code.upper()
    area_info = _area_lookup().get(area_code)
    if not area_info:
        return render_template("404.html", message=f"Area {area_code} not found"), 404

    # Get all data for this area
    data = models.get_all_area_data(area_code)

    # Price trend data
    yearly_prices = models.get_yearly_prices()
    area_prices = yearly_prices[
        yearly_prices["postcode_district"] == area_code
    ] if not yearly_prices.empty and "postcode_district" in yearly_prices.columns else pd.DataFrame()

    # Prices by type
    prices_by_type = models.get_prices_by_type()
    area_type_prices = prices_by_type[
        prices_by_type["postcode_district"] == area_code
    ] if not prices_by_type.empty and "postcode_district" in prices_by_type.columns else pd.DataFrame()

    # Noise
    noise = models.get_noise_data()
    area_noise = noise[
        noise["area_code"] == area_code
    ].to_dict("records") if not noise.empty and "area_code" in noise.columns else []

    # Scores
    scores = scoring.compute_scores()
    area_score = scores[
        scores["area_code"] == area_code
    ].to_dict("records") if not scores.empty else []

    return render_template(
        "detail.html",
        area=area_info,
        area_code=area_code,
        transactions=data["transactions"].to_dict("records") if not data["transactions"].empty else [],
        crime_categories=data["crime_categories"].to_dict("records") if not data["crime_categories"].empty else [],
        crime_trend=data["crime_trend"].to_json(orient="records") if not data["crime_trend"].empty else "[]",
        schools=data["schools"].to_dict("records") if not data["schools"].empty else [],
        amenities=data["amenities"].to_dict("records") if not data["amenities"].empty else [],
        stations=data["stations"].to_dict("records") if not data["stations"].empty else [],
        price_trend=area_prices.to_json(orient="records") if not area_prices.empty else "[]",
        type_prices=area_type_prices.to_dict("records") if not area_type_prices.empty else [],
        noise=area_noise[0] if area_noise else {},
        area_score=area_score[0] if area_score else {},
        property_type_labels=config.PROPERTY_TYPE_LABELS,
    )


@bp.route("/compare")
def compare():
    """Side-by-side area comparison."""
    area_codes = request.args.get("areas", "").upper().split(",")
    area_codes = [c.strip() for c in area_codes if c.strip()]

    if not area_codes:
        area_codes = [a["code"] for a in config.AREAS_OF_INTEREST[:4]]

    lookup = _area_lookup()
    selected_areas = [lookup[c] for c in area_codes if c in lookup]

    # Custom weights from query params
    weights = config.DEFAULT_WEIGHTS.copy()
    for key in weights:
        val = request.args.get(f"w_{key}")
        if val:
            try:
                weights[key] = float(val)
            except ValueError:
                pass

    scores = scoring.compute_scores(weights)
    compare_scores = scores[scores["area_code"].isin(area_codes)]
    radar_data = scoring.get_radar_data(compare_scores)

    all_areas = config.AREAS_OF_INTEREST

    return render_template(
        "compare.html",
        selected_areas=selected_areas,
        selected_codes=area_codes,
        all_areas=all_areas,
        scores=compare_scores.to_dict("records") if not compare_scores.empty else [],
        radar_data=json.dumps(radar_data),
        weights=weights,
    )


@bp.route("/settings")
def settings():
    """Settings page."""
    return render_template(
        "settings.html",
        areas=config.AREAS_OF_INTEREST,
        weights=config.DEFAULT_WEIGHTS,
        budget=config.BUDGET_RANGE,
        property_types=config.PROPERTY_TYPES,
        property_type_labels=config.PROPERTY_TYPE_LABELS,
        commute_destinations=config.COMMUTE_DESTINATIONS,
    )


@bp.route("/api/scores")
def api_scores():
    """API endpoint for scores with custom weights."""
    weights = config.DEFAULT_WEIGHTS.copy()
    for key in weights:
        val = request.args.get(key)
        if val:
            try:
                weights[key] = float(val)
            except ValueError:
                pass

    # Normalise weights to sum to 1
    total = sum(weights.values())
    if total > 0:
        weights = {k: v / total for k, v in weights.items()}

    scores = scoring.compute_scores(weights)
    radar = scoring.get_radar_data(scores)

    return jsonify({
        "scores": scores.to_dict("records") if not scores.empty else [],
        "radar": radar,
    })


@bp.route("/api/etl/run", methods=["POST"])
def api_run_etl():
    """Trigger ETL run (for the settings page)."""
    source = request.json.get("source", "all") if request.is_json else "all"

    from etl.pipeline import run_all, run_single

    try:
        if source == "all":
            results = run_all()
            return jsonify({"status": "complete", "results": results})
        else:
            result = run_single(source)
            return jsonify({"status": "complete", "results": {source: result}})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
