"""
Configuration for UK Home Dashboard.
Override any setting via environment variables or a .env file.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
CACHE_DIR = DATA_DIR / "cache"

# Ensure dirs exist
for d in (RAW_DIR, PROCESSED_DIR, CACHE_DIR):
    d.mkdir(parents=True, exist_ok=True)

# SQLite database for the Flask app
DATABASE_PATH = PROCESSED_DIR / "dashboard.db"

# ── API Keys ──────────────────────────────────────────────────────────
EPC_API_KEY = os.getenv("EPC_API_KEY", "")
EPC_API_EMAIL = os.getenv("EPC_API_EMAIL", "")
TFL_APP_ID = os.getenv("TFL_APP_ID", "")
TFL_APP_KEY = os.getenv("TFL_APP_KEY", "")

# ── Areas of interest ─────────────────────────────────────────────────
AREAS_OF_INTEREST = [
    {"code": "EN6", "name": "Potters Bar", "lat": 51.6930, "lng": -0.1720},
    {"code": "EN5", "name": "Barnet / New Barnet", "lat": 51.6530, "lng": -0.1990},
    {"code": "N14", "name": "Southgate", "lat": 51.6320, "lng": -0.1280},
    {"code": "AL1", "name": "St Albans", "lat": 51.7500, "lng": -0.3360},
    {"code": "WD6", "name": "Borehamwood", "lat": 51.6580, "lng": -0.2720},
    {"code": "N20", "name": "Whetstone / Totteridge", "lat": 51.6280, "lng": -0.1770},
    {"code": "N2",  "name": "East Finchley", "lat": 51.5870, "lng": -0.1650},
    {"code": "EN4", "name": "Cockfosters / Hadley Wood", "lat": 51.6520, "lng": -0.1490},
]

COMMUTE_DESTINATIONS = [
    {"name": "Kings Cross", "lat": 51.5308, "lng": -0.1238},
    {"name": "City of London", "lat": 51.5155, "lng": -0.0922},
]

BUDGET_RANGE = (500_000, 900_000)
PROPERTY_TYPES = ["D", "S", "T"]  # Detached, Semi, Terraced

# Property type labels
PROPERTY_TYPE_LABELS = {
    "D": "Detached",
    "S": "Semi-detached",
    "T": "Terraced",
    "F": "Flat/Maisonette",
    "O": "Other",
}

# ── Scoring defaults ──────────────────────────────────────────────────
DEFAULT_WEIGHTS = {
    "property_value": 0.20,
    "school_quality": 0.20,
    "crime_safety": 0.15,
    "transport": 0.15,
    "amenities": 0.15,
    "noise": 0.10,
    "green_space": 0.05,
}

# ── ETL settings ──────────────────────────────────────────────────────
REQUEST_CACHE_EXPIRY = 86400  # 24 hours in seconds
POLICE_API_RATE_LIMIT = 15    # requests per second
EPC_API_RATE_LIMIT = 1        # requests per second
OVERPASS_TIMEOUT = 30          # seconds

# ── Flask settings ────────────────────────────────────────────────────
SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-in-production")
DEBUG = os.getenv("FLASK_DEBUG", "1") == "1"
