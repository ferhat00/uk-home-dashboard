# UK Home Dashboard

Interactive Flask dashboard for evaluating UK locations to find the optimal place to live. Combines property prices, school quality, crime statistics, amenities, transport connectivity, and noise data into a weighted composite score — making it easy to compare areas side by side.

## Features

- **Property Prices**: HM Land Registry transactions, price trends by property type, EPC energy ratings
- **Schools**: GIAS data merged with live Ofsted inspection ratings (Outstanding / Good / Requires improvement / Inadequate)
- **Crime**: Police.uk street-level crime data with category breakdown and trends
- **Amenities**: OpenStreetMap data — supermarkets, cafes, pharmacies, parks, gyms, etc.
- **Transport**: TfL stations (tube, overground, Elizabeth line, national rail) with nearest stations and journey times to commute destinations
- **Noise**: DEFRA noise mapping — road, rail, and aircraft noise levels
- **Composite Scoring**: Weighted scoring engine with 7 adjustable dimensions
- **Interactive Maps**: Leaflet.js maps with toggleable data layers
- **Comparison**: Side-by-side area comparison with radar charts

---

## Quick Start

### 1. Install dependencies

```bash
python -m venv house
source house/bin/activate  # or house\Scripts\activate on Windows
pip install -r requirements.txt
```

### 2. Configure your locations

Open `config.py` and edit `AREAS_OF_INTEREST` — this is the only change most users need to make before running. See [Configuring Locations](#configuring-locations) below.

### 3. Configure API keys (optional)

```bash
cp .env.example .env
# Edit .env with your API keys
```

| Service | Required? | Notes |
|---------|-----------|-------|
| TfL API | Optional | Speeds up transport lookups; works without a key but may be slower |
| EPC Register | Optional | Needed for energy rating data |
| Police.uk | No key needed | Always free |
| Overpass (OSM) | No key needed | Always free |

### 4. Run the ETL pipelines

Fetch and process data for all configured areas:

```bash
python run_etl.py --all
```

Or run individual pipelines if you only need specific data:

```bash
python run_etl.py --source land_registry
python run_etl.py --source schools
python run_etl.py --source crime
python run_etl.py --source amenities
python run_etl.py --source transport
python run_etl.py --source noise
python run_etl.py --source epc
python run_etl.py --source ons_hpi
```

ETL notes:
- First run takes several minutes — data is cached in `data/cache/` so subsequent runs are fast
- `noise` runs entirely from built-in estimates and needs no external data
- `transport` queries TfL's geographic API per area — subsequent runs use the cache
- `schools` downloads GIAS data and merges live Ofsted ratings from gov.uk

### 5. Start the dashboard

```bash
python run_app.py
```

Open [http://127.0.0.1:5000](http://127.0.0.1:5000) in your browser.

---

## Configuring Locations

All location configuration lives in `config.py`. You do not need to edit any other file to change which areas are analysed.

### Areas of interest

```python
AREAS_OF_INTEREST = [
    {"code": "EN6", "name": "Potters Bar",          "lat": 51.6930, "lng": -0.1720},
    {"code": "EN5", "name": "Barnet / New Barnet",  "lat": 51.6530, "lng": -0.1990},
    # add or remove entries here
]
```

Each entry needs:

| Field | Description | Example |
|-------|-------------|---------|
| `code` | UK postcode district — used as a unique ID throughout | `"EN6"` |
| `name` | Human-readable label shown in the UI | `"Potters Bar"` |
| `lat` | Latitude of the approximate area centroid | `51.6930` |
| `lng` | Longitude of the approximate area centroid | `-0.1720` |

To find lat/lng for a new area, search for the postcode on [google.com/maps](https://www.google.com/maps) or [latlong.net](https://www.latlong.net/) and note the coordinates for the town centre.

After changing areas, re-run the ETL (`python run_etl.py --all`) to fetch data for the new locations. Delete files in `data/raw/` if you want to force a full re-download.

### Commute destinations

```python
COMMUTE_DESTINATIONS = [
    {"name": "Kings Cross",    "lat": 51.5308, "lng": -0.1238},
    {"name": "City of London", "lat": 51.5155, "lng": -0.0922},
]
```

Journey times in the transport section are calculated from the nearest station in each area to each destination. Add or change destinations to match your actual commute.

### Budget and property types

```python
BUDGET_RANGE = (500_000, 900_000)   # min/max in GBP
PROPERTY_TYPES = ["D", "S", "T"]    # D=Detached, S=Semi, T=Terraced, F=Flat
```

These filter the property price data shown in the dashboard. Transactions outside the budget range or excluded types are still stored in the database but de-emphasised in scoring.

---

## Scoring Weights

The composite score is computed from 7 dimensions. Default weights are set in `config.py` and can also be adjusted interactively on the Compare page using sliders.

```python
DEFAULT_WEIGHTS = {
    "property_value": 0.20,   # lower price-to-quality ratio = better
    "school_quality":  0.20,   # Ofsted ratings (Outstanding=4 … Inadequate=1)
    "crime_safety":    0.15,   # inverse of crime rate
    "transport":       0.15,   # station proximity + journey times
    "amenities":       0.15,   # count of shops, parks, cafes etc.
    "noise":           0.10,   # DEFRA road/rail/aircraft noise
    "green_space":     0.05,   # parks and open space coverage
}
```

All weights must sum to 1.0. Dimensions with no data fall back to a neutral score of 50.

---

## Project Structure

```
├── config.py              # All user-configurable settings (locations, weights, budget)
├── .env                   # API keys — copy from .env.example (not committed)
├── run_etl.py             # CLI: python run_etl.py --all | --source <name>
├── run_app.py             # Flask entry point
├── data/
│   ├── raw/               # Downloaded CSV/JSON source files
│   ├── processed/         # SQLite database (dashboard.db)
│   └── cache/             # HTTP response cache (speeds up re-runs)
├── etl/
│   ├── pipeline.py        # Orchestrator — calls all sources in order
│   ├── land_registry.py   # HM Land Registry Price Paid data
│   ├── epc.py             # EPC Register API (energy ratings)
│   ├── ons_hpi.py         # ONS House Price Index
│   ├── crime.py           # Police.uk API
│   ├── schools.py         # GIAS school list + Ofsted MI ratings
│   ├── amenities.py       # Overpass API (OpenStreetMap)
│   ├── transport.py       # TfL StopPoint API (geo search per area)
│   └── noise.py           # DEFRA noise estimates
└── app/
    ├── __init__.py        # Flask app factory
    ├── routes.py          # Page routes + JSON API endpoints
    ├── models.py          # SQLite query layer
    ├── scoring.py         # Composite scoring engine
    ├── templates/         # Jinja2 HTML templates
    └── static/            # CSS + JS (Bootstrap, Leaflet, Chart.js)
```

---

## Dashboard Pages

| Page | URL | Description |
|------|-----|-------------|
| Overview | `/` | Map + KPI cards (best value, safest, best schools) + ranked table |
| Area Detail | `/area/<code>` | Property prices, schools, crime, transport, amenities for one area |
| Compare | `/compare?areas=EN6,N14,AL1` | Side-by-side comparison with radar chart and adjustable weights |
| Settings | `/settings` | ETL status, API health checks, cache controls |

---

## Data Sources

| Data | Source | Refresh frequency |
|------|--------|-------------------|
| Property transactions | [HM Land Registry Price Paid](https://www.gov.uk/government/collections/price-paid-data) | Monthly |
| House price index | [ONS HPI](https://www.ons.gov.uk/economy/inflationandpriceindices/bulletins/housepriceindex/latest) | Monthly |
| Energy ratings | [EPC Register](https://epc.opendatacommunities.org/) | On demand |
| Schools | [GIAS (edubase)](https://get-information-schools.service.gov.uk/) | As needed |
| Ofsted ratings | [Ofsted MI data (gov.uk)](https://www.gov.uk/government/statistical-data-sets/monthly-management-information-ofsteds-school-inspections-outcomes) | Monthly |
| Crime | [Police.uk API](https://data.police.uk/docs/) | Monthly |
| Amenities | [OpenStreetMap via Overpass](https://overpass-api.de/) | As needed |
| Transport | [TfL Unified API](https://api.tfl.gov.uk/) | As needed |
| Noise | DEFRA noise mapping estimates | Static |
