# UK Home Dashboard

Interactive Flask dashboard for evaluating UK locations to find the optimal place to live. Combines property prices, school quality, crime statistics, amenities, transport connectivity, and noise data into a single decision-support tool.

## Features

- **Property Prices**: HM Land Registry transactions, price trends, EPC energy ratings
- **Schools**: GIAS data with Ofsted ratings, searchable by area
- **Crime**: Police.uk street-level crime data with category breakdown and trends
- **Amenities**: OpenStreetMap data — supermarkets, cafes, pharmacies, parks, etc.
- **Transport**: TfL stations, journey times to commute destinations
- **Noise**: DEFRA noise mapping — road, rail, and aircraft noise levels
- **Composite Scoring**: Weighted scoring engine with adjustable priorities
- **Interactive Maps**: Leaflet.js maps with toggleable data layers
- **Comparison**: Side-by-side area comparison with radar charts

## Quick Start

### 1. Install dependencies

```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### 2. Configure API keys (optional but recommended)

```bash
cp .env.example .env
# Edit .env with your API keys
```

| Service | Required? | Registration |
|---------|-----------|-------------|
| EPC Register | Optional | https://epc.opendatacommunities.org/ |
| TfL API | Optional | https://api-portal.tfl.gov.uk/signup |
| Police.uk | No key needed | Free |
| Overpass (OSM) | No key needed | Free |

### 3. Run the ETL pipeline

```bash
# Run all data pipelines
python run_etl.py --all

# Or run individual sources
python run_etl.py --source crime
python run_etl.py --source schools
python run_etl.py --source amenities
python run_etl.py --source noise
```

The noise ETL runs with built-in estimates and requires no external data, so it will always work. Other pipelines fetch data from external APIs and may take several minutes.

### 4. Start the dashboard

```bash
python run_app.py
```

Open http://127.0.0.1:5000 in your browser.

## Project Structure

```
├── config.py              # API keys, areas of interest, settings
├── data/
│   ├── raw/               # Downloaded CSV/JSON files
│   ├── processed/         # SQLite database
│   └── cache/             # API response cache
├── etl/
│   ├── land_registry.py   # HM Land Registry Price Paid
│   ├── epc.py             # EPC Register API
│   ├── ons_hpi.py         # ONS House Price Index
│   ├── crime.py           # Police.uk API
│   ├── schools.py         # GIAS + Ofsted
│   ├── amenities.py       # Overpass API (OSM)
│   ├── transport.py       # TfL API
│   ├── noise.py           # DEFRA noise mapping
│   └── pipeline.py        # Orchestrator
├── app/
│   ├── __init__.py        # Flask app factory
│   ├── routes.py          # Page routes + API endpoints
│   ├── models.py          # Database access layer
│   ├── scoring.py         # Composite scoring engine
│   ├── templates/         # Jinja2 HTML templates
│   └── static/            # CSS + JS
├── run_etl.py             # CLI for data pipeline
└── run_app.py             # Flask entry point
```

## Dashboard Pages

| Page | URL | Description |
|------|-----|-------------|
| Overview | `/` | Map + KPI cards + rankings table |
| Area Detail | `/area/<code>` | Deep dive on a single area |
| Compare | `/compare?areas=EN6,N14,AL1` | Side-by-side comparison |
| Settings | `/settings` | Config, ETL controls, API status |

## Configuring Areas

Edit `AREAS_OF_INTEREST` in `config.py` to add or change the postcode districts you want to evaluate. Each area needs a code, name, and approximate lat/lng centroid.

## Scoring Weights

The composite score is computed from 7 dimensions with adjustable weights. Default weights can be changed in `config.py` or interactively on the Compare page using sliders.

| Dimension | Default Weight |
|-----------|---------------|
| Property Value | 20% |
| School Quality | 20% |
| Crime Safety | 15% |
| Transport | 15% |
| Amenities | 15% |
| Noise | 10% |
| Green Space | 5% |
