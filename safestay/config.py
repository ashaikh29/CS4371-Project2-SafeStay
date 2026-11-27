"""Project-wide settings: paths, environment variables and the San Francisco dataset details."""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_dotenv(path: Path = PROJECT_ROOT / ".env") -> None:
    """Load KEY=VALUE lines from .env into os.environ (existing variables win).

    A tiny hand-written loader, so the project needs no extra package for it.
    """
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_dotenv()

# Everything the code writes goes under DATA_ROOT (defaults to the project folder).
DATA_ROOT = Path(os.environ.get("SAFESTAY_DATA_DIR") or PROJECT_ROOT)
CACHE_DIR = DATA_ROOT / "cache"        # every raw API response, with its retrieval time
RAW_DIR = DATA_ROOT / "data" / "raw"   # raw incident Parquet (not submitted)
OUTPUT_DIR = DATA_ROOT / "outputs"     # files named in the assignment (t1_..., t2_..., ...)

START_DATE = "2022-01-01T00:00:00"
END_DATE = "2024-12-31T23:59:59"
FIRST_YEAR, LAST_YEAR = 2022, 2024

CATEGORY_PERSONS = "crimes against persons"
CATEGORY_PROPERTY = "crimes against property"
CATEGORY_OTHER = "other"

# ---------------------------------------------------------------------------
# San Francisco Police Department Incident Reports (2018 to present), DataSF
# https://data.sf.gov/resource/wg3w-h783.json
# ---------------------------------------------------------------------------
CITY_NAME = "San Francisco"
SOCRATA_DOMAIN = "data.sf.gov"
INCIDENTS_DATASET = "wg3w-h783"
INCIDENTS_URL = f"https://{SOCRATA_DOMAIN}/resource/{INCIDENTS_DATASET}.json"

# Columns requested with $select (only these are downloaded and stored).
INCIDENT_FIELDS = [
    "row_id", "incident_id", "incident_datetime", "incident_category",
    "incident_subcategory", "incident_description", "police_district",
    "analysis_neighborhood", "latitude", "longitude",
]
ID_FIELD = "row_id"                          # one row per incident + incident code
DATE_FIELD = "incident_datetime"
NEIGHBOURHOOD_FIELD = "analysis_neighborhood"  # 41 Analysis Neighborhoods
OFFENCE_TYPE_FIELD = "incident_category"
CATEGORY_FIELD = "incident_category"

# Persons/property mapping of incident_category (compared upper-cased). It follows the
# FBI NIBRS "crime against" groups, so robbery counts as a crime against property.
# Every value not listed here maps to "other"; outputs/t1_category_mapping.csv lists
# every value seen with its category and row count.
PERSONS_VALUES = [
    "ASSAULT", "HOMICIDE", "RAPE", "SEX OFFENSE",
    "HUMAN TRAFFICKING (A), COMMERCIAL SEX ACTS",
    "HUMAN TRAFFICKING (B), INVOLUNTARY SERVITUDE",
    "HUMAN TRAFFICKING, COMMERCIAL SEX ACTS",
    "OFFENCES AGAINST THE FAMILY AND CHILDREN",
]
PROPERTY_VALUES = [
    "LARCENY THEFT", "BURGLARY", "MOTOR VEHICLE THEFT", "MOTOR VEHICLE THEFT?",
    "ROBBERY", "VANDALISM", "MALICIOUS MISCHIEF", "ARSON", "STOLEN PROPERTY",
    "FRAUD", "FORGERY AND COUNTERFEITING", "EMBEZZLEMENT",
]

# "Inside the city": San Francisco's bounding box (min_lat, max_lat, min_lon, max_lon),
# and rows the police department itself marks as outside San Francisco.
CITY_BBOX = (37.70, 37.84, -122.52, -122.35)
OUT_OF_CITY_DISTRICT = "Out of SF"


def socrata_app_token() -> str | None:
    """Free Socrata app token (avoids throttling). Optional, but strongly recommended."""
    return os.environ.get("SOCRATA_APP_TOKEN") or None
