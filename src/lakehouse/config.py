"""Paths and thresholds shared by the pipeline and the tests."""

import os
from pathlib import Path

ROOT = Path(os.environ.get("LAKEHOUSE_ROOT", Path(__file__).resolve().parents[2]))
LANDING_DIR = ROOT / "data" / "landing"
SAMPLE_PRODUCTION_FILE = LANDING_DIR / "energinet_prodex5min_2026-11-01.json"
PRICE_AREAS_FILE = ROOT / "data" / "reference" / "price_areas.csv"
WAREHOUSE_DIR = ROOT / "warehouse"
REPORT_DIR = ROOT / "reports"

DEFAULT_DATABASE = "lakehouse"
ENERGINET_URL = "https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime"
PRICE_AREAS = ("DK1", "DK2")

INTERVAL_MINUTES = 5
INTERVALS_PER_DAY = 288  # 5-minute intervals in one UTC day
MAX_ABS_MW = 20000.0  # generous ceiling for any single 5-minute value
