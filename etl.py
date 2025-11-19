import uuid
from datetime import datetime, timezone
from typing import Dict, List

from database import create_document, db
from schemas import RawMetric, NormalizedMetric, Score, ETLLog
from adapters import COUNTRIES, METRICS, fetch_country_data

# Normalization: maps raw values to 0-100 scale per metric using min-max with guardrails
DEFAULT_RANGES = {
    "population": (50000, 10000000),
    "median_income": (20000, 150000),
    "education_level": (0, 100),
    "unemployment_rate": (1, 15),  # lower is better -> invert later
    "crime_index": (20, 80),       # lower is better -> invert later
    "cost_of_living_index": (70, 140),  # lower is better -> invert later
}

# Weights sum to 1.0
WEIGHTS = {
    "population": 0.10,
    "median_income": 0.22,
    "education_level": 0.22,
    "unemployment_rate": 0.16,
    "crime_index": 0.15,
    "cost_of_living_index": 0.15,
}

LOWER_IS_BETTER = {"unemployment_rate", "crime_index", "cost_of_living_index"}


def minmax(v: float, lo: float, hi: float) -> float:
    if v is None:
        return 0.0
    if hi == lo:
        return 0.0
    n = (v - lo) / (hi - lo)
    n = max(0.0, min(1.0, n))
    return n


def normalize(metrics: Dict[str, float]) -> Dict[str, float]:
    out = {}
    for m in METRICS:
        lo, hi = DEFAULT_RANGES[m]
        n = minmax(metrics.get(m, 0), lo, hi)
        if m in LOWER_IS_BETTER:
            n = 1.0 - n
        out[m] = round(n * 100, 2)
    return out


def score_city(norm: Dict[str, float]) -> (float, Dict[str, float]):
    breakdown = {}
    total = 0.0
    for m, w in WEIGHTS.items():
        c = norm_val = norm_value = norm[m] / 100.0
        part = norm_value * w
        breakdown[m] = round(part * 100, 2)
        total += part
    return round(total * 100, 2), breakdown


def run_etl() -> str:
    run_id = str(uuid.uuid4())
    # Log start
    create_document("etllog", ETLLog(run_id=run_id, stage="start", status="running", started_at=datetime.now(timezone.utc)))

    for c in COUNTRIES:
        code = c["code"]
        # Fetch raw data
        data = fetch_country_data(code)
        for row in data:
            raw = RawMetric(country_code=code, city=row["city"], metrics={k: float(row.get(k, 0)) for k in METRICS}, source={"adapter": code})
            create_document("rawmetric", raw)
            # Normalize
            norm = normalize(raw.metrics)
            create_document("normalizedmetric", NormalizedMetric(country_code=code, city=raw.city, normalized=norm))
            # Score
            s, br = score_city(norm)
            create_document("score", Score(country_code=code, city=raw.city, score=s, breakdown=br))

    create_document("etllog", ETLLog(run_id=run_id, stage="finish", status="success", finished_at=datetime.now(timezone.utc)))
    return run_id
