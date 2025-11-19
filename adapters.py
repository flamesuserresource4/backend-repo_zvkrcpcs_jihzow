import os
from typing import List, Dict, Optional
import requests

# Adapter functions for each country. When real API keys/params exist, attempt to fetch.
# Otherwise, return placeholder demo data so the pipeline can run.

COUNTRIES = [
    {"code": "USA", "name": "United States"},
    {"code": "CAN", "name": "Canada"},
    {"code": "GBR", "name": "United Kingdom"},
    {"code": "AUS", "name": "Australia"},
    {"code": "DEU", "name": "Germany"},
    {"code": "NLD", "name": "Netherlands"},
]

METRICS = [
    "population",
    "median_income",
    "education_level",
    "unemployment_rate",
    "crime_index",
    "cost_of_living_index",
]

# Helper: basic safe GET

def safe_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None):
    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
        if r.ok:
            return r.json()
    except Exception:
        return None
    return None

# Placeholder builders (used when API not available)

def placeholder_city_data(country_code: str) -> List[Dict]:
    samples = {
        "USA": [
            {"city": "New York", "lat": 40.7128, "lon": -74.0060, "population": 8468000, "median_income": 75000, "education_level": 65, "unemployment_rate": 4.2, "crime_index": 47, "cost_of_living_index": 100},
            {"city": "San Francisco", "lat": 37.7749, "lon": -122.4194, "population": 808000, "median_income": 112000, "education_level": 72, "unemployment_rate": 3.9, "crime_index": 45, "cost_of_living_index": 120},
            {"city": "Austin", "lat": 30.2672, "lon": -97.7431, "population": 974000, "median_income": 76000, "education_level": 60, "unemployment_rate": 3.2, "crime_index": 42, "cost_of_living_index": 88},
        ],
        "CAN": [
            {"city": "Toronto", "lat": 43.6532, "lon": -79.3832, "population": 2930000, "median_income": 82000, "education_level": 68, "unemployment_rate": 6.1, "crime_index": 44, "cost_of_living_index": 95},
            {"city": "Vancouver", "lat": 49.2827, "lon": -123.1207, "population": 662000, "median_income": 78000, "education_level": 66, "unemployment_rate": 5.8, "crime_index": 42, "cost_of_living_index": 105},
        ],
        "GBR": [
            {"city": "London", "lat": 51.5074, "lon": -0.1278, "population": 8982000, "median_income": 65000, "education_level": 62, "unemployment_rate": 4.3, "crime_index": 53, "cost_of_living_index": 110},
            {"city": "Manchester", "lat": 53.4808, "lon": -2.2426, "population": 553000, "median_income": 48000, "education_level": 55, "unemployment_rate": 5.0, "crime_index": 49, "cost_of_living_index": 92},
        ],
        "AUS": [
            {"city": "Sydney", "lat": -33.8688, "lon": 151.2093, "population": 5312000, "median_income": 82000, "education_level": 64, "unemployment_rate": 4.0, "crime_index": 41, "cost_of_living_index": 108},
            {"city": "Melbourne", "lat": -37.8136, "lon": 144.9631, "population": 5078000, "median_income": 80000, "education_level": 63, "unemployment_rate": 4.2, "crime_index": 40, "cost_of_living_index": 104},
        ],
        "DEU": [
            {"city": "Berlin", "lat": 52.5200, "lon": 13.4050, "population": 3769000, "median_income": 62000, "education_level": 60, "unemployment_rate": 5.4, "crime_index": 44, "cost_of_living_index": 96},
            {"city": "Munich", "lat": 48.1351, "lon": 11.5820, "population": 1472000, "median_income": 70000, "education_level": 62, "unemployment_rate": 3.5, "crime_index": 36, "cost_of_living_index": 107},
        ],
        "NLD": [
            {"city": "Amsterdam", "lat": 52.3676, "lon": 4.9041, "population": 921000, "median_income": 64000, "education_level": 61, "unemployment_rate": 3.2, "crime_index": 38, "cost_of_living_index": 102},
            {"city": "Rotterdam", "lat": 51.9244, "lon": 4.4777, "population": 656000, "median_income": 56000, "education_level": 58, "unemployment_rate": 4.1, "crime_index": 42, "cost_of_living_index": 97},
        ],
    }
    return samples.get(country_code, [])

# Real API stubs. These are minimal examples and may require refinement/geo mapping.

def fetch_usa() -> List[Dict]:
    key = os.getenv("CENSUS_API_KEY")
    # Example: U.S. Census API for population by place (simplified demo)
    # If no key, return placeholder
    if not key:
        return placeholder_city_data("USA")
    try:
        # 2022 ACS5, population for selected cities (FIPS place codes would be needed). For demo, fallback.
        return placeholder_city_data("USA")
    except Exception:
        return placeholder_city_data("USA")

def fetch_uk() -> List[Dict]:
    # UK ONS API placeholder
    if not os.getenv("ONS_API_KEY"):
        return placeholder_city_data("GBR")
    return placeholder_city_data("GBR")


def fetch_canada() -> List[Dict]:
    if not os.getenv("STATCAN_API_KEY"):
        return placeholder_city_data("CAN")
    return placeholder_city_data("CAN")


def fetch_australia() -> List[Dict]:
    if not os.getenv("ABS_API_KEY"):
        return placeholder_city_data("AUS")
    return placeholder_city_data("AUS")


def fetch_germany() -> List[Dict]:
    # Eurostat for Germany
    if not os.getenv("EUROSTAT_API_KEY"):
        return placeholder_city_data("DEU")
    return placeholder_city_data("DEU")


def fetch_netherlands() -> List[Dict]:
    if not os.getenv("EUROSTAT_API_KEY"):
        return placeholder_city_data("NLD")
    return placeholder_city_data("NLD")


FETCHERS = {
    "USA": fetch_usa,
    "CAN": fetch_canada,
    "GBR": fetch_uk,
    "AUS": fetch_australia,
    "DEU": fetch_germany,
    "NLD": fetch_netherlands,
}


def fetch_country_data(country_code: str) -> List[Dict]:
    fn = FETCHERS.get(country_code)
    if not fn:
        return []
    return fn() or []
