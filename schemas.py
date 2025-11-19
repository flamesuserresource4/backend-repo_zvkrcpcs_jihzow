from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime

# Core domain schemas for database collections

class Country(BaseModel):
    code: str = Field(..., description="ISO3 country code")
    name: str

class City(BaseModel):
    country_code: str = Field(..., description="ISO3 code")
    name: str
    lat: Optional[float] = None
    lon: Optional[float] = None

class RawMetric(BaseModel):
    country_code: str
    city: str
    metrics: Dict[str, float]
    source: Dict[str, Any] = {}

class NormalizedMetric(BaseModel):
    country_code: str
    city: str
    normalized: Dict[str, float]

class Score(BaseModel):
    country_code: str
    city: str
    score: float
    breakdown: Dict[str, float]

class ETLLog(BaseModel):
    run_id: str
    stage: str
    status: str
    message: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
