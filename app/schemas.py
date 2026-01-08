from pydantic import BaseModel
from datetime import datetime
from typing import Dict


class PredictRequest(BaseModel):
    line_id: str
    stop_id: str


class HealthResponse(BaseModel):
    status: str


class PredictResponse(BaseModel):
    line_id: str
    stop_id: str
    as_of: datetime
    risk_label: int
    risk_probability: float
    features: Dict[str, float]

