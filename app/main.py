import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fastapi import FastAPI, HTTPException
import numpy as np
import logging
from prometheus_fastapi_instrumentator import Instrumentator

from app.schemas import PredictRequest, PredictResponse, HealthResponse
from nyc_delay_risk.serving.model_loader import load_model, get_model
from nyc_delay_risk.serving.features_online import compute_features_online

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="NYC Subway Delay Risk API")

# Instrument app for Prometheus metrics
Instrumentator().instrument(app).expose(app)

# Feature order matching training (alphabetical order after excluding identifiers)
FEATURE_ORDER = [
    'alerts_count',
    'alerts_sum_15m',
    'alerts_sum_60m',
    'day_of_week',
    'hour_of_day',
    'major_alerts_count',
    'trip_updates_count',
    'trip_updates_sum_15m',
    'trip_updates_sum_60m',
    'vehicle_positions_count',
    'vehicle_positions_sum_15m',
    'vehicle_positions_sum_60m',
]


@app.on_event("startup")
async def startup_event():
    """Load model at startup."""
    model_path = Path("/app/models/best_model.pkl")
    if not model_path.exists():
        model_path = Path(__file__).parent.parent / "models" / "best_model.pkl"
    
    if not model_path.exists():
        logger.warning(f"Model not found at {model_path}, predictions will fail")
    else:
        load_model(model_path)


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    return HealthResponse(status="ok")


@app.post("/predict", response_model=PredictResponse)
async def predict(request: PredictRequest):
    """Predict delay risk for a station."""
    try:
        # Compute features
        features_dict, latest_bucket_start = compute_features_online(
            request.line_id, request.stop_id
        )
        
        # Convert features to array in correct order
        feature_array = np.array([[features_dict[key] for key in FEATURE_ORDER]], dtype=np.float32)
        
        # Get model and make prediction
        model = get_model()
        
        # Handle both sklearn and LightGBM models
        if hasattr(model, 'predict_proba'):
            # Sklearn model
            risk_probability = float(model.predict_proba(feature_array)[0, 1])
            risk_label = int(model.predict(feature_array)[0])
        else:
            # LightGBM model
            proba = model.predict(feature_array, num_iteration=model.best_iteration if hasattr(model, 'best_iteration') else None)
            risk_probability = float(proba[0])
            risk_label = int(1 if risk_probability > 0.5 else 0)
        
        return PredictResponse(
            line_id=request.line_id,
            stop_id=request.stop_id,
            as_of=latest_bucket_start,
            risk_label=risk_label,
            risk_probability=risk_probability,
            features={k: float(v) for k, v in features_dict.items()}
        )
    
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Prediction error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

