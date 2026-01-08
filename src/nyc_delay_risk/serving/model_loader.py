import joblib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Module-level variable to store loaded model
_model = None


def load_model(model_path):
    """Load model from pickle file and store in module-level variable."""
    global _model
    logger.info(f"Loading model from {model_path}")
    _model = joblib.load(model_path)
    logger.info("Model loaded successfully")
    return _model


def get_model():
    """Get the loaded model."""
    if _model is None:
        raise RuntimeError("Model not loaded. Call load_model() first.")
    return _model

