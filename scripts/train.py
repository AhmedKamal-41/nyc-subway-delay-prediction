import sys
import os
import logging
import joblib
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
import mlflow.lightgbm
from nyc_delay_risk.training.train import (
    get_feature_columns, train_logistic_regression, train_lightgbm
)
from nyc_delay_risk.training.evaluate import compute_metrics, plot_confusion_matrix

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    # Load data
    data_dir = Path(__file__).parent.parent / "data"
    logger.info("Loading datasets...")
    train_df = pd.read_parquet(data_dir / "train.parquet")
    val_df = pd.read_parquet(data_dir / "val.parquet")
    test_df = pd.read_parquet(data_dir / "test.parquet")
    
    logger.info(f"Train: {len(train_df)} rows, Val: {len(val_df)} rows, Test: {len(test_df)} rows")
    
    # Extract features and labels
    feature_cols = get_feature_columns(train_df)
    logger.info(f"Using {len(feature_cols)} features")
    
    X_train = train_df[feature_cols].values
    y_train = train_df['label'].values
    X_val = val_df[feature_cols].values
    y_val = val_df['label'].values
    X_test = test_df[feature_cols].values
    y_test = test_df['label'].values
    
    # Set MLflow experiment
    mlflow.set_experiment("nyc_delay_risk")
    
    # Create models directory
    models_dir = Path(__file__).parent.parent / "models"
    models_dir.mkdir(exist_ok=True)
    
    best_val_roc_auc = -1
    best_model = None
    best_model_name = None
    best_test_metrics = None
    
    # Train Logistic Regression
    logger.info("\n=== Training Logistic Regression ===")
    with mlflow.start_run(run_name="logistic_regression"):
        # Train
        lr_model = train_logistic_regression(X_train, y_train)
        
        # Evaluate on validation
        y_val_pred = lr_model.predict(X_val)
        y_val_pred_proba = lr_model.predict_proba(X_val)[:, 1]
        val_metrics = compute_metrics(y_val, y_val_pred, y_val_pred_proba)
        
        # Evaluate on test
        y_test_pred = lr_model.predict(X_test)
        y_test_pred_proba = lr_model.predict_proba(X_test)[:, 1]
        test_metrics = compute_metrics(y_test, y_test_pred, y_test_pred_proba)
        
        # Log parameters
        mlflow.log_params({
            'model': 'logistic_regression',
            'solver': 'liblinear',
            'class_weight': 'balanced',
            'max_iter': 200
        })
        
        # Log metrics
        for metric_name, metric_value in val_metrics.items():
            mlflow.log_metric(f'val_{metric_name}', metric_value)
        for metric_name, metric_value in test_metrics.items():
            mlflow.log_metric(f'test_{metric_name}', metric_value)
        
        logger.info(f"Val ROC-AUC: {val_metrics['roc_auc']:.4f}")
        logger.info(f"Test ROC-AUC: {test_metrics['roc_auc']:.4f}")
        
        # Plot and save confusion matrix
        cm_path = models_dir / "confusion_matrix_lr.png"
        plot_confusion_matrix(y_test, y_test_pred, "Logistic Regression", cm_path)
        mlflow.log_artifact(str(cm_path))
        
        # Log model
        mlflow.sklearn.log_model(lr_model, "model")
        
        # Check if best
        if val_metrics['roc_auc'] > best_val_roc_auc:
            best_val_roc_auc = val_metrics['roc_auc']
            best_model = lr_model
            best_model_name = "logistic_regression"
            best_test_metrics = test_metrics.copy()
    
    # Train LightGBM
    logger.info("\n=== Training LightGBM ===")
    with mlflow.start_run(run_name="lightgbm"):
        # Train
        lgb_model = train_lightgbm(X_train, y_train, X_val, y_val)
        
        # Evaluate on validation
        y_val_pred = lgb_model.predict(X_val, num_iteration=lgb_model.best_iteration)
        y_val_pred_binary = (y_val_pred > 0.5).astype(int)
        val_metrics = compute_metrics(y_val, y_val_pred_binary, y_val_pred)
        
        # Evaluate on test
        y_test_pred = lgb_model.predict(X_test, num_iteration=lgb_model.best_iteration)
        y_test_pred_binary = (y_test_pred > 0.5).astype(int)
        test_metrics = compute_metrics(y_test, y_test_pred_binary, y_test_pred)
        
        # Log parameters
        mlflow.log_params({
            'model': 'lightgbm',
            'objective': 'binary',
            'n_estimators': 500,
            'learning_rate': 0.05,
            'num_leaves': 31,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'early_stopping_rounds': 50
        })
        
        # Log metrics
        for metric_name, metric_value in val_metrics.items():
            mlflow.log_metric(f'val_{metric_name}', metric_value)
        for metric_name, metric_value in test_metrics.items():
            mlflow.log_metric(f'test_{metric_name}', metric_value)
        
        logger.info(f"Val ROC-AUC: {val_metrics['roc_auc']:.4f}")
        logger.info(f"Test ROC-AUC: {test_metrics['roc_auc']:.4f}")
        
        # Plot and save confusion matrix
        cm_path = models_dir / "confusion_matrix_lgb.png"
        plot_confusion_matrix(y_test, y_test_pred_binary, "LightGBM", cm_path)
        mlflow.log_artifact(str(cm_path))
        
        # Log model
        mlflow.lightgbm.log_model(lgb_model, "model")
        
        # Check if best
        if val_metrics['roc_auc'] > best_val_roc_auc:
            best_val_roc_auc = val_metrics['roc_auc']
            best_model = lgb_model
            best_model_name = "lightgbm"
            best_test_metrics = test_metrics.copy()
    
    # Save best model
    logger.info(f"\n=== Saving Best Model ({best_model_name}) ===")
    best_model_path = models_dir / "best_model.pkl"
    joblib.dump(best_model, best_model_path)
    logger.info(f"Best model saved to {best_model_path}")
    logger.info(f"Best validation ROC-AUC: {best_val_roc_auc:.4f}")
    
    # Save metrics JSON
    if best_test_metrics:
        metrics_data = {
            'model_name': best_model_name,
            'val_roc_auc': best_val_roc_auc,
            'test_metrics': best_test_metrics,
            'timestamp': pd.Timestamp.now().isoformat()
        }
        metrics_path = models_dir / "last_metrics.json"
        with open(metrics_path, 'w') as f:
            json.dump(metrics_data, f, indent=2)
        logger.info(f"Metrics saved to {metrics_path}")
    
    logger.info("\nTraining completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

