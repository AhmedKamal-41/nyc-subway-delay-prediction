import sys
import subprocess
import json
import logging
import shutil
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    project_root = Path(__file__).parent.parent
    models_dir = project_root / "models"
    models_dir.mkdir(exist_ok=True)
    
    logger.info("=== Starting Weekly Retraining ===")
    
    # Backup current model and metrics BEFORE training (if exists)
    old_metrics_path = models_dir / "last_metrics.json"
    old_test_f1 = None
    old_metrics_data = None
    best_model_path = models_dir / "best_model.pkl"
    old_model_backup = models_dir / "best_model.pkl.backup"
    
    if old_metrics_path.exists() and best_model_path.exists():
        # Backup current model
        shutil.copy(best_model_path, old_model_backup)
        
        # Load current metrics
        with open(old_metrics_path, 'r') as f:
            old_metrics_data = json.load(f)
        old_test_f1 = old_metrics_data.get('test_metrics', {}).get('f1', 0.0)
        logger.info(f"Current model test F1: {old_test_f1:.6f}")
    else:
        logger.info("No existing model found, will accept new model")
    
    # Step 1: Run dataset build
    logger.info("\nStep 1: Building dataset...")
    result = subprocess.run(
        [sys.executable, "scripts/build_dataset.py"],
        cwd=project_root,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        logger.error("Dataset build failed!")
        logger.error(result.stderr)
        return 1
    
    logger.info("Dataset build completed successfully")
    
    # Step 2: Run training
    logger.info("\nStep 2: Training models...")
    result = subprocess.run(
        [sys.executable, "scripts/train.py"],
        cwd=project_root,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        logger.error("Training failed!")
        logger.error(result.stderr)
        return 1
    
    logger.info("Training completed successfully")
    
    # Step 3: Load and compare metrics
    logger.info("\nStep 3: Comparing model performance...")
    
    # Load new metrics
    new_metrics_path = models_dir / "last_metrics.json"
    if not new_metrics_path.exists():
        logger.error(f"New metrics file not found: {new_metrics_path}")
        logger.error("Training may have failed to save metrics")
        return 1
    
    with open(new_metrics_path, 'r') as f:
        new_metrics_data = json.load(f)
    
    new_test_f1 = new_metrics_data.get('test_metrics', {}).get('f1', 0.0)
    logger.info(f"New model test F1: {new_test_f1:.6f}")
    
    # Step 4: Decide whether to replace model
    improvement_threshold = 0.01
    
    if old_test_f1 is None:
        # No previous model, accept new one
        logger.info("\n=== Decision: Accepting new model (no previous model) ===")
        accept_new_model = True
    else:
        improvement = new_test_f1 - old_test_f1
        logger.info(f"F1 improvement: {improvement:.6f} (threshold: {improvement_threshold})")
        
        if improvement >= improvement_threshold:
            logger.info("\n=== Decision: Accepting new model (improvement >= threshold) ===")
            accept_new_model = True
        else:
            logger.info("\n=== Decision: Keeping previous model (improvement < threshold) ===")
            logger.info(f"New model F1: {new_test_f1:.6f}, Current model F1: {old_test_f1:.6f}")
            logger.info(f"Improvement: {improvement:.6f} < threshold: {improvement_threshold}")
            
            # Restore old model and metrics
            if old_model_backup.exists():
                shutil.copy(old_model_backup, best_model_path)
                logger.info("Restored previous model")
            
            # Restore old metrics - we need to recreate it from the data we loaded
            if old_test_f1 is not None:
                # Recreate old metrics file with the values we saved
                old_metrics_restored = {
                    'model_name': old_metrics_data.get('model_name', 'unknown'),
                    'val_roc_auc': old_metrics_data.get('val_roc_auc', 0.0),
                    'test_metrics': old_metrics_data.get('test_metrics', {}),
                    'timestamp': old_metrics_data.get('timestamp', '')
                }
                with open(old_metrics_path, 'w') as f:
                    json.dump(old_metrics_restored, f, indent=2)
                logger.info("Restored previous metrics")
            
            accept_new_model = False
    
    if accept_new_model:
        logger.info(f"New model at {best_model_path} is now active")
        logger.info(f"Test F1: {new_test_f1:.6f}")
    else:
        logger.info("Previous model remains active")
        logger.info(f"Current F1: {old_test_f1:.6f}")
    
    logger.info("\nWeekly retraining completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

