import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import numpy as np
from nyc_delay_risk.monitoring.drift import compute_psi, get_feature_values

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    # Define time windows
    now = datetime.now()
    baseline_start = now - timedelta(days=7)
    baseline_end = now - timedelta(days=1)  # 24 hours ago
    current_start = now - timedelta(days=1)
    current_end = now
    
    logger.info(f"Baseline window: {baseline_start.isoformat()} to {baseline_end.isoformat()}")
    logger.info(f"Current window: {current_start.isoformat()} to {current_end.isoformat()}")
    
    # Features to compute PSI for
    features = [
        'alerts_sum_15m',
        'trip_updates_sum_15m',
        'vehicle_positions_sum_15m'
    ]
    
    results = {
        'report_timestamp': now.isoformat(),
        'baseline_window': {
            'start': baseline_start.isoformat(),
            'end': baseline_end.isoformat()
        },
        'current_window': {
            'start': current_start.isoformat(),
            'end': current_end.isoformat()
        },
        'psi_values': {}
    }
    
    # Compute PSI for each feature
    for feature_name in features:
        logger.info(f"\nComputing PSI for {feature_name}...")
        
        # Get baseline values
        logger.info("  Fetching baseline values...")
        baseline_values = get_feature_values(feature_name, baseline_start, baseline_end)
        logger.info(f"  Baseline samples: {len(baseline_values)}")
        
        # Get current values
        logger.info("  Fetching current values...")
        current_values = get_feature_values(feature_name, current_start, current_end)
        logger.info(f"  Current samples: {len(current_values)}")
        
        if len(baseline_values) == 0 or len(current_values) == 0:
            logger.warning(f"  Insufficient data for {feature_name}, skipping PSI calculation")
            results['psi_values'][feature_name] = None
            continue
        
        # Convert to numpy arrays
        baseline_array = np.array(baseline_values, dtype=float)
        current_array = np.array(current_values, dtype=float)
        
        # Compute PSI
        psi = compute_psi(current_array, baseline_array, bins=10)
        
        logger.info(f"  PSI: {psi:.6f}")
        results['psi_values'][feature_name] = float(psi) if not np.isnan(psi) else None
    
    # Print summary
    logger.info("\n=== Drift Report Summary ===")
    for feature_name, psi in results['psi_values'].items():
        if psi is not None:
            logger.info(f"{feature_name}: PSI = {psi:.6f}")
        else:
            logger.info(f"{feature_name}: PSI = N/A (insufficient data)")
    
    # Save report
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    report_path = data_dir / "drift_report.json"
    
    with open(report_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"\nReport saved to {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

