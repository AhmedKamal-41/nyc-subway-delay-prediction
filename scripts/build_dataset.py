import sys
import os
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nyc_delay_risk.dataset.build import load_facts, create_features, create_label
from nyc_delay_risk.dataset.split import time_split

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    logger.info("Loading facts from database...")
    df = load_facts()
    logger.info(f"Loaded {len(df)} rows")
    
    logger.info("Creating features...")
    df = create_features(df)
    
    logger.info("Creating labels...")
    df = create_label(df)
    
    logger.info("Splitting into train/val/test...")
    train_df, val_df, test_df = time_split(df)
    
    # Create data directory if it doesn't exist
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    
    # Save parquet files
    logger.info("Saving datasets...")
    train_df.to_parquet(data_dir / "train.parquet", index=False)
    val_df.to_parquet(data_dir / "val.parquet", index=False)
    test_df.to_parquet(data_dir / "test.parquet", index=False)
    
    # Print statistics
    logger.info("\n=== Dataset Statistics ===")
    logger.info(f"Train: {len(train_df)} rows ({len(train_df)/len(df)*100:.1f}%)")
    logger.info(f"  Positive label rate: {train_df['label'].mean()*100:.2f}%")
    
    logger.info(f"Val: {len(val_df)} rows ({len(val_df)/len(df)*100:.1f}%)")
    logger.info(f"  Positive label rate: {val_df['label'].mean()*100:.2f}%")
    
    logger.info(f"Test: {len(test_df)} rows ({len(test_df)/len(df)*100:.1f}%)")
    logger.info(f"  Positive label rate: {test_df['label'].mean()*100:.2f}%")
    
    logger.info("\nDataset building completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

