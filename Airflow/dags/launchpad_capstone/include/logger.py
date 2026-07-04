import logging
import os
from pathlib import Path

LOG_FILE = "/opt/airflow/dags/launchpad_capstone/pipeline_logs/pipeline.log"

# Extract directory from file path
log_dir = os.path.dirname(LOG_FILE)

# Create directory if it doesn't exist
os.makedirs(log_dir, exist_ok=True)

def get_logger(name: str) -> logging.Logger:
    """
    Create and return a configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(LOG_FILE)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s"
        )

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger