"""Shared rotating file and console logging for project scripts."""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def configure_logging(script_file: str) -> logging.Logger:
    project_root = Path(script_file).resolve().parent.parent
    log_dir = project_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("meetup_pipeline")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s - %(message)s"
        )
        file_handler = RotatingFileHandler(
            log_dir / "pipeline.log",
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
