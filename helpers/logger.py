import os
import logging
from datetime import datetime

def setup_logger(name: str) -> logging.Logger:
    """
    Sets up a logger with the given name, configured to write to a
    file in the "logs" directory with the current date in the filename,
    and also write to stdout. If the logger already has handlers,
    doesn't do anything.

    Returns:
        logger (logging.Logger): the configured logger.
    """
    log_directory = "logs"
    os.makedirs(log_directory, exist_ok=True)
    log_filename = os.path.join(log_directory, datetime.now().strftime("log_%Y-%m-%d.log"))

    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger