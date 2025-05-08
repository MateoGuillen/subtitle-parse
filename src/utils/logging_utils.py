""" Utilities for setting up and configuring loggers. """
import logging
import os
from config.settings import BASE_LOGS_DIR


def setup_logger(name: str, log_file: str = BASE_LOGS_DIR) -> logging.Logger:
    """
    Sets up a logger with the given name and log file.

    Args:
        name (str): The name of the logger.
        log_file (str, optional): The path to the log file. Defaults to BASE_LOGS_DIR.

    Returns:
        logging.Logger: The configured logger.
    """
    log_path = f'{log_file}/{name}.log'
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        stream_handler = logging.StreamHandler()
        stream_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%H:%M:%S")
        stream_handler.setFormatter(stream_formatter)

        file_handler = logging.FileHandler(log_path, mode='a')
        file_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(file_formatter)

        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)

    return logger
