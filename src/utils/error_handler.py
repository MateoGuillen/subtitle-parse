"""
    error_handler.py

    Provides an error handling decorator that logs exceptions and saves tracebacks to a debug log file 
    in BASE_LOGS_DIR. Use @error_handling(default_return=...) to wrap functions with standardized error logging.
"""


import functools
import traceback
from datetime import datetime
import os

from config.settings import BASE_LOGS_DIR
from src.utils.logging_utils import setup_logger
from src.exceptions.exceptions import ExtractionError, TransformationError, LoadError

logger = setup_logger(__name__)

def error_handling(default_return=None):
    """
    Decorator for handling exceptions and saving tracebacks to a debug log file.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (ExtractionError, TransformationError, LoadError) as e:
                logger.error("Error controlado en %s: %s", func.__name__, str(e))
                _save_traceback(func.__name__, e)
                return default_return
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.exception("Error inesperado en %s: %s", func.__name__, str(e))
                _save_traceback(func.__name__, e)
                return default_return
        return wrapper
    return decorator

def _save_traceback(func_name: str, error: Exception):
    """
    Save the traceback of an exception to a debug log file.
    """
    os.makedirs(BASE_LOGS_DIR, exist_ok=True)
    log_file_path = os.path.join(BASE_LOGS_DIR, "debug_errors.debug.log")
    with open(log_file_path, "a", encoding="utf-8") as f:
        f.write(f"\n[{datetime.now().isoformat()}] --- Error en función: {func_name} ---\n")
        f.write(traceback.format_exc())
        f.write("\n" + "-" * 80 + "\n")
