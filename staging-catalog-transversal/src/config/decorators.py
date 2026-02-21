"""
Decorators for logging and exception handling:

- log_decorator: logs function execution start.
- raise_decorator: logs and re-raises exceptions from decorated functions.
"""
from src.config.logger import logger
import functools

def log_decorator(func):
    """A decorator that logs the start of the execution of the decorated function."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        """Wrapper function that logs the start of the execution."""
        logger.info("[INFO]: Start %s", func.__name__)
        return func(*args, **kwargs)
    return wrapper

def raise_decorator(func):
    """
    A decorator that wraps a function to provide a general-purpose exception handler.
    Logs exceptions and re-raises them.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        """General-purpose exception handler decorator."""
        try:
            return func(*args, **kwargs)
        except Exception as err:
            msg_error = str(err)
            logger.error("[ERROR]: En funcion %s: %s", func.__name__, msg_error)
            raise
    return wrapper
