"""Utility functions for Email Manager Agent."""

import time
from functools import wraps
from typing import Any, Callable, TypeVar

import structlog

logger = structlog.get_logger()

F = TypeVar("F", bound=Callable[..., Any])


def retry_on_failure(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
) -> Callable[[F], F]:
    """Decorator to retry a function on failure with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts.
        delay: Initial delay between retries in seconds.
        backoff: Multiplier for delay after each retry.

    Returns:
        Decorated function with retry logic.
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            current_delay = delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(
                            "function_retry",
                            function=func.__name__,
                            attempt=attempt + 1,
                            max_retries=max_retries,
                            delay=current_delay,
                            error=str(e),
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            "function_retry_exhausted",
                            function=func.__name__,
                            attempts=max_retries + 1,
                            error=str(e),
                        )

            raise last_exception  # type: ignore

        return wrapper  # type: ignore

    return decorator
