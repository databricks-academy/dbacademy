"""
Common libraries that do not depend on other libraries.
This was moved out of dbgems because dbgems has a dependency on pyspark.
"""
from __future__ import annotations

from typing import Callable

__all__ = ["deprecated", "overrides", "print_warning", "CachedStaticProperty"]

deprecation_log_level = "error"


def print_warning(title: str, message: str, length: int = 100) -> None:
    title_len = length - len(title) - 3
    print(f"""* {title.upper()} {("*"*title_len)}""")
    for line in message.split("\n"):
        print(f"* {line}")
    print("*"*length)


def deprecated(reason=None, action="warn") -> Callable:
    """Decorator to indicate that a function should no longer be used."""
    from functools import wraps
    if not reason:
        reason = "Replacement unknown"
    if not action:
        action = "ignore"
    action = action.lower()

    def decorator(inner_function):
        @wraps(inner_function)
        def wrapper(*args, **kwargs):
            if deprecation_log_level in (None, "ignore") or action == "ignore":
                return inner_function(*args, **kwargs)
            try:
                import inspect
                signature = inspect.signature(inner_function)
            except Exception:
                # Just in case
                signature = "(..)"
            function_name = f"{inner_function.__module__}.{inner_function.__name__}{signature}"
            message = f"{function_name}: {reason}"
            if action == "error" or deprecation_log_level == "error":
                raise DeprecationWarning(message)
            else:
                print_warning(title="DEPRECATED", message=message)
            return inner_function(*args, **kwargs)
        return wrapper

    return decorator


def overrides(func: Callable = None, check_signature: bool = True) -> Callable:
    """Decorator to indicate that a function overrides a base-class function."""
    if callable(func):
        return func
    else:
        return lambda f: f


class CachedStaticProperty:
    """Works like @property and @staticmethod combined"""

    def __init__(self, func):
        self.func = func

    def __get__(self, inst, owner):
        result = self.func()
        setattr(owner, self.func.__name__, result)
        return result
