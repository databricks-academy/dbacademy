"""
Common libraries that do not depend on other libraries.
This was moved out of dbgems because dbgems has a dependency on pyspark.
"""
from __future__ import annotations

__all__ = ["deprecation_log_level", "deprecated", "overrides", "print_title", "print_warning", "CachedStaticProperty", "clean_string", "load_databricks_cfg", "Cloud", "validate", "ValidationError", "combine_var_args"]

from typing import Callable, Any, TypeVar, List, Iterable, Tuple, Dict
from dbacademy.common.cloud import Cloud
from dbacademy.common.validator import Validator, ValidationError

deprecation_log_level = "error"
ParamType = TypeVar("ParamType")


def validate(**kwargs) -> Validator:
    return Validator(**kwargs)


def combine_var_args(first: Any, others: Tuple) -> List[Any]:
    values = list()

    if isinstance(first, str):
        # Process strings because they are iterable.
        values.append(first)
    elif isinstance(first, Dict):
        # Processes all dictionaries as if they are keys only.
        values.extend(first.keys())
    elif isinstance(first, Iterable):
        # Processes all iterables next.
        values.extend(first)
    else:
        # All other values should be singletons.
        values.append(first)

    # Add all the "other" values
    values.extend(others)

    return values


def print_title(title: str, divider: str = "-", length: int = 100) -> None:
    tail_length = length - 3 - len(title)
    tail = divider * tail_length
    print(f"-- {title} {tail}")


def print_warning(title: str, message: str, length: int = 100) -> None:
    title_len = length - len(title) - 4
    print()
    print(f"""** {title.upper()} {("*"*title_len)}""")
    for line in message.split("\n"):
        print(f"* {line}")
    print("*"*length)
    print()


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


# noinspection PyUnusedLocal
# TODO remove or implement unused parameter
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


def clean_string(value, replacement: str = "_") -> str:
    import re
    replacement_2x = replacement+replacement
    value = re.sub(r"[^a-zA-Z\d]", replacement, str(value))
    while replacement_2x in value:
        value = value.replace(replacement_2x, replacement)
    return value


def load_databricks_cfg(path: str):
    with open(path) as file:
        section_name = None
        sections = dict()

        lines = file.read().split("\n")
        for line in lines:
            if line.startswith("["):
                section_name = line.strip()[1:-1]
                sections[section_name] = dict()
            elif line.strip() != "" and not line.strip().startswith("#"):
                pos = line.index("=")
                key = line[:pos].strip()
                value = line[pos + 1:].strip()

                if key == "host" and value.endswith("/"):
                    value = value[:-1]

                sections[section_name][key] = value

        return sections
