"""
Common libraries that do not depend on other libraries.
This was moved out of dbgems because dbgems has a dependency on pyspark.
"""
from __future__ import annotations

from typing import Callable, Any, Iterable

from dbacademy.common.cloud_class import Cloud

__all__ = ["deprecated", "overrides", "print_title", "print_warning", "CachedStaticProperty", "validate_type", "validate_element_type", "Cloud"]

deprecation_log_level = "error"


def print_title(title: str, divider: str = "-", length: int = 100) -> None:
    print(divider * length)
    tail_length = length - 3 - len(title)
    tail = divider * tail_length
    print(f"- {title} {tail}")


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


# @deprecated("Use verify_type() instead")
def validate_type(actual_value: Any, name: str, expected_type: Any):
    msg = f"""Expected the parameter "{name}" to be of type {expected_type}, found {type(actual_value)}"""
    assert isinstance(actual_value, expected_type), msg
    return actual_value


E_NOT_NONE = "Error-Not-None"
E_TYPE = "Error-Type"
E_MIN_L = "Error-Min-Len"
E_MIN_V = "Error-Min-Value"


def verify_type(parameter_type: Any, *, min_length: int = None, non_none: Any = None, min_value=None, **kwargs):
    import numbers

    assert len(kwargs) == 1, f"validate_type_2() expects two and only two parameters."

    parameter_name = list(kwargs)[0]
    value = kwargs.get(parameter_name)

    # Logically not None if we have a min_length
    non_none = True if min_length is not None else non_none

    if non_none is not None:
        assert type(non_none) == bool, f"""Expected the parameter "non_none" to be of type bool, found {type(non_none)}"""

        # These collections cannot be None so default them to empty list or dict.
        if parameter_type == list and value is None:
            value = list()
        elif parameter_type == dict and value is None:
            value = dict()

        if value is None:
            raise AssertionError(f"""{E_NOT_NONE}| The parameter "{parameter_name}" must  not be "None".""")

        # No issues with it being None, now verify the actual type
        if type(value) != parameter_type:
            msg = f"""{E_TYPE}| Expected the parameter "{parameter_name}" to be of type {parameter_type}, found {type(value)}"""
            raise AssertionError(msg)
    else:
        # Already expected that "value" can be None
        if value is not None and type(value) != parameter_type:
            msg = f"""{E_TYPE}| Expected the parameter "{parameter_name}" to be None or of type {parameter_type}, found {type(value)}"""
            raise AssertionError(msg)

    # No issues with the type, now verify value attributes.
    if min_length is not None:
        assert type(min_length) == int, f"""Expected the parameter "min_length" to be of type int, found {type(min_length)}"""
        if len(value) < min_length:
            raise AssertionError(f"""{E_MIN_L}| The parameter "{parameter_name}" must have a minimum length of {min_length}, found "{len(value)}".""")

    if min_value is not None:
        assert type(min_value) == int, f"""Expected the parameter "min_value" to be of type int, found {type(min_value)}"""
        if not isinstance(value, numbers.Number):
            raise AssertionError(f"""{E_TYPE}| The parameter "{parameter_name}" must be numerical, found "{type(value)}".""")

        if value < min_value:
            raise AssertionError(f"""{E_MIN_V}| The parameter "{parameter_name}" must have a minimum value of {min_value}, found "{value}".""")

    return value


def validate_element_type(actual_values: Iterable[Any], name, expected_type):
    validate_type(actual_values, "actual_values", Iterable)
    for i, actual_value in enumerate(actual_values):
        msg = f"""Expected element {i} of "{name}" to be of type {expected_type}, found {type(actual_value)}"""
        assert isinstance(actual_value, expected_type), msg


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
            elif line.strip() != "":
                pos = line.index("=")
                key = line[:pos].strip()
                value = line[pos + 1:].strip()

                if key == "host" and value.endswith("/"):
                    value = value[:-1]

                sections[section_name][key] = value

        return sections
