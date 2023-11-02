from numbers import Number
from typing import Type, Optional, Any, Iterable

E_NOT_NONE = "Error-Not-None"
E_TYPE = "Error-Type"
E_MIN_L = "Error-Min-Len"
E_MIN_V = "Error-Min-Value"
E_INTERNAL = "Error-Internal"


def __validate_my_parameters(*, parameter_type: Type, min_length: int = None, required: bool = None, min_value: int = None):
    import numbers

    assert parameter_type is not None, f"""{E_INTERNAL} | The parameter 'parameter_type' must be specified."""

    if required is not None:
        assert isinstance(required, bool), f"""{E_INTERNAL} | Expected the parameter 'required' to be of type bool, found {type(required)}."""

    if min_length is not None:
        assert isinstance(min_length, numbers.Number), f"""{E_INTERNAL} | Expected the parameter 'min_length' to be of type Number, found {type(min_length)}."""

    if min_value is not None:
        assert isinstance(min_value, numbers.Number), f"""{E_INTERNAL} | Expected the parameter 'min_value' to be of type Number, found {type(min_value)}."""


def any_value(parameter_type: Type, *, min_length: int = None, required: bool = None, min_value: Number = None, **kwargs) -> Any:
    import numbers

    assert len(kwargs) == 1, f"Error-Internal | verify_type({kwargs}) expects one and only one parameter, found {len(kwargs)}."

    parameter_name = list(kwargs)[0]
    value: Any = kwargs.get(parameter_name)

    # Logically not None if we have a min_length
    required = True if min_length is not None else required

    __validate_my_parameters(required=required, min_length=min_length, min_value=min_value, parameter_type=parameter_type)

    required = False if required is None else required

    if required:
        # These collections cannot be None so default them to empty list or dict.
        if parameter_type == list and value is None:
            value = list()
        elif parameter_type == dict and value is None:
            value = dict()

        assert value is not None, f"""{E_NOT_NONE} | The parameter '{parameter_name}' must be specified."""

        # No issues with it being None, now verify the actual type
        assert isinstance(value, parameter_type), f"""{E_TYPE} | Expected the parameter '{parameter_name}' to be of type {parameter_type}, found {type(value)}."""
    else:
        # Already expected that "value" can be None
        if value is not None and not isinstance(value, parameter_type):
            msg = f"""{E_TYPE} | Expected the parameter '{parameter_name}' to be None or of type {parameter_type}, found {type(value)}."""
            raise AssertionError(msg)

    # No issues with the type, now verify value attributes.
    if min_length is not None:
        from collections.abc import Sized
        assert isinstance(value, Sized), f"""Expected the parameter 'min_length' to be of type Number, found {type(min_length)}."""

        actual_length = len(value)
        assert actual_length >= min_length, f"""{E_MIN_L} | The parameter '{parameter_name}' must have a minimum length of {min_length}, found {actual_length}."""

    if min_value is not None:
        assert isinstance(value, numbers.Number), f"""{E_TYPE} | The parameter '{parameter_name}' to be of type Number, found, found {type(value)}."""
        assert value >= min_value, f"""{E_MIN_V} | The parameter '{parameter_name}' must have a minimum value of '{min_value}', found '{value}'."""

    return value


def str_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> str:
    return any_value(str, min_length=min_length, required=required, min_value=min_value, **kwargs)


def int_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> int:
    return any_value(int, min_length=min_length, required=required, min_value=min_value, **kwargs)


def float_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> int:
    return any_value(float, min_length=min_length, required=required, min_value=min_value, **kwargs)


def bool_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> bool:
    return any_value(bool, min_length=min_length, required=required, min_value=min_value, **kwargs)


def list_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> list:
    return any_value(list, min_length=min_length, required=required, min_value=min_value, **kwargs)


def dict_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> dict:
    return any_value(dict, min_length=min_length, required=required, min_value=min_value, **kwargs)


def iterable_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> Iterable:
    return any_value(Iterable, min_length=min_length, required=required, min_value=min_value, **kwargs)


def element_type(actual_values: Iterable[Any], name, expected_type):
    iterable_value(actual_values=actual_values)
    for i, actual_value in enumerate(actual_values):
        msg = f"""Expected element {i} of '{name}' to be of type {expected_type}, found {type(actual_value)}."""
        assert isinstance(actual_value, expected_type), msg
