from typing import Type, Optional, Any, Iterable

E_NOT_NONE = "Error-Not-None"
E_TYPE = "Error-Type"
E_MIN_L = "Error-Min-Len"
E_MIN_V = "Error-Min-Value"


def any_value(parameter_type: Type, *, min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> Any:
    import numbers

    assert len(kwargs) == 1, f"verify_type(..) expects one and only one parameter."

    parameter_name = list(kwargs)[0]
    value = kwargs.get(parameter_name)

    # Logically not None if we have a min_length
    required = True if min_length is not None else required

    if required is not None:
        assert type(required) == bool, f"""Expected the parameter "required" to be of type bool, found {type(required)}"""

        # These collections cannot be None so default them to empty list or dict.
        if parameter_type == list and value is None:
            value = list()
        elif parameter_type == dict and value is None:
            value = dict()

        if value is None:
            raise AssertionError(f"""{E_NOT_NONE} | The parameter "{parameter_name}" must not be "None".""")

        # No issues with it being None, now verify the actual type
        if type(value) != parameter_type:
            msg = f"""{E_TYPE} | Expected the parameter "{parameter_name}" to be of type {parameter_type}, found {type(value)}"""
            raise AssertionError(msg)
    else:
        # Already expected that "value" can be None
        if value is not None and not isinstance(value, parameter_type):
            msg = f"""{E_TYPE} | Expected the parameter "{parameter_name}" to be None or of type {parameter_type}, found {type(value)}"""
            raise AssertionError(msg)

    # No issues with the type, now verify value attributes.
    if min_length is not None:
        assert type(min_length) == int, f"""Expected the parameter "min_length" to be of type int, found {type(min_length)}"""
        if len(value) < min_length:
            raise AssertionError(f"""{E_MIN_L} | The parameter "{parameter_name}" must have a minimum length of {min_length}, found "{len(value)}".""")

    if min_value is not None:
        assert type(min_value) == int, f"""Expected the parameter "min_value" to be of type int, found {type(min_value)}"""
        if not isinstance(value, numbers.Number):
            raise AssertionError(f"""{E_TYPE} | The parameter "{parameter_name}" must be numerical, found "{type(value)}".""")

        if value < min_value:
            raise AssertionError(f"""{E_MIN_V} | The parameter "{parameter_name}" must have a minimum value of {min_value}, found "{value}".""")

    return value


def str_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> str:
    return any_value(str, min_length=min_length, required=required, min_value=min_value, **kwargs)


def int_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> int:
    return any_value(int, min_length=min_length, required=required, min_value=min_value, **kwargs)


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
        msg = f"""Expected element {i} of "{name}" to be of type {expected_type}, found {type(actual_value)}"""
        assert isinstance(actual_value, expected_type), msg
