__all__ = ["any_value", "str_value", "int_value", "float_value", "bool_value", "list_value", "dict_value", "iterable_value", "list_of_type", "list_of_strings", "list_of_ints", "list_of_floats", "list_of_bools", "set_of_type", "set_of_strings", "set_of_ints", "set_of_floats", "set_of_bools", "ValidationError"]

import numbers
from typing import Type, Optional, Any, Iterable, List, Dict, Set, Union, TypeVar

E_NOT_NONE = "Error-Not-None"
E_TYPE = "Error-Type"
E_MIN_L = "Error-Min-Len"
E_MIN_V = "Error-Min-Value"
E_INTERNAL = "Error-Internal"

ValidatorReturnType = TypeVar("ValidatorReturnType", bound=Union[list, dict, set, int, float, str, Iterable, Any])


class ValidationError(Exception):

    def __init__(self, message: str):
        self.__message = message

    @property
    def message(self) -> str:
        return self.__message


def __validate(passed: bool, error_message: str) -> None:
    if not passed:
        raise ValidationError(error_message)


def __validate_my_parameters(*, parameter_type: Type, min_length: int = None, required: bool = None, min_value: int = None) -> None:
    import numbers

    __validate(parameter_type is not None, f"""{E_INTERNAL} | The parameter 'parameter_type' must be specified.""")

    if required is not None:
        __validate(isinstance(required, bool), f"""{E_INTERNAL} | Expected the parameter 'required' to be of type bool, found {type(required)}.""")

    if min_length is not None:
        __validate(isinstance(min_length, int), f"""{E_INTERNAL} | Expected the parameter 'min_length' to be of type int, found {type(min_length)}.""")

    if min_value is not None:
        __validate(isinstance(min_value, numbers.Number), f"""{E_INTERNAL} | Expected the parameter 'min_value' to be of type numbers.Number, found {type(min_value)}.""")


def __validate_required(*, required: bool, parameter_name: str, parameter_type: Type, value: Any) -> None:
    if required:
        __validate(value is not None, f"""{E_NOT_NONE} | The parameter '{parameter_name}' must be specified.""")

        # No issues with it being None, now verify the actual type
        __validate(isinstance(value, parameter_type), f"""{E_TYPE} | Expected the parameter '{parameter_name}' to be of type {parameter_type}, found {type(value)}.""")
    else:
        # Already expected that "value" can be None
        if value is not None and not isinstance(value, parameter_type):
            msg = f"""{E_TYPE} | Expected the parameter '{parameter_name}' to be None or of type {parameter_type}, found {type(value)}."""
            raise ValidationError(msg)


def __min_length(*, min_length: int, parameter_name: str, value: Any) -> None:

    if min_length is not None:
        from collections.abc import Sized
        __validate(isinstance(value, Sized), f"""Expected the parameter 'min_length' to be of type Number, found {type(min_length)}.""")

        actual_length = len(value)
        __validate(actual_length >= min_length, f"""{E_MIN_L} | The parameter '{parameter_name}' must have a minimum length of {min_length}, found {actual_length}.""")


def __validate_min_value(*, min_value: numbers.Number, parameter_name: str, value: Any) -> None:

    if min_value is not None:
        __validate(isinstance(value, numbers.Number), f"""{E_TYPE} | The parameter '{parameter_name}' to be of type Number, found, found {type(value)}.""")
        __validate(value >= min_value, f"""{E_MIN_V} | The parameter '{parameter_name}' must have a minimum value of '{min_value}', found '{value}'.""")


def any_value(parameter_type: Type[ValidatorReturnType], *, min_length: int = None, required: bool = False, min_value: numbers.Number = None, parameter_name: str = None, **kwargs) -> ValidatorReturnType:

    __validate(len(kwargs) == 1, f"Error-Internal | validate.any_value({kwargs}) expects one and only one parameter, found {len(kwargs)}.")

    true_parameter_name = list(kwargs)[0]
    value: Any = kwargs.get(true_parameter_name)
    parameter_name = true_parameter_name or parameter_name

    # Logically not None if we have a min_length
    required = True if min_length is not None else required

    __validate_my_parameters(required=required, min_length=min_length, min_value=min_value, parameter_type=parameter_type)

    required = False if required is None else required

    __validate_required(required=required, parameter_name=parameter_name, parameter_type=parameter_type, value=value)
    __min_length(min_length=min_length, parameter_name=parameter_name, value=value)
    __validate_min_value(min_value=min_value, parameter_name=parameter_name, value=value)

    return value


def str_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> str:
    return any_value(parameter_type=str, min_length=min_length, required=required, min_value=min_value, **kwargs)


def int_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> int:
    return any_value(parameter_type=int, min_length=min_length, required=required, min_value=min_value, **kwargs)


def float_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> float:
    return any_value(parameter_type=float, min_length=min_length, required=required, min_value=min_value, **kwargs)


def bool_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> bool:
    return any_value(parameter_type=bool, min_length=min_length, required=required, min_value=min_value, **kwargs)


def list_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> list:
    return any_value(parameter_type=list, min_length=min_length, required=required, min_value=min_value, **kwargs)


def dict_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> dict:
    return any_value(parameter_type=dict, min_length=min_length, required=required, min_value=min_value, **kwargs)


def set_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> set:
    return any_value(parameter_type=set, min_length=min_length, required=required, min_value=min_value, **kwargs)


def iterable_value(min_length: int = None, required: Optional[bool] = None, min_value=None, **kwargs) -> Iterable:
    return any_value(Iterable, min_length=min_length, required=required, min_value=min_value, **kwargs)


def __element_types(parameter_type: Type[ValidatorReturnType], element_type: Type, required: bool, auto_create: bool, **kwargs) -> ValidatorReturnType:

    __validate(len(kwargs) == 1, f"Error-Internal | validate.element_type({kwargs}) expects one and only one parameter, found {len(kwargs)}.")

    __validate(parameter_type is not None, f"""{E_INTERNAL} | The parameter 'parameter_type' must be specified.""")
    __validate(element_type is not None, f"""{E_INTERNAL} | The parameter 'element_type' must be specified.""")

    parameter_name = list(kwargs)[0]
    raw_value: Any = kwargs.get(parameter_name)

    if auto_create and raw_value is None:
        required = True  # Obviously...
        if parameter_type in [list, List]:
            values = list()
        elif parameter_type in [dict, Dict]:
            values = dict()
        elif parameter_type in [set, Set]:
            values = set()
        else:
            raise Exception(f"Cannot auto-create collections of type {parameter_type}.")
    else:
        values: Iterable = raw_value

    values = any_value(values=values, parameter_type=parameter_type, required=required, parameter_name=parameter_name)

    if values is not None:
        if parameter_type in [list, List, set, Set]:
            for i, actual_value in enumerate(values):
                msg = f"""Expected element {i} of '{parameter_name}' to be of type {element_type}, found {type(actual_value)}."""
                __validate(isinstance(actual_value, element_type), msg)
        else:
            raise Exception(f"Cannot auto-create collections of type {parameter_type}.")

    return values


def list_of_type(element_type: Type[ValidatorReturnType], required: bool = False, auto_create: bool = False, **kwargs) -> List:
    return __element_types(parameter_type=list, element_type=element_type, required=required, auto_create=auto_create, **kwargs)


def list_of_strings(required: bool = False, auto_create: bool = False, **kwargs) -> List[str]:
    return __element_types(parameter_type=List, element_type=str, required=required, auto_create=auto_create, **kwargs)


def list_of_ints(required: bool = False, auto_create: bool = False, **kwargs) -> List[int]:
    return __element_types(parameter_type=List, element_type=int, required=required, auto_create=auto_create, **kwargs)


def list_of_floats(required: bool = False, auto_create: bool = False, **kwargs) -> List[float]:
    return __element_types(parameter_type=List, element_type=float, required=required, auto_create=auto_create, **kwargs)


def list_of_bools(required: bool = False, auto_create: bool = False, **kwargs) -> List[bool]:
    return __element_types(parameter_type=List, element_type=bool, required=required, auto_create=auto_create, **kwargs)


# def dict_of_type(element_type: Type[ValidatorReturnType], required: bool = False, auto_create: bool = False, **kwargs) -> Dict[Any, Any]:
#     return __element_types(parameter_type=Dict, element_type=element_type, required=required, auto_create=auto_create, **kwargs)
#
#
# def dict_of_int_keys(element_type: Type[ValidatorReturnType], required: bool = False, auto_create: bool = False, **kwargs) -> Dict[int, Any]:
#     return __element_types(parameter_type=Dict, element_type=element_type, required=required, auto_create=auto_create, **kwargs)
#
#
# def dict_of_str(element_type: Type[ValidatorReturnType], required: bool = False, auto_create: bool = False, **kwargs) -> Dict[str, Any]:
#     return __element_types(parameter_type=Dict, element_type=element_type, required=required, auto_create=auto_create, **kwargs)


def set_of_type(element_type: Type[ValidatorReturnType], required: bool = False, auto_create: bool = False, **kwargs) -> Set:
    return __element_types(parameter_type=Set, element_type=element_type, required=required, auto_create=auto_create, **kwargs)


def set_of_strings(required: bool = False, auto_create: bool = False, **kwargs) -> Set[str]:
    return __element_types(parameter_type=Set, element_type=str, required=required, auto_create=auto_create, **kwargs)


def set_of_ints(required: bool = False, auto_create: bool = False, **kwargs) -> Set[int]:
    return __element_types(parameter_type=Set, element_type=int, required=required, auto_create=auto_create, **kwargs)


def set_of_floats(required: bool = False, auto_create: bool = False, **kwargs) -> Set[int]:
    return __element_types(parameter_type=Set, element_type=float, required=required, auto_create=auto_create, **kwargs)


def set_of_bools(required: bool = False, auto_create: bool = False, **kwargs) -> Set[bool]:
    return __element_types(parameter_type=Set, element_type=bool, required=required, auto_create=auto_create, **kwargs)
