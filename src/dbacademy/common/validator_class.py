__all__ = ["Validator", "ValidationError"]

import numbers
from typing import Type, Any, Iterable, List, Dict, Set, Union, TypeVar, Sized

E_NOT_NONE = "Error-Not-None"
E_TYPE = "Error-Type"
ELEM_TYPE = "Element-Type"
E_MIN_L = "Error-Min-Len"
E_MIN_V = "Error-Min-Value"
E_MAX_V = "Error-Max-Value"
E_INTERNAL = "Error-Internal"

ReturnType = TypeVar("ReturnType", bound=Union[list, dict, set, int, float, str, Iterable, Any])
ElementType = TypeVar("ReturnType", bound=Union[list, dict, set, int, float, str, Iterable, Any])


def do_validate(*, passed: bool, message: str) -> None:
    if not passed:
        raise ValidationError(message)


class ValidatorProxy:

    def __init__(self, *, parameter_name: str, value: Any):
        self.__value = value
        self.__parameter_name = parameter_name

    @property
    def value(self) -> Any:
        return self.__value

    def _reset_value(self, value: Any) -> Any:
        self.__value = value

    @property
    def parameter_name(self) -> str:
        return self.__parameter_name


def do_validate_data_type(name: str, data_type: Union[type]) -> None:
    import typing

    do_validate(passed=data_type is not None, message=f"""{E_INTERNAL} | Expected the Validator's parameter '{name}' to be specified.""")

    # noinspection PyUnresolvedReferences,PyProtectedMember
    passed = isinstance(data_type, (type, typing._SpecialGenericAlias))
    do_validate(passed=passed, message=f"""{E_INTERNAL} | Expected the Validator's parameter '{name}' to be a python "type", found {type(data_type)}.""")


def do_min_length(*, v: ValidatorProxy, min_length: int) -> None:

    # We need to verify that min_length is of tye int
    do_validate(passed=min_length is not None, message=f"""{E_INTERNAL} | Expected the Validator's parameter 'min_length' to be specified.""")
    do_validate(passed=isinstance(min_length, int), message=f"""{E_INTERNAL} | Expected the Validator's parameter 'min_length' to be of type int, found {type(min_length)}.""")

    if min_length > 0:
        # We cannot test the length if the value is not of type Sized.
        do_validate(passed=isinstance(v.value, Sized), message=f"""{E_TYPE} |  Expected the parameter '{v.parameter_name}' to be of type Sized, found {type(v.value)}.""")

        actual_length = len(v.value)
        do_validate(passed=actual_length >= min_length, message=f"""{E_MIN_L} | The parameter '{v.parameter_name}' must have a minimum length of {min_length}, found {actual_length}.""")


def do_min_value(*, v: ValidatorProxy, min_value: numbers.Number) -> None:

    # We need to verify that min_value is of type numbers.Number
    do_validate(passed=min_value is not None, message=f"""{E_INTERNAL} | Expected the Validator's parameter 'min_value' to be specified.""")
    do_validate(passed=isinstance(min_value, numbers.Number), message=f"""{E_INTERNAL} | Expected the Validator's parameter 'min_value' to be of type numbers.Number, found {type(min_value)}.""")

    # We cannot test the min value if the value is not of type numbers.Number
    do_validate(passed=isinstance(v.value, numbers.Number), message=f"""{E_TYPE} | Expected the parameter '{v.parameter_name}' to be of type numbers.Number, found {type(v.value)}.""")

    do_validate(passed=v.value >= min_value, message=f"""{E_MIN_V} | The parameter '{v.parameter_name}' must have a minimum value of '{min_value}', found '{v.value}'.""")


def do_max_value(*, v: ValidatorProxy, max_value: numbers.Number) -> None:

    # We need to verify that max_value is of type numbers.Number
    do_validate(passed=max_value is not None, message=f"""{E_INTERNAL} | Expected the Validator's parameter 'max_value' to be specified.""")
    do_validate(passed=isinstance(max_value, numbers.Number), message=f"""{E_INTERNAL} | Expected the Validator's parameter 'max_value' to be of type numbers.Number, found {type(max_value)}.""")

    # We cannot test the max value if the value is not of type numbers.Number
    do_validate(passed=isinstance(v.value, numbers.Number), message=f"""{E_TYPE} | Expected the parameter '{v.parameter_name}' to be of type numbers.Number, found {type(v.value)}.""")

    do_validate(passed=v.value <= max_value, message=f"""{E_MAX_V} | The parameter '{v.parameter_name}' must have a maximum value of '{max_value}', found '{v.value}'.""")


class ValidationError(Exception):

    def __init__(self, message: str):
        self.__message = message

    @property
    def message(self) -> str:
        return self.__message


class SizedValidator(ValidatorProxy):

    def __init__(self, *, parameter_name: str, value: Sized):
        super().__init__(parameter_name=parameter_name, value=value)

    def _sized_of_type(self, *, parameter_type: ElementType, element_type: ElementType, min_length: int) -> None:

        do_validate_data_type("element_type", element_type)
        do_min_length(v=self, min_length=min_length)

        if isinstance(self.value, (List, Set)):
            for i, actual_value in enumerate(self.value):
                message = f"""{ELEM_TYPE} | Expected element {i} of '{self.parameter_name}' to be of type {element_type}, found "{actual_value}" of type {type(actual_value)}."""
                do_validate(passed=isinstance(actual_value, element_type), message=message)

        elif isinstance(self.value, Dict):
            for i, key in enumerate(self.value.keys()):
                message = f"""{ELEM_TYPE} | Expected key {i} of '{self.parameter_name}' to be of type {element_type}, found "{key}" of type {type(key)}."""
                do_validate(passed=isinstance(key, element_type), message=message)

        elif self.value is not None:
            raise Exception(f"Cannot validate collections of type {parameter_type}.")


class IterableValidator(SizedValidator):

    def __init__(self, *, parameter_name: str, value: Sized):
        super().__init__(parameter_name=parameter_name, value=value)

    def of_type(self, element_type: Type[ElementType], *, min_length: int = 0) -> Iterable[ElementType]:
        self._sized_of_type(parameter_type=Iterable, element_type=element_type, min_length=min_length)
        return self.value

    def of_any(self, *, min_length: int = 0) -> Iterable[Any]:
        return self.of_type(element_type=Any, min_length=min_length)

    def of_str(self, *, min_length: int = 0) -> Iterable[str]:
        return self.of_type(element_type=str, min_length=min_length)

    def of_int(self, *, min_length: int = 0) -> Iterable[int]:
        return self.of_type(element_type=int, min_length=min_length)

    def of_float(self, *, min_length: int = 0) -> Iterable[float]:
        return self.of_type(element_type=float, min_length=min_length)

    def of_bool(self, *, min_length: int = 0) -> Iterable[bool]:
        return self.of_type(element_type=bool, min_length=min_length)


class ListValidator(SizedValidator):

    def __init__(self, *, parameter_name: str, value: List):
        super().__init__(parameter_name=parameter_name, value=value)

    def of_type(self, element_type: Type[ElementType], *, min_length: int = 0) -> List[ElementType]:
        self._sized_of_type(parameter_type=List, element_type=element_type, min_length=min_length)
        return self.value

    def of_any(self, *, min_length: int = 0) -> List[Any]:
        return self.of_type(element_type=Any, min_length=min_length)

    def of_str(self, *, min_length: int = 0) -> List[str]:
        return self.of_type(element_type=str, min_length=min_length)

    def of_int(self, *, min_length: int = 0) -> List[int]:
        return self.of_type(element_type=int, min_length=min_length)

    def of_float(self, *, min_length: int = 0) -> List[float]:
        return self.of_type(element_type=float, min_length=min_length)

    def of_bool(self, *, min_length: int = 0) -> List[bool]:
        return self.of_type(element_type=bool, min_length=min_length)


class SetValidator(SizedValidator):

    def __init__(self, *, parameter_name: str, value: Set):
        super().__init__(parameter_name=parameter_name, value=value)

    def of_type(self, element_type: Type[ElementType], *, min_length: int = 0) -> Set[ElementType]:
        self._sized_of_type(parameter_type=Set, element_type=element_type, min_length=min_length)
        return self.value

    def of_any(self, *, min_length: int = 0) -> Set[Any]:
        return self.of_type(element_type=Any, min_length=min_length)

    def of_str(self, *, min_length: int = 0) -> Set[str]:
        return self.of_type(element_type=str, min_length=min_length)

    def of_int(self, *, min_length: int = 0) -> Set[int]:
        return self.of_type(element_type=int, min_length=min_length)

    def of_float(self, *, min_length: int = 0) -> Set[float]:
        return self.of_type(element_type=float, min_length=min_length)

    def of_bool(self, *, min_length: int = 0) -> Set[bool]:
        return self.of_type(element_type=bool, min_length=min_length)


class DictValidator(SizedValidator):

    def __init__(self, *, parameter_name: str, value: Dict):
        super().__init__(parameter_name=parameter_name, value=value)

    def keys_of_type(self, element_type: Type[ElementType], *, min_length: int = 0) -> Dict[ElementType, Any]:
        self._sized_of_type(parameter_type=Dict, element_type=element_type, min_length=min_length)
        return self.value

    def keys_of_any(self, *, min_length: int = 0) -> Dict[Any, Any]:
        return self.keys_of_type(element_type=Any, min_length=min_length)

    def keys_of_str(self, *, min_length: int = 0) -> Dict[str, Any]:
        return self.keys_of_type(element_type=str, min_length=min_length)

    def keys_of_int(self, *, min_length: int = 0) -> Dict[int, Any]:
        return self.keys_of_type(element_type=int, min_length=min_length)

    def keys_of_float(self, *, min_length: int = 0) -> Dict[float, Any]:
        return self.keys_of_type(element_type=float, min_length=min_length)

    def keys_of_bool(self, *, min_length: int = 0) -> Dict[bool, Any]:
        return self.keys_of_type(element_type=bool, min_length=min_length)


class StringValidator(SizedValidator):

    def __init__(self, *, parameter_name: str, value: str):
        super().__init__(parameter_name=parameter_name, value=value)

    def min_length(self, min_length: int) -> str:
        do_min_length(v=self, min_length=min_length)
        return self.value


class NumberValidator(ValidatorProxy):

    def __init__(self, *, parameter_name: str, value: numbers.Number):
        super().__init__(parameter_name=parameter_name, value=value)

    def _number_min_value(self, *, number_type: Type[ReturnType], min_value: numbers.Number) -> ReturnType:
        do_validate_data_type("number_type", number_type)
        do_min_value(v=self, min_value=min_value)

        return self.value

    def _number_max_value(self, *, number_type: Type[ReturnType], max_value: numbers.Number) -> ReturnType:
        do_validate_data_type("number_type", number_type)
        do_max_value(v=self, max_value=max_value)

        return self.value

    def _number_min_max_value(self, *, number_type: Type[ReturnType], min_value: numbers.Number, max_value: numbers.Number) -> ReturnType:
        do_validate_data_type("number_type", number_type)
        do_min_value(v=self, min_value=min_value)

        do_validate_data_type("number_type", number_type)
        do_max_value(v=self, max_value=max_value)

        return self.value

    def min_value(self, min_value: numbers.Number) -> numbers.Number:
        return self._number_min_value(number_type=numbers.Number, min_value=min_value)

    def max_value(self, max_value: numbers.Number) -> numbers.Number:
        return self._number_max_value(number_type=numbers.Number, max_value=max_value)

    def min_max_value(self, min_value: numbers.Number, max_value: numbers.Number) -> numbers.Number:
        return self._number_min_max_value(number_type=numbers.Number, min_value=min_value, max_value=max_value)


class IntValidator(NumberValidator):

    def __init__(self, *, parameter_name: str, value: int):
        super().__init__(parameter_name=parameter_name, value=value)

    def min_value(self, min_value: int) -> int:
        return self._number_min_value(number_type=int, min_value=min_value)

    def max_value(self, max_value: int) -> int:
        return self._number_max_value(number_type=int, max_value=max_value)

    def min_max_value(self, min_value: int, max_value: int) -> int:
        return self._number_min_max_value(number_type=int, min_value=min_value, max_value=max_value)

    @property
    def int(self) -> int:
        return self.value


class FloatValidator(NumberValidator):

    def __init__(self, *, parameter_name: str, value: float):
        super().__init__(parameter_name=parameter_name, value=value)

    def min_value(self, min_value: float) -> float:
        return self._number_min_value(number_type=float, min_value=min_value)

    def max_value(self, max_value: float) -> float:
        return self._number_max_value(number_type=float, max_value=max_value)

    def min_max_value(self, min_value: float, max_value: float) -> float:
        return self._number_min_max_value(number_type=float, min_value=min_value, max_value=max_value)

    @property
    def float(self) -> float:
        return self.value


class Validator(ValidatorProxy):

    def __init__(self, **kwargs):
        do_validate(passed=len(kwargs) == 1, message=f"{E_INTERNAL} | validate(..) expects one and only one parameter, found {len(kwargs)}.")

        parameter_name = list(kwargs)[0]
        value: Any = kwargs.get(parameter_name)

        super().__init__(parameter_name=parameter_name, value=value)

    def __required(self):
        do_validate(passed=self.value is not None, message=f"""{E_NOT_NONE} | The parameter '{self.parameter_name}' must be specified.""")
        
    def type(self, parameter_type: Type[ReturnType]) -> ReturnType:

        if parameter_type is float and isinstance(self.value, int):
            self._reset_value(float(self.value))

        do_validate_data_type("parameter_type", parameter_type)

        if self.value is not None:
            do_validate(passed=isinstance(self.value, parameter_type), message=f"""{E_TYPE} | Expected the parameter '{self.parameter_name}' to be of type {parameter_type}, found {type(self.value)}.""")

        return self.value

    def type_required(self, *, parameter_type: Type[ReturnType]) -> ReturnType:
        self.__required()
        return self.type(parameter_type=parameter_type)

    def str(self) -> StringValidator:
        self.type(parameter_type=str)
        return StringValidator(parameter_name=self.parameter_name, value=self.value)

    def str_required(self) -> StringValidator:
        self.__required()
        return self.str()

    def number(self) -> NumberValidator:
        self.type(parameter_type=numbers.Number)
        return NumberValidator(parameter_name=self.parameter_name, value=self.value)

    def number_required(self) -> NumberValidator:
        self.__required()
        return self.number()

    def int(self) -> IntValidator:
        self.type(parameter_type=int)
        return IntValidator(parameter_name=self.parameter_name, value=self.value)

    def int_required(self) -> IntValidator:
        self.__required()
        return self.int()

    def float(self) -> FloatValidator:
        self.type(parameter_type=float)
        return FloatValidator(parameter_name=self.parameter_name, value=self.value)

    def float_required(self) -> FloatValidator:
        self.__required()
        return self.float()

    def bool(self) -> bool:
        self.type(parameter_type=bool)
        return self.value

    def bool_required(self) -> bool:
        self.__required()
        return self.bool()

    def iterable(self) -> IterableValidator:
        self.type(parameter_type=Iterable)
        return IterableValidator(parameter_name=self.parameter_name, value=self.value)

    def iterable_required(self) -> IterableValidator:
        self.__required()
        return self.iterable()

    def list(self, *, create: bool = False) -> ListValidator:
        do_validate(passed=isinstance(create, bool), message=f"""{E_INTERNAL} | Expected the Validator's parameter 'create' to be of type bool, found {type(create)}.""")

        if create:
            self._reset_value(self.value or list())

        self.type(parameter_type=list)
        return ListValidator(parameter_name=self.parameter_name, value=self.value)

    def list_required(self) -> ListValidator:
        self.__required()
        return self.list()

    def set(self, *, create: bool = False) -> SetValidator:
        do_validate(passed=isinstance(create, bool), message=f"""{E_INTERNAL} | Expected the Validator's parameter 'create' to be of type bool, found {type(create)}.""")

        if create:
            self._reset_value(self.value or set())

        self.type(parameter_type=set)
        return SetValidator(parameter_name=self.parameter_name, value=self.value)

    def set_required(self) -> SetValidator:
        self.__required()
        return self.set()

    def dict(self, *, create: bool = False) -> DictValidator:
        do_validate(passed=isinstance(create, bool), message=f"""{E_INTERNAL} | Expected the Validator's parameter 'create' to be of type bool, found {type(create)}.""")

        if create:
            self._reset_value(self.value or dict())

        self.type(parameter_type=dict)
        return DictValidator(parameter_name=self.parameter_name, value=self.value)

    def dict_required(self) -> DictValidator:
        self.__required()
        return self.dict()


# # noinspection PyMethodMayBeStatic
# class Validator(ValidatorProxy):
#
#     def __init__(self, **kwargs):
#         super().__init__(parameter_name=parameter_name, value=value)
#
#     # def __validate_my_parameters(self, *, min_length: Optional[int], required: bool, min_value: Optional[int]) -> None:
#     #     import numbers
#     #
#     #     self.__validate(isinstance(required, bool), f"""{E_INTERNAL} | Expected the parameter 'required' to be of type bool, found {type(required)}.""")
#     #
#     #     if min_length is not None:
#     #         self.__validate(isinstance(min_length, int), f"""{E_INTERNAL} | Expected the parameter 'min_length' to be of type int, found {type(min_length)}.""")
#     #
#     #     if min_value is not None:
#     #         self.__validate(isinstance(min_value, numbers.Number), f"""{E_INTERNAL} | Expected the parameter 'min_value' to be of type numbers.Number, found {type(min_value)}.""")
#
#     # def required(self) -> ValidatorProxy:
#     #
#     #     self.__validate(self.value is not None, f"""{E_NOT_NONE} | The parameter '{self.parameter_name}' must be specified.""")
#     #
#     #     return ValidatorProxy(**{self.parameter_name: self.value})
#
#
#     # def any_value(self, *, parameter_type: Type[ReturnType], required: bool, min_length: Optional[int] = None, min_value: Optional[numbers.Number] = None, other_value: Optional[Any] = None) -> ReturnType:
#     #
#     #     # Logically its required if we have a min_length
#     #     required = True if min_length is not None else required
#     #
#     #     self.__validate_my_parameters(required=required, min_length=min_length, min_value=min_value, parameter_type=parameter_type)
#     #     self.__validate_required(required=required, parameter_name=self.parameter_name, parameter_type=parameter_type, value=self.value)
#     #     self.__validate_min_length(min_length=min_length, parameter_name=self.parameter_name, value=self.value)
#     #     self.__validate_min_value(min_value=min_value, parameter_name=self.parameter_name, value=self.value)
#
#         return self.value
#
#     def any_value_required(self, *, parameter_type: Type[ReturnType], min_length: int = None, min_value: numbers.Number = None) -> ReturnType:
#
#         return self.any_value(parameter_type=parameter_type,
#                               required=True,
#                               min_length=min_length,
#                               min_value=min_value)
#
#     def str_value(self, *, required: bool, min_length: int = None) -> str:
#         return self.any_value(parameter_type=str, min_length=min_length, required=required, min_value=None)
#
#     def str_value_required(self, *, min_length: int = None) -> str:
#         return self.any_value(parameter_type=str, min_length=min_length, required=True, min_value=None)
#
#     def int_value(self, *, required: bool, min_value=None) -> int:
#         return self.any_value(parameter_type=int, min_length=None, required=required, min_value=min_value)
#
#     def int_value_required(self, *, min_value=None) -> int:
#         return self.any_value(parameter_type=int, min_length=None, required=True, min_value=min_value)
#
#     def float_value(self, *, required: bool, min_value=None) -> float:
#         return self.any_value(parameter_type=float, min_length=None, required=required, min_value=min_value)
#
#     def float_value_required(self, *, min_value=None) -> float:
#         return self.any_value(parameter_type=float, min_length=None, required=True, min_value=min_value)
#
#     def bool_value(self, *, required: bool) -> bool:
#         return self.any_value(parameter_type=bool, min_length=None, required=required, min_value=None)
#
#     def bool_value_required(self) -> bool:
#         return self.any_value(parameter_type=bool, min_length=None, required=True, min_value=None)
#
#     def list_value(self, *, required: bool, min_length: int = None) -> list:
#         return self.any_value(parameter_type=list, min_length=min_length, required=required, min_value=None)
#
#     def list_value_required(self, *, min_length: int = None) -> list:
#         return self.any_value(parameter_type=list, min_length=min_length, required=True, min_value=None)
#
#     def dict_value(self, *, required: bool, min_length: int = None) -> dict:
#         return self.any_value(parameter_type=dict, min_length=min_length, required=required, min_value=None)
#
#     def dict_value_required(self, *, min_length: int = None) -> dict:
#         return self.any_value(parameter_type=dict, min_length=min_length, required=True, min_value=None)
#
#     def set_value(self, *, required: bool, min_length: int = None) -> set:
#         return self.any_value(parameter_type=set, min_length=min_length, required=required, min_value=None)
#
#     def set_value_required(self, *, min_length: int = None) -> set:
#         return self.any_value(parameter_type=set, min_length=min_length, required=True, min_value=None)
#
#     def iterable_value(self, *, required: bool, min_length: int = None) -> Iterable:
#         return self.any_value(parameter_type=Iterable, min_length=min_length, required=required, min_value=None)
#
#     def iterable_value_required(self, *, min_length: int = None) -> Iterable:
#         return self.any_value(parameter_type=Iterable, min_length=min_length, required=True, min_value=None)
#
#     def list_of_type(self, *, element_type: Type[ReturnType], required: bool, auto_create: bool, **kwargs) -> List[ReturnType]:
#         return self.__element_types(parameter_type=List, element_type=element_type, required=required, auto_create=auto_create, **kwargs)
#
#     def list_of_type_required(self, *, element_type: Type[ReturnType], auto_create: bool, **kwargs) -> List[ReturnType]:
#         return self.__element_types(parameter_type=List, element_type=element_type, required=True, auto_create=auto_create, **kwargs)
#
#     def list_of_str(self, *, required: bool, auto_create: bool, **kwargs) -> List[str]:
#         return self.__element_types(parameter_type=List, element_type=str, required=required, auto_create=auto_create, **kwargs)
#
#     def list_of_str_required(self, *, auto_create: bool, **kwargs) -> List[str]:
#         return self.__element_types(parameter_type=List, element_type=str, required=True, auto_create=auto_create, **kwargs)
#
#     def list_of_ints(self, *, required: bool, auto_create: bool, **kwargs) -> List[int]:
#         return self.__element_types(parameter_type=List, element_type=int, required=required, auto_create=auto_create, **kwargs)
#
#     def list_of_ints_required(self, *, auto_create: bool, **kwargs) -> List[int]:
#         return self.__element_types(parameter_type=List, element_type=int, required=True, auto_create=auto_create, **kwargs)
#
#     def list_of_floats(self, *, required: bool, auto_create: bool, **kwargs) -> List[float]:
#         return self.__element_types(parameter_type=List, element_type=float, required=required, auto_create=auto_create, **kwargs)
#
#     def list_of_floats_required(self, *, auto_create: bool, **kwargs) -> List[float]:
#         return self.__element_types(parameter_type=List, element_type=float, required=True, auto_create=auto_create, **kwargs)
#
#     def list_of_bools(self, *, required: bool, auto_create: bool, **kwargs) -> List[bool]:
#         return self.__element_types(parameter_type=List, element_type=bool, required=required, auto_create=auto_create, **kwargs)
#
#     def list_of_bools_required(self, *, auto_create: bool, **kwargs) -> List[bool]:
#         return self.__element_types(parameter_type=List, element_type=bool, required=True, auto_create=auto_create, **kwargs)
#
#     def dict_of_key_type(self, *, element_type: Type[ReturnType], required: bool, auto_create: bool, **kwargs) -> Dict[ReturnType, Any]:
#         return self.__element_types(parameter_type=Dict, element_type=element_type, required=required, auto_create=auto_create, **kwargs)
#
#     def dict_of_key_type_required(self, *, element_type: Type[ReturnType], auto_create: bool, **kwargs) -> Dict[ReturnType, Any]:
#         return self.__element_types(parameter_type=Dict, element_type=element_type, required=True, auto_create=auto_create, **kwargs)
#
#     def dict_of_key_str(self, *, required: bool, auto_create: bool, **kwargs) -> Dict[str, Any]:
#         return self.__element_types(parameter_type=Dict, element_type=str, required=required, auto_create=auto_create, **kwargs)
#
#     def dict_of_key_str_required(self, *, auto_create: bool, **kwargs) -> Dict[str, Any]:
#         return self.__element_types(parameter_type=Dict, element_type=str, required=True, auto_create=auto_create, **kwargs)
#
#     def dict_of_key_int(self, *, required: bool, auto_create: bool, **kwargs) -> Dict[int, Any]:
#         return self.__element_types(parameter_type=Dict, element_type=int, required=required, auto_create=auto_create, **kwargs)
#
#     def dict_of_key_int_required(self, *, auto_create: bool, **kwargs) -> Dict[int, Any]:
#         return self.__element_types(parameter_type=Dict, element_type=int, required=True, auto_create=auto_create, **kwargs)
#
#     def dict_of_key_float(self, *, required: bool, auto_create: bool, **kwargs) -> Dict[float, Any]:
#         return self.__element_types(parameter_type=Dict, element_type=float, required=required, auto_create=auto_create, **kwargs)
#
#     def dict_of_key_float_required(self, *, auto_create: bool, **kwargs) -> Dict[float, Any]:
#         return self.__element_types(parameter_type=Dict, element_type=float, required=True, auto_create=auto_create, **kwargs)
#
#     def dict_of_key_bool(self, *, required: bool, auto_create: bool, **kwargs) -> Dict[bool, Any]:
#         return self.__element_types(parameter_type=Dict, element_type=bool, required=required, auto_create=auto_create, **kwargs)
#
#     def dict_of_key_bool_required(self, *, auto_create: bool, **kwargs) -> Dict[bool, Any]:
#         return self.__element_types(parameter_type=Dict, element_type=bool, required=True, auto_create=auto_create, **kwargs)
#
#     def set_of_type(self, *, element_type: Type[ReturnType], required: bool, auto_create: bool, **kwargs) -> Set[ReturnType]:
#         return self.__element_types(parameter_type=Set, element_type=element_type, required=required, auto_create=auto_create, **kwargs)
#
#     def set_of_type_required(self, *, element_type: Type[ReturnType], auto_create: bool, **kwargs) -> Set[ReturnType]:
#         return self.__element_types(parameter_type=Set, element_type=element_type, required=True, auto_create=auto_create, **kwargs)
#
#     def set_of_str(self, *, required: bool, auto_create: bool, **kwargs) -> Set[str]:
#         return self.__element_types(parameter_type=Set, element_type=str, required=required, auto_create=auto_create, **kwargs)
#
#     def set_of_str_required(self, *, auto_create: bool, **kwargs) -> Set[str]:
#         return self.__element_types(parameter_type=Set, element_type=str, required=True, auto_create=auto_create, **kwargs)
#
#     def set_of_ints(self, *, required: bool, auto_create: bool, **kwargs) -> Set[int]:
#         return self.__element_types(parameter_type=Set, element_type=int, required=required, auto_create=auto_create, **kwargs)
#
#     def set_of_ints_required(self, *, auto_create: bool, **kwargs) -> Set[int]:
#         return self.__element_types(parameter_type=Set, element_type=int, required=True, auto_create=auto_create, **kwargs)
#
#     def set_of_floats(self, *, required: bool, auto_create: bool, **kwargs) -> Set[int]:
#         return self.__element_types(parameter_type=Set, element_type=float, required=required, auto_create=auto_create, **kwargs)
#
#     def set_of_floats_required(self, *, auto_create: bool, **kwargs) -> Set[int]:
#         return self.__element_types(parameter_type=Set, element_type=float, required=True, auto_create=auto_create, **kwargs)
#
#     def set_of_bools(self, *, required: bool, auto_create: bool, **kwargs) -> Set[bool]:
#         return self.__element_types(parameter_type=Set, element_type=bool, required=required, auto_create=auto_create, **kwargs)
#
#     def set_of_bools_required(self, *, auto_create: bool, **kwargs) -> Set[bool]:
#         return self.__element_types(parameter_type=Set, element_type=bool, required=True, auto_create=auto_create, **kwargs)
