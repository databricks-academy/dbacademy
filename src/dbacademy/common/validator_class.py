__all__ = ["Validator", "ValidationError"]

import numbers, inspect
from collections.abc import Collection
from typing import Type, Any, List, Dict, Set, TypeVar, Sized, Optional, Iterable, Union, Tuple
from abc import ABC, abstractmethod
import typing

E_NOT_NONE = "Error-Not-None"
E_ONE_OF = "Error-One-Of"
E_TYPE = "Error-Type"
ELEM_TYPE = "Element-Type"
E_MIN_L = "Error-Min-Len"
E_MIN_V = "Error-Min-Value"
E_MAX_V = "Error-Max-Value"
E_INTERNAL = "Error-Internal"

KeyType = TypeVar("KeyType")

CollectionType = TypeVar("CollectionType", bound=Union[Collection, Iterable])
ElementType = TypeVar("ElementType")
ParameterType = TypeVar("ParameterType")


class ValidationError(Exception):

    def __init__(self, message: str):
        super().__init__(message)
        self.__message = message

    @property
    def message(self) -> str:
        return self.__message


class AbstractValidator(ABC):

    def __init__(self, parameter_name_override: str = None, **kwargs):
        self.__parameter_name: str = parameter_name_override or list(kwargs)[0]
        self.__parameter_value: Any = kwargs.get(self.__parameter_name)

    @property
    def parameter_value(self) -> Any:
        return self.__parameter_value

    @parameter_value.setter
    def parameter_value(self, value):
        self.__parameter_value = value

    @property
    def parameter_name(self) -> str:
        return self.__parameter_name

    @abstractmethod
    def as_one_of(self, parameter_type: Type[ParameterType], value: Any, *or_values: Any) -> ParameterType:
        pass

    @abstractmethod
    def as_type(self, parameter_type: Type[ParameterType], *or_type: Type) -> ParameterType:
        pass

    @abstractmethod
    def enum(self, enum_type: Type[ParameterType], auto_convert: bool = False) -> ParameterType:
        pass

    @abstractmethod
    def number(self, min_value: Optional[numbers.Number] = None, max_value: Optional[numbers.Number] = None) -> numbers.Number:
        pass

    @abstractmethod
    def int(self, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
        pass

    @abstractmethod
    def float(self, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:
        pass

    @abstractmethod
    def bool(self) -> bool:
        pass

    @abstractmethod
    def str(self, *, min_length: int = 0) -> str:
        pass

    @abstractmethod
    def iterable(self, element_type: Type[ElementType]) -> Iterable[ElementType]:
        pass

    @abstractmethod
    def list(self, element_type: Type[ElementType], *, min_length: int = 0, auto_create: bool = False) -> List[ElementType]:
        pass

    @abstractmethod
    def set(self, element_type: Type[ElementType], *, min_length: int = 0, auto_create: bool = False) -> Set[ElementType]:
        pass

    @abstractmethod
    def dict(self, key_type: Type[KeyType], element_type: Type[ParameterType] = Any, *, min_length: int = 0, auto_create: bool = False) -> Dict[KeyType, ParameterType]:
        pass


class Validator(AbstractValidator):

    def __init__(self, parameter_name_override: str = None, **kwargs):
        """
        Creates an instance of a validator relying on kwargs to specify both the parameter_name, and it's value.
        In cases where the parameter name needs to be specified dynamically (e.g. when loaded from a dictionary), the parameter name can be specified directly via parameter_name_override.
        Usage:

        def some_function(some_argument: int) -> None
            from dbacademy.common import validate
            validate(some_argument=some_argument).<snip-some-validation-method>

        Once an instance of the Validator is created, any number of the class's validation methods can be used.

        This pattern uses the **kwargs to dynamically declare the name of the parameter, and it's corresponding value. Because the parameter name is effectively a loose string (key of **kwargs),
        it cannot be validated in any real way with the current implementation. Future versions may interrogate the function's parameters to provide additional guarantees, but even then, they
        may only be runtime validations. Because the parameter name is only used in generation of the error method, the risk to type safety is minimal while providing one of the cleanest implementations.

        :param parameter_name_override: The name of the parameter to be used when the dynamically generated parameter name is not
        correct such as is the case when validating parameters loaded from a dictionary and thus not an actual function parameter.
        :param kwargs: The one and only one parameter to be validated by this class expressed as a dictionary.
        """

        message = f"{E_INTERNAL} | {self.__class__.__name__}.{inspect.stack()[0].function}(..) expects one and only one parameter, found {len(kwargs)}."
        self.__validate(passed=len(kwargs) == 1, message=message)

        super().__init__(parameter_name_override=parameter_name_override, **kwargs)

    @property
    def required(self) -> AbstractValidator:
        message = f"""{E_NOT_NONE} | The parameter '{self.parameter_name}' must be specified."""
        self.__validate(passed=self.parameter_value is not None, message=message)
        return self

    def as_one_of(self, parameter_type: Type[ParameterType], value: Any, *or_values: Any) -> ParameterType:
        self.__validate_value_type(parameter_type)

        expected_values = list()

        if isinstance(value, List):
            # First arg is a list of values
            expected_values.extend(value)
        elif isinstance(value, Tuple):
            # First arg is a list of values
            expected_values.extend(value)
        elif type(value) is type(typing.Literal[0]):
            expected_values.extend(typing.get_args(value))
        else:
            # First arg is a single value
            expected_values.append(value)

        # Add all of our "other" values
        expected_values.extend(or_values)

        message = f"""{E_ONE_OF} | The parameter '{self.parameter_name}' must be one of the expected values {expected_values}, found "{self.parameter_value}"."""
        self.__validate(passed=self.parameter_value in expected_values, message=message)

        return self.parameter_value

    def as_type(self, parameter_type: Type[ParameterType], *or_type: Type) -> ParameterType:
        self.__validate_value_type(parameter_type, *or_type)
        return self.parameter_value

    def enum(self, enum_type: Type[ParameterType], auto_convert: bool = False) -> ParameterType:
        self.__validate_data_type("enum_type", enum_type)

        message = f"""{E_INTERNAL} | Expected {self.__class__.__name__}.{inspect.stack()[0].function}(..)'s parameter 'auto_convert' to be of type bool, found {type(auto_convert)}."""
        self.__validate(passed=isinstance(auto_convert, bool), message=message)

        if auto_convert and not isinstance(self.parameter_value, enum_type):
            for value in enum_type:
                if value == self.parameter_value:
                    self.parameter_value = value
                elif isinstance(self.parameter_value, str) and self.parameter_value == value.value:
                    self.parameter_value = value
                elif isinstance(self.parameter_value, str) and self.parameter_value.lower() == value.value:
                    self.parameter_value = value
                elif isinstance(self.parameter_value, str) and self.parameter_value.upper() == value.value:
                    self.parameter_value = value

            message = f"""{E_TYPE} | Cannot convert the value "{self.parameter_value}" of type {type(self.parameter_value)} to {enum_type}."""
            self.__validate(passed=isinstance(self.parameter_value, enum_type), message=message)

        self.__validate_value_type(enum_type)
        return self.parameter_value

    def number(self, min_value: Optional[numbers.Number] = None, max_value: Optional[numbers.Number] = None) -> numbers.Number:
        self.__validate_value_type(numbers.Number)
        self.__validate_min_value(min_value=min_value)
        self.__validate_max_value(max_value=max_value)
        return self.parameter_value

    def int(self, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
        self.__validate_value_type(int)
        self.__validate_min_value(min_value=min_value)
        self.__validate_max_value(max_value=max_value)
        return self.parameter_value

    def float(self, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:

        if isinstance(self.parameter_value, int):
            self.parameter_value = float(self.parameter_value)

        self.__validate_value_type(float)
        self.__validate_min_value(min_value=min_value)
        self.__validate_max_value(max_value=max_value)
        return self.parameter_value

    def bool(self) -> bool:
        self.__validate_value_type(bool)
        return self.parameter_value

    def str(self, *, min_length: int = 0) -> str:
        return self.__validate_collection(parameter_type=str, key_type=Any, element_type=str, min_length=min_length)

    def iterable(self, element_type: Type[ElementType]) -> Iterable[ElementType]:
        return self.__validate_collection(parameter_type=Iterable, key_type=Any, element_type=element_type, min_length=0)

    def list(self, element_type: Type[ElementType], *, min_length: int = 0, auto_create: bool = False) -> List[ElementType]:
        message = f"""{E_INTERNAL} | Expected {self.__class__.__name__}.{inspect.stack()[0].function}(..)'s parameter 'auto_create' to be of type bool, found {type(auto_create)}."""
        self.__validate(passed=isinstance(auto_create, bool), message=message)

        self.parameter_value = self.parameter_value or list() if auto_create else self.parameter_value
        return self.__validate_collection(parameter_type=list, key_type=Any, element_type=element_type, min_length=min_length)

    def set(self, element_type: Type[ElementType], *, min_length: int = 0, auto_create: bool = False) -> Set[ElementType]:
        message = f"""{E_INTERNAL} | Expected {self.__class__.__name__}.{inspect.stack()[0].function}(..)'s parameter 'auto_create' to be of type bool, found {type(auto_create)}."""
        self.__validate(passed=isinstance(auto_create, bool), message=message)

        self.parameter_value = self.parameter_value or set() if auto_create else self.parameter_value
        return self.__validate_collection(parameter_type=set, key_type=Any, element_type=element_type, min_length=min_length)

    def dict(self, key_type: Type[KeyType], element_type: Type[ParameterType] = Any, *, min_length: int = 0, auto_create: bool = False) -> Dict[KeyType, ParameterType]:
        message = f"""{E_INTERNAL} | Expected {self.__class__.__name__}.{inspect.stack()[0].function}(..)'s parameter 'auto_create' to be of type bool, found {type(auto_create)}."""
        self.__validate(passed=isinstance(auto_create, bool), message=message)

        self.parameter_value = self.parameter_value or dict() if auto_create else self.parameter_value
        return self.__validate_collection(parameter_type=dict, key_type=key_type, element_type=element_type, min_length=min_length)

    def __validate_data_type(self, name: str, data_type: Type) -> None:
        import typing

        message = f"""{E_INTERNAL} | Expected {self.__class__.__name__}.{inspect.stack()[0].function}(..)'s parameter '{name}' to be specified."""
        self.__validate(passed=data_type is not None, message=message)

        # noinspection PyUnresolvedReferences,PyProtectedMember
        message = f"""{E_INTERNAL} | Expected {self.__class__.__name__}.{inspect.stack()[0].function}(..)'s parameter '{name}' to be a python "type", found {type(data_type)}."""
        self.__validate(passed=isinstance(data_type, type) or str(data_type).startswith("typing."), message=message)

    def __validate_min_value(self, *, min_value: Optional[numbers.Number]) -> None:

        if min_value is not None:
            # We need to verify that min_value is of type numbers.Number
            message = f"""{E_INTERNAL} | Expected {self.__class__.__name__}.{inspect.stack()[0].function}(..)'s parameter 'min_value' to be of type numbers.Number, found {type(min_value)}."""
            self.__validate(passed=isinstance(min_value, numbers.Number), message=message)

            # We cannot test the min value if the value is not of type numbers.Number
            message = f"""{E_TYPE} | Expected the parameter '{self.parameter_name}' to be of type numbers.Number, found {type(self.parameter_value)}."""
            self.__validate(passed=isinstance(self.parameter_value, numbers.Number), message=message)

            message = f"""{E_MIN_V} | The parameter '{self.parameter_name}' must have a minimum value of '{min_value}', found '{self.parameter_value}'."""
            self.__validate(passed=self.parameter_value >= min_value, message=message)

    def __validate_max_value(self, *, max_value: Optional[numbers.Number]) -> None:

        if max_value is not None:
            # INTERNAL, We need to verify that max_value is of type numbers.Number
            message = f"""{E_INTERNAL} | Expected {self.__class__.__name__}.{inspect.stack()[0].function}(..)'s parameter 'max_value' to be of type numbers.Number, found {type(max_value)}."""
            self.__validate(passed=isinstance(max_value, numbers.Number), message=message)

            # We cannot test the max value if the value is not of type numbers.Number
            message = f"""{E_TYPE} | Expected the parameter '{self.parameter_name}' to be of type numbers.Number, found {type(self.parameter_value)}."""
            self.__validate(passed=isinstance(self.parameter_value, numbers.Number), message=message)

            message = f"""{E_MAX_V} | The parameter '{self.parameter_name}' must have a maximum value of '{max_value}', found '{self.parameter_value}'."""
            self.__validate(passed=self.parameter_value <= max_value, message=message)

    def __validate_value_type(self, _parameter_type: Type, *or_type: Type):
        self.__validate_data_type("parameter_type", _parameter_type)
        for i, t in enumerate(or_type):
            self.__validate_data_type(f"or_types[{i}]", or_type[i])

        all_types = [_parameter_type]
        all_types.extend(list(or_type))

        if self.parameter_value is not None:
            passed = False
            for t in all_types:
                is_of_type = isinstance(self.parameter_value, t)
                passed = passed or is_of_type

            if not passed:
                last_type = all_types.pop()
                expected_types = ", ".join([str(t) for t in all_types])
                if len(expected_types) > 0:
                    expected_types += " or "
                expected_types += str(last_type)

                message = f"""{E_TYPE} | Expected the parameter '{self.parameter_name}' to be of type {expected_types}, found {type(self.parameter_value)}."""
                self.__validate(passed=passed, message=message)

    def __validate_collection(self, *, parameter_type: Type[CollectionType], key_type: Type[KeyType], element_type: Type[ElementType], min_length: int = 0) -> CollectionType:
        self.__validate_data_type("parameter_type", parameter_type)
        self.__validate_data_type("key_type", key_type)
        self.__validate_data_type("element_type", element_type)

        self.__validate_value_type(parameter_type)

        self.__validate_collection_of_type(parameter_type=parameter_type,
                                           key_type=key_type,
                                           element_type=element_type)

        self.__validate_min_length(min_length=min_length)

        return self.parameter_value

    def __validate_collection_of_type(self, *, parameter_type: Type[CollectionType], key_type: Type[KeyType], element_type: Type[ElementType]) -> None:
        self.__validate_data_type("parameter_type", parameter_type)
        self.__validate_data_type("element_type", element_type)

        if isinstance(self.parameter_value, (List, Set)):
            for i, actual_value in enumerate(self.parameter_value):
                message = f"""{ELEM_TYPE} | Expected element {i} of '{self.parameter_name}' to be of type {element_type}, found "{actual_value}" of type {type(actual_value)}."""
                self.__validate(passed=isinstance(actual_value, element_type), message=message)

        elif isinstance(self.parameter_value, Dict):
            for key, value in self.parameter_value.items():
                message = f"""{ELEM_TYPE} | Expected the key "{key}" of '{self.parameter_name}' to be of type {key_type}, found the type {type(key)}."""
                self.__validate(passed=isinstance(key, key_type), message=message)

                if element_type is not Any:
                    message = f"""{ELEM_TYPE} | Expected the entry for key "{key}" of '{self.parameter_name}' to be of type {element_type}, found the type {type(value)}."""
                    self.__validate(passed=isinstance(value, element_type), message=message)

        elif isinstance(self.parameter_value, str):
            pass  # We don't need to test these.

        elif self.parameter_value is not None:
            raise Exception(f"Cannot validate collections of type {parameter_type}.")

    def __validate_min_length(self, *, min_length: int = 0) -> None:
        # We need to verify that min_length is of tye int
        message = f"""{E_INTERNAL} | Expected {self.__class__.__name__}.{inspect.stack()[0].function}(..)'s parameter 'min_length' to be specified."""
        self.__validate(passed=min_length is not None, message=message)

        message = f"""{E_INTERNAL} | Expected {self.__class__.__name__}.{inspect.stack()[0].function}(..)'s parameter 'min_length' to be of type int, found {type(min_length)}."""
        self.__validate(passed=isinstance(min_length, int), message=message)

        if min_length > 0:
            # We cannot test the length if the value is not of type Sized.
            message = f"""{E_TYPE} |  Expected the parameter '{self.parameter_name}' to be of type Sized, found {type(self.parameter_value)}."""
            self.__validate(passed=isinstance(self.parameter_value, Sized), message=message)

            actual_length = len(self.parameter_value)
            message = f"""{E_MIN_L} | The parameter '{self.parameter_name}' must have a minimum length of {min_length}, found {actual_length}."""
            self.__validate(passed=actual_length >= min_length, message=message)

    @classmethod
    def __validate(cls, *, passed: bool, message: str) -> None:
        if not passed:
            raise ValidationError(message)
