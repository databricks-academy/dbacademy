__all__ = ["ValidateTests"]

import numbers
import unittest
from typing import Dict, List, Set, Any, Tuple
from dbacademy.common import validate, ValidationError

EXPECTED_ASSERTION_ERROR = "Expected AssertionError"


class ValidateTests(unittest.TestCase):

    def test_no_value(self):
        try:
            validate()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Validator.__init__(..) expects one and only one parameter, found 0.""", e.message)

    def test_optional(self):
        value = validate(value=1).optional.int()
        self.assertEqual(1, value)

        value = validate(value=None).optional.int()
        self.assertEqual(None, value)

    def test_ars(self):
        value = validate(value=1).args(required=True).int()
        self.assertEqual(1, value)

        value = validate(value=None).args(required=False).int()
        self.assertIsNone(value)

        value = validate(value=1).args().int()
        self.assertEqual(1, value)

        try:
            value = validate(value=None).args(parameter_name="whatever", required=True).int()
            self.assertEqual(1, value)
        except ValidationError as e:
            self.assertEqual("""Error-Not-None | The parameter 'whatever' must be specified.""", e.message)

    def test_as_type(self):
        value = validate(value=1).as_type(int)
        self.assertEqual(1, value)

        value = validate(value=[1, 2, 3]).as_type(List)
        self.assertEqual([1, 2, 3], value)

        value = validate(value={1, 2, 3}).as_type(Set)
        self.assertEqual({1, 2, 3}, value)

        value = validate(value={1, 2, 3}).set(int, min_length=0)
        self.assertEqual({1, 2, 3}, value)

        value = validate(value={"color": "red", "size": "large"}).as_type(Dict)
        self.assertEqual({"color": "red", "size": "large"}, value)

        try:
            validate(value=1).as_type(None)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected Validator.__validate_data_type(..)'s parameter 'parameter_type' to be specified.""", e.message)

        try:
            # noinspection PyTypeChecker
            validate(value=1).as_type("moo")
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected Validator.__validate_data_type(..)'s parameter 'parameter_type' to be a python "type", found <class 'str'>.""", e.message)

    def test_parameter_min_length_type(self):

        value = validate(value="moo").str(min_length=0)
        self.assertEqual("moo", value)

        try:
            # noinspection PyTypeChecker
            validate(value="moo").str(min_length=None)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected Validator.__validate_min_length(..)'s parameter 'min_length' to be specified.""", e.message)

        try:
            # noinspection PyTypeChecker
            validate(value="moo").str(min_length="moo")
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected Validator.__validate_min_length(..)'s parameter 'min_length' to be of type int, found <class 'str'>.""", e.message)

    def test_parameter_min_value_type(self):

        value = validate(value=1).int(min_value=1)
        self.assertEqual(1, value)

        value = validate(value=1).int(min_value=None)
        self.assertEqual(1, value)

        try:
            # noinspection PyTypeChecker
            validate(value=1).int(min_value="moo")
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected Validator.__validate_min_value(..)'s parameter 'min_value' to be of type numbers.Number, found <class 'str'>.""", e.message)

    def test_parameter_max_value_type(self):
        value = validate(value=5).int(max_value=5)
        self.assertEqual(5, value)

        value = validate(value=1).int(max_value=None)
        self.assertEqual(1, value)

        try:
            # noinspection PyTypeChecker
            validate(value=1).int(max_value="moo")
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected Validator.__validate_max_value(..)'s parameter 'max_value' to be of type numbers.Number, found <class 'str'>.""", e.message)

    def test_wrong_parameter_type(self):

        value = validate(value=1).int()
        self.assertEqual(1, value)

        value = validate(value=1).as_type(numbers.Number)
        self.assertEqual(1, value)

        try:
            validate(value=1).str()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Type | Expected the parameter 'value' to be of type <class 'str'>, found <class 'int'>.""", e.message)

        try:
            validate(value=1).dict(str)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Type | Expected the parameter 'value' to be of type <class 'dict'>, found <class 'int'>.""", e.message)

    def test_str_value(self):
        value = validate(value="asdf").str(min_length=0)
        self.assertEqual("asdf", value)

        value = validate(value="asdf").str(min_length=1)
        self.assertEqual("asdf", value)

        value = validate(value="asdf").str(min_length=2)
        self.assertEqual("asdf", value)

        value = validate(value="asdf").str(min_length=3)
        self.assertEqual("asdf", value)

        value = validate(value="asdf").str(min_length=4)
        self.assertEqual("asdf", value)

        try:
            validate(value="asdf").str(min_length=5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Len | The parameter 'value' must have a minimum length of 5, found 4.""", e.message)

    def test_tuple_value(self):
        value: Tuple = validate(value=("asdf",)).tuple(str)
        self.assertEqual(("asdf",), value)

        value: Tuple = validate(value=("asdf", 1, 2.0, False)).tuple(str, int, float, bool)
        self.assertEqual(("asdf", 1, 2.0, False), value)

        try:
            validate(value=["asdf", 1, 2.0, False]).tuple(str, int, float, bool)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Type | Expected the parameter 'value' to be of type <class 'tuple'>, found <class 'list'>.""", e.message)

        try:
            validate(value=("asdf", 1, 2.0, False)).tuple("str", int, float, bool)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected Validator.__validate_data_type(..)'s parameter 'element_types[0]' to be a python "type", found <class 'str'>.""", e.message)

        try:
            validate(value=("asdf", 1, 2.0, False)).tuple(str, int, False, bool)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected Validator.__validate_data_type(..)'s parameter 'element_types[2]' to be a python "type", found <class 'bool'>.""", e.message)

        try:
            validate(value=("asdf", 1, 2.0, False)).tuple(str, str, float, bool)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            # self.assertEqual("""Element-Type | Expected element 2 of 'value' to be of type <class 'str'>, found "1" of type <class 'int'>.""", e.message)
            self.assertEqual("""Error-Type | Expected the parameter 'value[1]' to be of type <class 'str'>, found <class 'int'>.""", e.message)

    def test_int_value(self):
        value = validate(value=None).int()
        self.assertIsNone(value)

        value = validate(value=1).int()
        self.assertEqual(1, value)

        value = validate(value=1).required.int()
        self.assertEqual(1, value)

        value = validate(value=4).int(min_value=2)
        self.assertEqual(4, value)

        value = validate(value=3).int(min_value=2)
        self.assertEqual(3, value)

        value = validate(value=2).int(min_value=2)
        self.assertEqual(2, value)

        value = validate(value=4).int(max_value=4)
        self.assertEqual(4, value)

        value = validate(value=3).int(max_value=4)
        self.assertEqual(3, value)

        value = validate(value=2).int(max_value=4)
        self.assertEqual(2, value)

        value = validate(value=4).int(min_value=2, max_value=4)
        self.assertEqual(4, value)

        value = validate(value=3).int(min_value=2, max_value=4)
        self.assertEqual(3, value)

        value = validate(value=2).int(min_value=2, max_value=4)
        self.assertEqual(2, value)

        try:
            validate(value=1).int(min_value=2)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Value | The parameter 'value' must have a minimum value of '2', found '1'.""", e.message)

        try:
            validate(value=5).int(max_value=4)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Max-Value | The parameter 'value' must have a maximum value of '4', found '5'.""", e.message)

        try:
            validate(value=1).int(min_value=2, max_value=4)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Value | The parameter 'value' must have a minimum value of '2', found '1'.""", e.message)

        try:
            validate(value=5).int(min_value=2, max_value=4)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Max-Value | The parameter 'value' must have a maximum value of '4', found '5'.""", e.message)

        try:
            validate(value=None).required.int()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Not-None | The parameter 'value' must be specified.""", e.message)

        try:
            validate(value=1.0).int()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Type | Expected the parameter 'value' to be of type <class 'int'>, found <class 'float'>.""", e.message)

    def test_float_value(self):
        value = validate(value=None).float()
        self.assertIsNone(value)

        value = validate(value=1.0).float()
        self.assertEqual(1.0, value)

        value = validate(value=1.0).required.float()
        self.assertEqual(1.0, value)

        value = validate(value=4.0).float(min_value=2)
        self.assertEqual(4.0, value)

        value = validate(value=4.0).float(min_value=2.5)
        self.assertEqual(4.0, value)

        value = validate(value=3.0).float(min_value=2)
        self.assertEqual(3.0, value)

        value = validate(value=3.0).float(min_value=2.5)
        self.assertEqual(3.0, value)

        # Start with an int, but require a float
        value = validate(value=1).float()
        self.assertEqual(1, value)

        try:
            value = validate(value=2.0).float(min_value=2.5)
            self.assertEqual(2.0, value)

            validate(value=2.0).float(min_value=2)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Value | The parameter 'value' must have a minimum value of '2.5', found '2.0'.""", e.message)

        try:
            validate(value=2.0).float(min_value=3)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Value | The parameter 'value' must have a minimum value of '3', found '2.0'.""", e.message)

        try:
            validate(value=None).required.float()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Not-None | The parameter 'value' must be specified.""", e.message)

    def test_number_value(self):
        value = validate(value=None).number()
        self.assertIsNone(value)

        value = validate(value=1).number()
        self.assertEqual(1, value)

        value = validate(value=1.0).number()
        self.assertEqual(1.0, value)

        value = validate(value=1).required.number()
        self.assertEqual(1, value)

        value = validate(value=1.0).required.number()
        self.assertEqual(1.0, value)

        value = validate(value=4).number(min_value=2)
        self.assertEqual(4, value)

        value = validate(value=4.0).number(min_value=2)
        self.assertEqual(4.0, value)

        value = validate(value=4).number(min_value=2.5)
        self.assertEqual(4, value)

        value = validate(value=4.0).number(min_value=2.5)
        self.assertEqual(4.0, value)

        value = validate(value=3).number(min_value=2)
        self.assertEqual(3, value)

        value = validate(value=3.0).number(min_value=2.5)
        self.assertEqual(3.0, value)

        value = validate(value=3).number(min_value=2)
        self.assertEqual(3, value)

        value = validate(value=3.0).number(min_value=2.5)
        self.assertEqual(3.0, value)

        try:
            value = validate(value=2.0).number(min_value=2)
            self.assertEqual(2.0, value)

            validate(value=2.0).number(min_value=2.5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Value | The parameter 'value' must have a minimum value of '2.5', found '2.0'.""", e.message)

        try:
            validate(value=2.0).number(min_value=3)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Value | The parameter 'value' must have a minimum value of '3', found '2.0'.""", e.message)

        try:
            validate(value=None).required.number()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Not-None | The parameter 'value' must be specified.""", e.message)

    def test_bool_value(self):
        value = validate(value=True).bool()
        self.assertEqual(True, value)

        value = validate(value=False).bool()
        self.assertEqual(False, value)

        value = validate(value=None).bool()
        self.assertIsNone(value)

        try:
            validate(value=None).required.bool()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Not-None | The parameter 'value' must be specified.""", e.message)

    def test_enum_value(self):
        from dbacademy.common import Cloud

        value = validate(value=Cloud.AWS).enum(Cloud)
        self.assertEqual(Cloud.AWS, value)

        value = validate(value=Cloud.MSA).enum(Cloud)
        self.assertEqual(Cloud.MSA, value)

        value = validate(value=Cloud.GCP).enum(Cloud)
        self.assertEqual(Cloud.GCP, value)

        value = validate(value=Cloud.UNKNOWN).enum(Cloud)
        self.assertEqual(Cloud.UNKNOWN, value)

        value = validate(value="AWS").enum(Cloud, auto_convert=True)
        self.assertEqual(Cloud.AWS, value)

        value = validate(value="aws").enum(Cloud, auto_convert=True)
        self.assertEqual(Cloud.AWS, value)

        try:
            validate(value="asdf").enum(Cloud, auto_convert=True)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Type | Cannot convert the value "asdf" of type <class 'str'> to <enum 'Cloud'>.""", e.message)

        try:
            validate(value="aws").enum(Cloud, auto_convert=False)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Type | Expected the parameter 'value' to be of type <enum 'Cloud'>, found <class 'str'>.""", e.message)

    def test_one_of(self):
        import typing

        value = validate(value=1).required.as_one_of(int, 1, 2, 3, 4)
        self.assertEqual(1, value)

        value = validate(value=2).required.as_one_of(int, [1, 2, 3, 4])
        self.assertEqual(2, value)

        one_to_four = typing.Literal[1, 2, 3, 4]
        value = validate(value=3).required.as_one_of(int, one_to_four)
        self.assertEqual(3, value)

        try:
            validate(value=0).required.as_one_of(int, 1, 2, 3, 4)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-One-Of | The parameter 'value' must be one of the expected values [1, 2, 3, 4], found "0".""", e.message)

    def test_many_types(self):
        for v in ["a", 1, 2.3, True]:
            value = validate(value=v).as_type(str, int, bool, float)
            self.assertEqual(v, value)

        try:
            validate(value="moo").as_type(int, bool, float)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Type | Expected the parameter 'value' to be of type <class 'int'>, <class 'bool'> or <class 'float'>, found <class 'str'>.""", e.message)

    def test_list_value(self):

        values: List[str] = validate(value=["a", "b", "c", "d"]).list(str)
        self.assertEqual(list, type(values))
        self.assertEqual(4, len(values))

        values: List[str] = validate(value=["a", "b", "c", "d"]).list(str, min_length=0)
        self.assertEqual(list, type(values))
        self.assertEqual(4, len(values))

        values: List[str] = validate(value=["a", "b", "c", "d"]).list(str, min_length=1)
        self.assertEqual(list, type(values))
        self.assertEqual(4, len(values))

        values: List[str] = validate(value=["a", "b", "c", "d"]).list(str, min_length=2)
        self.assertEqual(list, type(values))
        self.assertEqual(4, len(values))

        values: List[str] = validate(value=["a", "b", "c", "d"]).list(str, min_length=3)
        self.assertEqual(list, type(values))
        self.assertEqual(4, len(values))

        values: List[str] = validate(value=["a", "b", "c", "d"]).list(str, min_length=4)
        self.assertEqual(list, type(values))
        self.assertEqual(4, len(values))

        try:
            validate(value=["a", "b", "c", "d"]).list(str, min_length=5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Len | The parameter 'value' must have a minimum length of 5, found 4.""", e.message)

        try:
            validate(value=["a", "b", 1, "d"]).list(str)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Element-Type | Expected element 2 of 'value' to be of type <class 'str'>, found "1" of type <class 'int'>.""", e.message)

    def test_dict_value(self):
        values: Dict[str, int] = validate(value={"a": 1, "b": 2, "c": 3, "d": 4}).dict(str, min_length=0)
        self.assertEqual(dict, type(values))
        self.assertEqual(4, len(values))

        values: Dict[str, int] = validate(value={"a": 1, "b": 2, "c": 3, "d": 4}).dict(str, min_length=1)
        self.assertEqual(dict, type(values))
        self.assertEqual(4, len(values))

        values: Dict[str, int] = validate(value={"a": 1, "b": 2, "c": 3, "d": 4}).dict(str, min_length=2)
        self.assertEqual(dict, type(values))
        self.assertEqual(4, len(values))

        values: Dict[str, int] = validate(value={"a": 1, "b": 2, "c": 3, "d": 4}).dict(str, min_length=3)
        self.assertEqual(dict, type(values))
        self.assertEqual(4, len(values))

        values: Dict[str, int] = validate(value={"a": 1, "b": 2, "c": 3, "d": 4}).dict(str, min_length=4)
        self.assertEqual(dict, type(values))
        self.assertEqual(4, len(values))

        try:
            validate(value={"a": 1, "b": 2, "c": 3, "d": 4}).dict(str, min_length=5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Len | The parameter 'value' must have a minimum length of 5, found 4.""", e.message)

    def test_iterable_value(self):
        from typing import Iterable

        values: Iterable = validate(value=["a", "b", "c", "d"]).iterable(str)
        self.assertTrue(isinstance(values, Iterable))
        self.assertEqual(4, len(list(values)))

        values: Iterable = validate(value=["a", "b", "c", "d"]).iterable(str)
        self.assertTrue(isinstance(values, Iterable))
        self.assertEqual(4, len(list(values)))

        values: Iterable = validate(value=["a", "b", "c", "d"]).iterable(str)
        self.assertTrue(isinstance(values, Iterable))
        self.assertEqual(4, len(list(values)))

        values: Iterable = validate(value=["a", "b", "c", "d"]).iterable(str)
        self.assertTrue(isinstance(values, Iterable))
        self.assertEqual(4, len(list(values)))

        values: Iterable = validate(value=["a", "b", "c", "d"]).iterable(str)
        self.assertTrue(isinstance(values, Iterable))
        self.assertEqual(4, len(list(values)))

    def test_lists_element_types(self):

        values = validate(some_list=None).list(numbers.Number)
        self.assertIsNone(values)

        values = validate(some_list=None).list(numbers.Number, auto_create=True)
        self.assertIsNotNone(values)
        self.assertEqual(list(), values)

        expected = [1, 2.0, 3]
        values = validate(some_list=expected).list(numbers.Number)
        self.assertEqual(expected, values)

        expected = ["1", "2", "3"]
        values = validate(some_list=expected).list(str)
        self.assertEqual(expected, values)

        expected = [1, 2, 3]
        values = validate(some_list=expected).list(int)
        self.assertEqual(expected, values)

        expected = [1.0, 2.0, 3.0]
        values = validate(some_list=expected).list(float)
        self.assertEqual(expected, values)

        expected = [True, False, True]
        values = validate(some_list=expected).list(bool)
        self.assertEqual(expected, values)

        try:
            validate(some_list=[1, 2.0, 3]).list(int)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Element-Type | Expected element 1 of 'some_list' to be of type <class 'int'>, found "2.0" of type <class 'float'>.""", e.message)

    def test_sets_element_types(self):

        values = validate(some_set=None).set(numbers.Number)
        self.assertIsNone(values)

        values = validate(some_set=None).set(numbers.Number, auto_create=True)
        self.assertIsNotNone(values)
        self.assertEqual(set(), values)

        expected = {1, 2.0, 3}
        values = validate(some_set=expected).set(numbers.Number)
        self.assertEqual(expected, values)

        expected = {"1", "2", "3"}
        values = validate(some_set=expected).set(str)
        self.assertEqual(expected, values)

        expected = {1, 2, 3}
        values = validate(some_set=expected).set(int)
        self.assertEqual(expected, values)

        expected = {1.0, 2.0, 3.0}
        values = validate(some_set=expected).set(float)
        self.assertEqual(expected, values)

        expected = {True, False, True}
        values = validate(some_set=expected).set(bool)
        self.assertEqual(expected, values)

        try:
            validate(some_set={1, 2.0, 3}).set(int)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Element-Type | Expected element 1 of 'some_set' to be of type <class 'int'>, found "2.0" of type <class 'float'>.""", e.message)

    def test_dict_key_types(self):

        values = validate(some_dict=None).dict(numbers.Number)
        self.assertIsNone(values)

        values = validate(some_dict=None).dict(numbers.Number, auto_create=True)
        self.assertIsNotNone(values)
        self.assertEqual(dict(), values)

        expected = {1: "moo", 2.0: None, 3: True}
        values = validate(some_dict=expected).dict(numbers.Number)
        self.assertEqual(expected, values)

        expected = {"1": "moo", "2": None, "3": True}
        values = validate(some_dict=expected).dict(str)
        self.assertEqual(expected, values)

        expected = {1: "moo", 2: None, 3: True}
        values = validate(some_dict=expected).dict(int)
        self.assertEqual(expected, values)

        expected = {1.0: "moo", 2.0: None, 3.0: True}
        values = validate(some_dict=expected).dict(float)
        self.assertEqual(expected, values)

        expected = {True: "Moo", False: None}
        values = validate(some_dict=expected).dict(bool)
        self.assertEqual(expected, values)

        try:
            validate(some_dict={1: "moo", 2.0: None, 3: True}).dict(int)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Element-Type | Expected the key "2.0" of 'some_dict' to be of type <class 'int'>, found the type <class 'float'>.""", e.message)

        expected = {"1": "one", "2": "two", "3": "three"}
        values = validate(some_dict=expected).dict(str)
        self.assertEqual(expected, values)

        expected = {"1": "one", "2": "two", "3": "three"}
        values = validate(some_dict=expected).dict(str, str)
        self.assertEqual(expected, values)

        try:
            expected = {"1": "one", "2": 2, "3": "three"}
            values = validate(some_dict=expected).dict(str, str)
            self.assertEqual(expected, values)
        except ValidationError as e:
            self.assertEqual("""Element-Type | Expected the entry for key "2" of 'some_dict' to be of type <class 'str'>, found the type <class 'int'>.""", e.message)

    def test_generic_types(self):

        values: List[str] = validate(value=["a", "b", "c", "d"]).list(str)
        self.assertEqual(["a", "b", "c", "d"], values)

        values: List[str] = validate(value=["a", "b", "c", "d"]).as_type(list)
        self.assertEqual(["a", "b", "c", "d"], values)

        values: List[str] = validate(value=["a", "b", "c", "d"]).as_type(List)
        self.assertEqual(["a", "b", "c", "d"], values)

        values: List[str] = validate(value=["a", "b", "c", "d"]).as_type(List[str])
        self.assertEqual(["a", "b", "c", "d"], values)

        values: Set[str] = validate(value={"a", "b", "a", "b"}).as_type(Set[str])
        self.assertEqual({"a", "b"}, values)

        # noinspection PyDictDuplicateKeys
        values: Dict[str] = validate(value={"1": "one", "2": "two", "3": "three", "3": "four"}).as_type(Dict[str, Any])
        self.assertEqual({"1": "one", "2": "two", "3": "four"}, values)

        values: Tuple[int, str, bool] = validate(value=(1, "one", True)).as_type(Tuple[int, str, bool])
        self.assertEqual((1, "one", True), values)

        try:
            values: List[str] = validate(value=["a", "b", "c", "d"]).as_type(List[str])
            self.assertEqual(4, len(values))
        except TypeError as e:
            self.assertEqual("Subscripted generics cannot be used with class and instance checks", e.args[0])


if __name__ == '__main__':
    unittest.main()
