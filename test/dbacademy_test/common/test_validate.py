__all__ = ["MyTestCase"]

import numbers
import unittest
from typing import Dict, List, Set
from dbacademy.common import validate, ValidationError

EXPECTED_ASSERTION_ERROR = "Expected AssertionError"


class MyTestCase(unittest.TestCase):

    def test_no_value(self):
        try:
            validate()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | validate(..) expects one and only one parameter, found 0.""", e.message)

    def test_parameter_type(self):
        validate(value=1).type(int)
        validate(value=[1, 2, 3]).type(List)
        validate(value={1, 2, 3}).type(Set)
        validate(value={1, 2, 3}).set().of_type(int, min_length=0)
        validate(value={"color": "red"}).type(Dict)

        try:
            validate(value=1).type(None)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected the Validator's parameter 'parameter_type' to be specified.""", e.message)

        try:
            # noinspection PyTypeChecker
            validate(value=1).type("moo")
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected the Validator's parameter 'parameter_type' to be a python "type", found <class 'str'>.""", e.message)

    def test_parameter_min_length_type(self):
        validate(value="moo").str().min_length(0)
        try:
            # noinspection PyTypeChecker
            validate(value="moo").str().min_length(None)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected the Validator's parameter 'min_length' to be specified.""", e.message)

        try:
            # noinspection PyTypeChecker
            validate(value="moo").str().min_length("moo")
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected the Validator's parameter 'min_length' to be of type int, found <class 'str'>.""", e.message)

    def test_parameter_min_value_type(self):
        validate(value=1).int().min_value(1)
        try:
            # noinspection PyTypeChecker
            validate(value=1).int().min_value(None)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected the Validator's parameter 'min_value' to be specified.""", e.message)

        try:
            # noinspection PyTypeChecker
            validate(value=1).int().min_value("moo")
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected the Validator's parameter 'min_value' to be of type numbers.Number, found <class 'str'>.""", e.message)

    def test_parameter_max_value_type(self):
        validate(value=5).int().max_value(5)
        try:
            # noinspection PyTypeChecker
            validate(value=1).int().max_value(None)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected the Validator's parameter 'max_value' to be specified.""", e.message)

        try:
            # noinspection PyTypeChecker
            validate(value=1).int().max_value("moo")
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Internal | Expected the Validator's parameter 'max_value' to be of type numbers.Number, found <class 'str'>.""", e.message)

    def test_wrong_parameter_type(self):

        validate(value=1).int()
        validate(value=1).type(numbers.Number)

        try:
            validate(value=1).str()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Type | Expected the parameter 'value' to be of type <class 'str'>, found <class 'int'>.""", e.message)

        try:
            validate(value=1).dict()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Type | Expected the parameter 'value' to be of type <class 'dict'>, found <class 'int'>.""", e.message)

    def test_str_value(self):
        validate(value="asdf").str().min_length(0)
        validate(value="asdf").str().min_length(1)
        validate(value="asdf").str().min_length(2)
        validate(value="asdf").str().min_length(3)
        validate(value="asdf").str().min_length(4)

        try:
            validate(value="asdf").str().min_length(5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Len | The parameter 'value' must have a minimum length of 5, found 4.""", e.message)

    def test_int_value(self):
        validate(value=None).int()
        validate(value=1).int()
        validate(value=1).int_required()

        validate(value=4).int().min_value(2)
        validate(value=3).int().min_value(2)
        validate(value=2).int().min_value(2)

        validate(value=4).int().max_value(4)
        validate(value=3).int().max_value(4)
        validate(value=2).int().max_value(4)

        validate(value=4).int().min_max_value(2, 4)
        validate(value=3).int().min_max_value(2, 4)
        validate(value=2).int().min_max_value(2, 4)

        try:
            validate(value=1).int().min_value(2)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Value | The parameter 'value' must have a minimum value of '2', found '1'.""", e.message)

        try:
            validate(value=5).int().max_value(4)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Max-Value | The parameter 'value' must have a maximum value of '4', found '5'.""", e.message)

        try:
            validate(value=1).int().min_max_value(2, 4)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Value | The parameter 'value' must have a minimum value of '2', found '1'.""", e.message)

        try:
            validate(value=5).int().min_max_value(2, 4)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Max-Value | The parameter 'value' must have a maximum value of '4', found '5'.""", e.message)

        try:
            validate(value=None).int_required()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Not-None | The parameter 'value' must be specified.""", e.message)

        try:
            validate(value=1.0).int()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Type | Expected the parameter 'value' to be of type <class 'int'>, found <class 'float'>.""", e.message)

    def test_float_value(self):
        validate(value=None).float()
        validate(value=1.0).float()
        validate(value=1.0).float_required()

        validate(value=4.0).float().min_value(2)
        validate(value=4.0).float().min_value(2.5)

        validate(value=3.0).float().min_value(2)
        validate(value=3.0).float().min_value(2.5)

        # Start with an int, but require a float
        validate(value=1).float()

        try:
            validate(value=2.0).float().min_value(2)
            validate(value=2.0).float().min_value(2.5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Value | The parameter 'value' must have a minimum value of '2.5', found '2.0'.""", e.message)

        try:
            validate(value=2.0).float().min_value(3)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Value | The parameter 'value' must have a minimum value of '3', found '2.0'.""", e.message)

        try:
            validate(value=None).float_required()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Not-None | The parameter 'value' must be specified.""", e.message)

    def test_number_value(self):
        validate(value=None).number()

        validate(value=1).number()
        validate(value=1.0).number()

        validate(value=1).number_required()
        validate(value=1.0).number_required()

        validate(value=4).number().min_value(2)
        validate(value=4.0).number().min_value(2)

        validate(value=4).number().min_value(2.5)
        validate(value=4.0).number().min_value(2.5)

        validate(value=3).number().min_value(2)
        validate(value=3.0).number().min_value(2.5)

        validate(value=3).number().min_value(2)
        validate(value=3.0).number().min_value(2.5)

        try:
            validate(value=2.0).number().min_value(2)
            validate(value=2.0).number().min_value(2.5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Value | The parameter 'value' must have a minimum value of '2.5', found '2.0'.""", e.message)

        try:
            validate(value=2.0).number().min_value(3)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Value | The parameter 'value' must have a minimum value of '3', found '2.0'.""", e.message)

        try:
            validate(value=None).number_required()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Not-None | The parameter 'value' must be specified.""", e.message)

    def test_bool_value(self):
        validate(value=True).bool()
        validate(value=False).bool()
        validate(value=None).bool()

        try:
            validate(value=None).bool_required()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Not-None | The parameter 'value' must be specified.""", e.message)

    def test_list_value(self):
        validate(value=["a", "b", "c", "d"]).list().of_str(min_length=0)
        validate(value=["a", "b", "c", "d"]).list().of_str(min_length=1)
        validate(value=["a", "b", "c", "d"]).list().of_str(min_length=2)
        validate(value=["a", "b", "c", "d"]).list().of_str(min_length=3)
        validate(value=["a", "b", "c", "d"]).list().of_str(min_length=4)

        try:
            validate(value=["a", "b", "c", "d"]).list().of_str(min_length=5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Len | The parameter 'value' must have a minimum length of 5, found 4.""", e.message)

    def test_dict_value(self):
        validate(value={"a": 1, "b": 2, "c": 3, "d": 4}).dict().keys_of_str(min_length=0)
        validate(value={"a": 1, "b": 2, "c": 3, "d": 4}).dict().keys_of_str(min_length=1)
        validate(value={"a": 1, "b": 2, "c": 3, "d": 4}).dict().keys_of_str(min_length=2)
        validate(value={"a": 1, "b": 2, "c": 3, "d": 4}).dict().keys_of_str(min_length=3)
        validate(value={"a": 1, "b": 2, "c": 3, "d": 4}).dict().keys_of_str(min_length=4)

        try:
            validate(value={"a": 1, "b": 2, "c": 3, "d": 4}).dict().keys_of_str(min_length=5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Len | The parameter 'value' must have a minimum length of 5, found 4.""", e.message)

    def test_iterable_value(self):
        validate(value=["a", "b", "c", "d"]).iterable().of_str(min_length=0)
        validate(value=["a", "b", "c", "d"]).iterable().of_str(min_length=1)
        validate(value=["a", "b", "c", "d"]).iterable().of_str(min_length=2)
        validate(value=["a", "b", "c", "d"]).iterable().of_str(min_length=3)
        validate(value=["a", "b", "c", "d"]).iterable().of_str(min_length=4)

        try:
            validate(value=["a", "b", "c", "d"]).iterable().of_str(min_length=5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Error-Min-Len | The parameter 'value' must have a minimum length of 5, found 4.""", e.message)

    def test_lists_element_types(self):

        values = validate(some_list=None).list().of_type(numbers.Number)
        self.assertIsNone(values)

        values = validate(some_list=None).list(create=True).of_type(numbers.Number)
        self.assertIsNotNone(values)
        self.assertEqual(list(), values)

        expected = [1, 2.0, 3]
        values = validate(some_list=expected).list().of_type(numbers.Number)
        self.assertEqual(expected, values)

        expected = ["1", "2", "3"]
        values = validate(some_list=expected).list().of_str()
        self.assertEqual(expected, values)

        expected = [1, 2, 3]
        values = validate(some_list=expected).list().of_int()
        self.assertEqual(expected, values)

        expected = [1.0, 2.0, 3.0]
        values = validate(some_list=expected).list().of_float()
        self.assertEqual(expected, values)

        expected = [True, False, True]
        values = validate(some_list=expected).list().of_bool()
        self.assertEqual(expected, values)

        try:
            validate(some_list=[1, 2.0, 3]).list().of_int()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Element-Type | Expected element 1 of 'some_list' to be of type <class 'int'>, found "2.0" of type <class 'float'>.""", e.message)

    def test_sets_element_types(self):

        values = validate(some_set=None).set().of_type(numbers.Number)
        self.assertIsNone(values)

        values = validate(some_set=None).set(create=True).of_type(numbers.Number)
        self.assertIsNotNone(values)
        self.assertEqual(set(), values)

        expected = {1, 2.0, 3}
        values = validate(some_set=expected).set().of_type(numbers.Number)
        self.assertEqual(expected, values)

        expected = {"1", "2", "3"}
        values = validate(some_set=expected).set().of_str()
        self.assertEqual(expected, values)

        expected = {1, 2, 3}
        values = validate(some_set=expected).set().of_int()
        self.assertEqual(expected, values)

        expected = {1.0, 2.0, 3.0}
        values = validate(some_set=expected).set().of_float()
        self.assertEqual(expected, values)

        expected = {True, False, True}
        values = validate(some_set=expected).set().of_bool()
        self.assertEqual(expected, values)

        try:
            validate(some_set={1, 2.0, 3}).set().of_int()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Element-Type | Expected element 1 of 'some_set' to be of type <class 'int'>, found "2.0" of type <class 'float'>.""", e.message)

    def test_dict_key_types(self):

        values = validate(some_dict=None).dict().keys_of_type(numbers.Number)
        self.assertIsNone(values)

        values = validate(some_dict=None).dict(create=True).keys_of_type(numbers.Number)
        self.assertIsNotNone(values)
        self.assertEqual(dict(), values)

        expected = {1: "moo", 2.0: None, 3: True}
        values = validate(some_dict=expected).dict().keys_of_type(numbers.Number)
        self.assertEqual(expected, values)

        expected = {"1": "moo", "2": None, "3": True}
        values = validate(some_dict=expected).dict().keys_of_str()
        self.assertEqual(expected, values)

        expected = {1: "moo", 2: None, 3: True}
        values = validate(some_dict=expected).dict().keys_of_int()
        self.assertEqual(expected, values)

        expected = {1.0: "moo", 2.0: None, 3.0: True}
        values = validate(some_dict=expected).dict().keys_of_float()
        self.assertEqual(expected, values)

        expected = {True: "Moo", False: None}
        values = validate(some_dict=expected).dict().keys_of_bool()
        self.assertEqual(expected, values)

        try:
            validate(some_dict={1: "moo", 2.0: None, 3: True}).dict().keys_of_int()
            self.fail(EXPECTED_ASSERTION_ERROR)
        except ValidationError as e:
            self.assertEqual("""Element-Type | Expected key 1 of 'some_dict' to be of type <class 'int'>, found "2.0" of type <class 'float'>.""", e.message)


if __name__ == '__main__':
    unittest.main()
