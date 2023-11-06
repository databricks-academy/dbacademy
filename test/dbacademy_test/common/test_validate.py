__all__ = ["MyTestCase"]

import numbers
import unittest
from dbacademy.common import validate

EXPECTED_ASSERTION_ERROR = "Expected AssertionError"


# noinspection PyMethodMayBeStatic
class MyTestCase(unittest.TestCase):

    def test_no_value(self):
        try:
            # noinspection PyTypeChecker
            validate.any_value(parameter_type=int)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Internal | validate.any_value({}) expects one and only one parameter, found 0.""")

    def test_parameter_type_is_missing(self):
        try:
            # noinspection PyArgumentList
            validate.any_value(value=1)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except TypeError as e:
            self.assertEqual(e.args[0], """any_value() missing 1 required positional argument: 'parameter_type'""")

    def test_parameter_type_is_none(self):
        try:
            # noinspection PyTypeChecker
            validate.any_value(value=1, parameter_type=None)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Internal | The parameter 'parameter_type' must be specified.""")

    def test_parameter_min_length_type(self):
        validate.any_value(value=1, parameter_type=int, min_length=None)
        try:
            # noinspection PyTypeChecker
            validate.any_value(value=1, parameter_type=int, min_length="moo")
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Internal | Expected the parameter 'min_length' to be of type int, found <class 'str'>.""")

    def test_parameter_min_value_type(self):
        validate.any_value(value=1, parameter_type=int, min_value=None)
        try:
            # noinspection PyTypeChecker
            validate.any_value(value=1, parameter_type=int, min_value="moo")
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Internal | Expected the parameter 'min_value' to be of type numbers.Number, found <class 'str'>.""")

    def test_wrong_parameter_type_or_none(self):
        value = 1

        validate.any_value(value=value, parameter_type=int)
        validate.any_value(value=value, parameter_type=numbers.Number)

        try:
            validate.any_value(value=value, parameter_type=str)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Type | Expected the parameter 'value' to be None or of type <class 'str'>, found <class 'int'>.""")

        # Required as None and False should behave the same.
        try:
            validate.any_value(value=value, parameter_type=str, required=False)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Type | Expected the parameter 'value' to be None or of type <class 'str'>, found <class 'int'>.""")

    def test_wrong_parameter_type_but_required(self):
        value = 1

        try:
            validate.any_value(value=value, parameter_type=str, required=True)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Type | Expected the parameter 'value' to be of type <class 'str'>, found <class 'int'>.""")

    def test_str_value(self):
        validate.str_value(value="asdf", min_length=0)
        validate.str_value(value="asdf", min_length=1)
        validate.str_value(value="asdf", min_length=2)
        validate.str_value(value="asdf", min_length=3)
        validate.str_value(value="asdf", min_length=4)

        try:
            validate.str_value(value="asdf", required=True, min_length=5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Min-Len | The parameter 'value' must have a minimum length of 5, found 4.""")

    def test_int_value(self):
        validate.int_value(value=None)
        validate.int_value(value=1)
        validate.int_value(value=1, required=True)

        validate.int_value(value=4, min_value=2)
        validate.int_value(value=3, min_value=2)
        validate.int_value(value=2, min_value=2)

        try:
            validate.int_value(value=1, min_value=2)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Min-Value | The parameter 'value' must have a minimum value of '2', found '1'.""")

        try:
            validate.int_value(value=None, required=True)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Not-None | The parameter 'value' must be specified.""")

        try:
            validate.int_value(value=1.0)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Type | Expected the parameter 'value' to be None or of type <class 'int'>, found <class 'float'>.""")

    def test_float_value(self):
        validate.float_value(value=None)
        validate.float_value(value=1.0)
        validate.float_value(value=1.0, required=True)

        validate.float_value(value=4.0, min_value=2)
        validate.float_value(value=4.0, min_value=2.5)

        validate.float_value(value=3.0, min_value=2)
        validate.float_value(value=3.0, min_value=2.5)

        try:
            validate.float_value(value=2.0, min_value=2)
            validate.float_value(value=2.0, min_value=2.5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Min-Value | The parameter 'value' must have a minimum value of '2.5', found '2.0'.""")

        try:
            validate.float_value(value=2.0, min_value=3)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Min-Value | The parameter 'value' must have a minimum value of '3', found '2.0'.""")

        try:
            validate.float_value(value=None, required=True)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Not-None | The parameter 'value' must be specified.""")

        try:
            validate.float_value(value=1)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Type | Expected the parameter 'value' to be None or of type <class 'float'>, found <class 'int'>.""")

    def test_number_value(self):
        from numbers import Number

        validate.any_value(value=None, parameter_type=Number)

        validate.any_value(value=1, parameter_type=Number)
        validate.any_value(value=1.0, parameter_type=Number)

        validate.any_value(value=1, required=True, parameter_type=Number)
        validate.any_value(value=1.0, required=True, parameter_type=Number)

        validate.any_value(value=4, min_value=2, parameter_type=Number)
        validate.any_value(value=4.0, min_value=2, parameter_type=Number)

        validate.any_value(value=4, min_value=2.5, parameter_type=Number)
        validate.any_value(value=4.0, min_value=2.5, parameter_type=Number)

        validate.any_value(value=3, min_value=2, parameter_type=Number)
        validate.any_value(value=3.0, min_value=2.5, parameter_type=Number)

        validate.any_value(value=3, min_value=2, parameter_type=Number)
        validate.any_value(value=3.0, min_value=2.5, parameter_type=Number)

        try:
            validate.any_value(value=2.0, min_value=2, parameter_type=Number)
            validate.any_value(value=2.0, min_value=2.5, parameter_type=Number)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Min-Value | The parameter 'value' must have a minimum value of '2.5', found '2.0'.""")

        try:
            validate.any_value(value=2.0, min_value=3, parameter_type=Number)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Min-Value | The parameter 'value' must have a minimum value of '3', found '2.0'.""")

        try:
            validate.any_value(value=None, required=True, parameter_type=Number)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Not-None | The parameter 'value' must be specified.""")

    def test_bool_value(self):
        validate.bool_value(value=True)
        validate.bool_value(value=False)
        validate.bool_value(value=None)

        try:
            validate.bool_value(value=None, required=True)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Not-None | The parameter 'value' must be specified.""")

    def test_list_value(self):
        validate.list_value(value=["a", "b", "c", "d"], min_length=0)
        validate.list_value(value=["a", "b", "c", "d"], min_length=1)
        validate.list_value(value=["a", "b", "c", "d"], min_length=2)
        validate.list_value(value=["a", "b", "c", "d"], min_length=3)
        validate.list_value(value=["a", "b", "c", "d"], min_length=4)

        try:
            validate.list_value(value=["a", "b", "c", "d"], min_length=5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Min-Len | The parameter 'value' must have a minimum length of 5, found 4.""")

    def test_dict_value(self):
        validate.dict_value(value={"a": 1, "b": 2, "c": 3, "d": 4}, min_length=0)
        validate.dict_value(value={"a": 1, "b": 2, "c": 3, "d": 4}, min_length=1)
        validate.dict_value(value={"a": 1, "b": 2, "c": 3, "d": 4}, min_length=2)
        validate.dict_value(value={"a": 1, "b": 2, "c": 3, "d": 4}, min_length=3)
        validate.dict_value(value={"a": 1, "b": 2, "c": 3, "d": 4}, min_length=4)

        try:
            validate.dict_value(value={"a": 1, "b": 2, "c": 3, "d": 4}, min_length=5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Min-Len | The parameter 'value' must have a minimum length of 5, found 4.""")

    def test_iterable_value(self):
        validate.iterable_value(value=["a", "b", "c", "d"], min_length=0)
        validate.iterable_value(value=["a", "b", "c", "d"], min_length=1)
        validate.iterable_value(value=["a", "b", "c", "d"], min_length=2)
        validate.iterable_value(value=["a", "b", "c", "d"], min_length=3)
        validate.iterable_value(value=["a", "b", "c", "d"], min_length=4)

        try:
            validate.iterable_value(value=["a", "b", "c", "d"], min_length=5)
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Error-Min-Len | The parameter 'value' must have a minimum length of 5, found 4.""")

    def test_lists_element_types(self):

        values = validate.list_of_type(some_list=None, element_type=numbers.Number, required=False)
        self.assertIsNone(values)

        values = validate.list_of_type(some_list=None, element_type=numbers.Number, auto_create=True)
        self.assertIsNotNone(values)

        expected = [1, 2.0, 3]
        values = validate.list_of_type(some_list=expected, element_type=numbers.Number)
        self.assertEqual(expected, values)

        expected = ["1", "2", "3"]
        values = validate.list_of_strings(some_list=expected)
        self.assertEqual(expected, values)

        expected = [1, 2, 3]
        values = validate.list_of_ints(some_list=expected)
        self.assertEqual(expected, values)

        expected = [1.0, 2.0, 3.0]
        values = validate.list_of_floats(some_list=expected)
        self.assertEqual(expected, values)

        expected = [True, False, True]
        values = validate.list_of_bools(some_list=expected)
        self.assertEqual(expected, values)

        try:
            validate.list_of_ints(some_list=[1, 2.0, 3])
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Expected element 1 of 'some_list' to be of type <class 'int'>, found <class 'float'>.""")

    def test_sets_element_types(self):

        values = validate.set_of_type(some_set=None, element_type=numbers.Number, required=False)
        self.assertIsNone(values)

        values = validate.set_of_type(some_set=None, element_type=numbers.Number, auto_create=True)
        self.assertIsNotNone(values)

        expected = {1, 2.0, 3}
        values = validate.set_of_type(some_set=expected, element_type=numbers.Number)
        self.assertEqual(expected, values)

        expected = {"1", "2", "3"}
        values = validate.set_of_strings(some_set=expected)
        self.assertEqual(expected, values)

        expected = {1, 2, 3}
        values = validate.set_of_ints(some_set=expected)
        self.assertEqual(expected, values)

        expected = {1.0, 2.0, 3.0}
        values = validate.set_of_floats(some_set=expected)
        self.assertEqual(expected, values)

        expected = {True, False, True}
        values = validate.set_of_bools(some_set=expected)
        self.assertEqual(expected, values)

        try:
            validate.set_of_ints(some_set={1, 2.0, 3})
            self.fail(EXPECTED_ASSERTION_ERROR)
        except validate.ValidationError as e:
            self.assertEqual(e.message, """Expected element 1 of 'some_set' to be of type <class 'int'>, found <class 'float'>.""")

# "list_of_type",
# "list_of_strings",
# "list_of_ints",
# "list_of_floats",
# "list_of_bools",
# "set_of_type",
# "set_of_strings",
# "set_of_ints",
# "set_of_floats",
# "set_of_bools",


if __name__ == '__main__':
    unittest.main()
