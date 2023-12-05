import unittest
from typing import Callable


def test_assertion_error(test: unittest.TestCase, error_code: str, test_lambda: Callable):
    try:
        test_lambda()
        raise Exception("Expected an AssertionError")

    except AssertionError as e:
        if str(e).startswith(error_code):
            test.assertTrue(str(e).startswith(error_code))


def test_index_error(test: unittest.TestCase, test_lambda: Callable):
    try:
        test_lambda()
        raise Exception("Expected an IndexError")

    except IndexError as e:
        if "list index out of range" != str(e):
            test.assertEqual("list index out of range", str(e))
