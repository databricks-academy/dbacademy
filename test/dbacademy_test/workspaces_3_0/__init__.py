import unittest
from typing import Callable


def test_assertion_error(test: unittest.TestCase, expected_msg: str, test_lambda: Callable):
    try:
        test_lambda()
        raise Exception("Expected an AssertionError")

    except AssertionError as e:
        if expected_msg != str(e):
            test.assertEqual(expected_msg, str(e))


def test_index_error(test: unittest.TestCase, test_lambda: Callable):
    try:
        test_lambda()
        raise Exception("Expected an IndexError")

    except IndexError as e:
        if "list index out of range" != str(e):
            test.assertEqual("list index out of range", str(e))
