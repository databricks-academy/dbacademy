import unittest
from dbacademy.dbhelper.tests.testing_suite_class import TestingSuite


class TestTesting(unittest.TestCase):

    DESCRIPTION = "Testing 123."
    SUITE_NAME = "Whatever"

    def test_suite(self):
        suite = TestingSuite(self.SUITE_NAME)
        self.validate_test_suite(suite, False, 0)

    def validate_test_suite(self, suite, expected_passed, test_cases_count):
        self.assertEqual(suite.name, self.SUITE_NAME)
        self.assertEqual(suite.passed, expected_passed)
        self.assertEqual(len(suite.test_cases), test_cases_count)

        return None if len(suite.test_cases) == 0 else suite.test_cases[0]

    def validate_test_case(self, test_case, actual_value):
        self.assertEqual(test_case.hint, f"Found, {actual_value}")
        self.assertEqual(test_case.description, self.DESCRIPTION)

    def test_is_none(self):

        for actual_value, expected_passed in [(None, True), ("Bananas", False)]:

            suite = TestingSuite(self.SUITE_NAME)
            suite.test_is_none(lambda: actual_value,
                               hint=f"Found, [[ACTUAL_VALUE]]",
                               description=f"Testing 123.")

            test_case = self.validate_test_suite(suite, expected_passed, 1)
            self.validate_test_case(test_case, actual_value)

    def test_not_none(self):

        for actual_value, expected_passed in [(None, False), ("Bananas", True)]:

            suite = TestingSuite(self.SUITE_NAME)
            suite.test_not_none(lambda: actual_value,
                                hint=f"Found, [[ACTUAL_VALUE]]",
                                description=f"Testing 123.")

            test_case = self.validate_test_suite(suite, expected_passed, 1)
            self.validate_test_case(test_case, actual_value)


if __name__ == '__main__':
    unittest.main()
