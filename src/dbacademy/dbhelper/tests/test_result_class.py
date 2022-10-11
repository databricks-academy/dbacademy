class TestResult(object):
    from dbacademy_helper.tests.test_case_class import TestCase

    __slots__ = ('test', 'skipped', 'passed', 'status', 'points', 'exception', 'message')

    def __init__(self, test: TestCase, skipped: bool = False):
        try:
            self.test = test
            self.skipped = skipped
            if skipped:
                self.status = "skipped"
                self.passed = False
                self.points = 0
            else:
                assert test.test_function(), "Test returned false"
                self.status = "passed"
                self.passed = True
                self.points = self.test.points
            self.exception = None
            self.message = ""

        except AssertionError as e:
            self.status = "failed"
            self.passed = False
            self.points = 0
            self.exception = e
            self.message = ""

        except Exception as e:
            self.status = "failed"
            self.passed = False
            self.points = 0
            self.exception = e
            self.message = str(e)
