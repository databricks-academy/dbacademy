class Validation(object):
    from typing import Callable, Any, List

    __slots__ = ('description', 'test_function', 'test_case_id', 'unique_id', 'depends_on', 'escape_html', 'points', 'hint', "actual_value")

    _LAST_ID = 0

    def __init__(self,
                 *,
                 suite,
                 test_function: Callable[[], Any],
                 actual_value: Callable[[], Any],
                 description: str,
                 test_case_id: str,
                 depends_on: List[str],
                 escape_html: bool,
                 points: int,
                 hint):

        # from dbacademy.dbhelper import TestSuite
        # assert str(type(suite)) == TestSuite, f"Expected the parameter \"suite\" to be of type TestSuite, found {type(suite)}."

        assert type(description) == str, f"Expected the parameter \"description\" to be of type str, found {type(description)}."

        if test_case_id is None:
            Validation._LAST_ID += 1
            test_case_id = str(Validation._LAST_ID)

        self.test_case_id = f"{suite.name}-{test_case_id}"

        self.hint = hint
        self.points = points
        self.escape_html = escape_html
        self.description = description
        self.test_function = test_function
        self.actual_value = actual_value

        depends_on = depends_on or [suite.last_test_id()]
        self.depends_on = depends_on if type(depends_on) is list else [depends_on]

    def update_hint(self):
        from html import escape
        if self.hint is not None:
            try:
                actual_value = self.actual_value()
            except:
                actual_value = "actual_value() call failed"

            # Set the hint's ACTUAL_VALUE to the actual value.
            self.hint = self.hint.replace("[[ACTUAL_VALUE]]", escape(str(actual_value)))

            try:
                # Set the hint's LEN_ACTUAL_VALUE to the length of the actual value.
                self.hint = self.hint.replace("[[LEN_ACTUAL_VALUE]]", str(len(actual_value)))
            except:
                # If that failed (e.g. length() cannot be used on the object), use a default rendering
                self.hint = self.hint.replace("[[LEN_ACTUAL_VALUE]]", escape(str(actual_value)))
