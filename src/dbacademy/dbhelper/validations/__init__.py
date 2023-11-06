__all__ = ["lazy_property", "TEST_RESULTS_STYLE", "ValidationSuite", "ValidationHelper"]

from typing import List, Callable, Iterable, Any, Sized
from dbacademy.clients.databricks import DBAcademyRestClient
from dbacademy.dbhelper.validations.validation_class import Validation
from dbacademy.dbhelper.validations.validation_result_class import ValidationResult


# Decorator to lazy evaluate - used by TestSuite
def lazy_property(fn):
    """
    Decorator that makes a property lazy-evaluated.
    """
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)

    return _lazy_property


TEST_RESULTS_STYLE = """
    <style>
      table { text-align: left; border-collapse: collapse; margin: 1em; caption-side: bottom; font-family: Sans-Serif; font-size: 16px}
      caption { text-align: left; padding: 5px }
      th, td { border: 1px solid #ddd; padding: 5px }
      th { background-color: #ddd }
      .passed { background-color: #97d897 }
      .failed { background-color: #e2716c }
      .skipped { background-color: #f9d275 }
      .results .points { display: none }
      .results .message { display: block; font-size:smaller; color:gray }
      .results .note { display: block; font-size:smaller; font-decoration:italics }
      .results .passed::before  { content: "Passed" }
      .results .failed::before  { content: "Failed" }
      .results .skipped::before { content: "Skipped" }
      .grade .passed  .message:empty::before { content:"Passed" }
      .grade .failed  .message:empty::before { content:"Failed" }
      .grade .skipped .message:empty::before { content:"Skipped" }
    </style>
        """.strip()


class ValidationSuite(object):
    import pyspark

    def __init__(self, name: str) -> None:
        from dbacademy.dbhelper.validations.validation_class import Validation

        self.name = name
        self.ids = set()
        self.test_cases: List[Validation] = list()

    @lazy_property
    def test_results(self) -> List[ValidationResult]:
        return self.run_tests()

    def run_tests(self) -> List[ValidationResult]:
        from dbacademy.dbhelper.validations.validation_results_aggregator_class import ValidationResultsAggregator
        from dbacademy.dbhelper.validations.validation_result_class import ValidationResult

        failed_tests = set()
        test_results = list()

        for test in self.test_cases:
            skip = any(test_id in failed_tests for test_id in test.depends_on)
            test.update_hint()
            result = ValidationResult(test, skip)

            if not result.passed and test.test_case_id is not None:
                failed_tests.add(test.test_case_id)

            test_results.append(result)
            ValidationResultsAggregator.update(result)

        return test_results

    def _display(self, css_class: str = "results") -> None:
        from html import escape
        from dbacademy import dbgems
        lines = [TEST_RESULTS_STYLE,
                 "<table class='" + css_class + "'>",
                 "  <tr><th class='points'>Points</th><th class='test'>Test</th><th class='result'>Result</th></tr>"]

        # noinspection PyTypeChecker
        for result in self.test_results:
            description_html = escape(str(result.test.description)) if result.test.escape_html else str(result.test.description)
            lines.append(f"<tr>")
            lines.append(f"  <td class='points'>{str(result.points)}</td>")
            lines.append(f"  <td class='test'>")
            lines.append(f"    {description_html}")

            if result.status == "failed" and result.test.hint:
                hint_html = escape(str(result.test.hint)) if result.test.escape_html else str(result.test.hint)
                lines.append(f"  <div class='note'>Hint: {hint_html}</div>")

            if result.message:
                lines.append(f"    <hr/>")
                lines.append(f"    <div class='message'>{str(result.message)}</div>")

            lines.append(f"  </td>")
            lines.append(f"  <td class='result {result.status}'></td>")
            lines.append(f"</tr>")

        lines.append("  <caption class='points'>Score: " + str(self.score) + "</caption>")
        lines.append("</table>")
        html = "\n".join(lines)
        dbgems.display_html(html)

    def display_results(self) -> None:
        self._display(css_class="results")

    def grade(self) -> int:
        self._display("grade")
        # noinspection PyTypeChecker
        return self.score

    @lazy_property
    def score(self) -> int:
        # noinspection PyTypeChecker
        return sum(map(lambda result: result.points, self.test_results))

    @lazy_property
    def max_score(self) -> int:
        # noinspection PyTypeChecker
        return sum(map(lambda result: result.test.points, self.test_results))

    @lazy_property
    def percentage(self) -> int:
        # noinspection PyTypeChecker
        return 0 if self.max_score == 0 else int(100.0 * self.score / self.max_score)

    @lazy_property
    def passed(self) -> bool:
        return self.percentage == 100

    def last_test_id(self) -> bool:
        return "-n/a-" if len(self.test_cases) == 0 else self.test_cases[-1].test_case_id

    def add_test(self, test_case: Validation):
        assert test_case.test_case_id is not None, "The test_case_id must be specified"
        assert test_case.test_case_id not in self.ids, f"Duplicate test case id: {test_case.test_case_id}"

        self.test_cases.append(test_case)
        self.ids.add(test_case.test_case_id)
        return self

    def test(self, test_function: Callable[[], Any], actual_value: Callable[[], Any], description: str, *, test_case_id: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy.dbhelper.validations.validation_class import Validation

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=actual_value,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint,
                                        test_function=test_function))

    def test_equals(self, actual_value: Callable[[], Any], expected_value: Any, description: str, *, test_case_id: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy.dbhelper.validations.validation_class import Validation

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=actual_value,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint,
                                        test_function=lambda: actual_value() == expected_value))

    def test_true(self, actual_value: Callable[[], bool], description: str, *, test_case_id: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy.dbhelper.validations.validation_class import Validation

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=actual_value,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint,
                                        test_function=lambda: actual_value() is True))

    def test_false(self, actual_value: Callable[[], bool], description: str, *, test_case_id: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy.dbhelper.validations.validation_class import Validation

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=actual_value,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint,
                                        test_function=lambda: actual_value() is False))

    def test_is_none(self, actual_value: Callable[[], Any], description: str, *, test_case_id: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy.dbhelper.validations.validation_class import Validation

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=actual_value,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint,
                                        test_function=lambda: actual_value() is None))

    def test_not_none(self, actual_value: Callable[[], Any], description: str, *, test_case_id: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy.dbhelper.validations.validation_class import Validation

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=actual_value,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint,
                                        test_function=lambda: actual_value() is not None))

    def test_length(self, actual_value: Callable[[], Sized], expected_length: int, description: str, *, test_case_id: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy.dbhelper.validations.validation_class import Validation

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=actual_value,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint,
                                        test_function=lambda: len(actual_value()) == expected_length))

    def fail_pre_req(self, *, test_case_id: str, e: Exception, depends_on: Iterable[str] = None):
        self.fail(test_case_id=test_case_id,
                  points=1,
                  depends_on=depends_on,
                  escape_html=False,
                  description=f"""<div>Execute prerequisites.</div><div style='max-width: 1024px; overflow-x:auto'>{e}</div>""")

    def fail(self, description: str, *, test_case_id: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy.dbhelper.validations.validation_class import Validation

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=lambda: False,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint,
                                        test_function=lambda: False))

    def test_floats(self, actual_value: Callable[[], float], expected_value: Any, description: str, *, test_case_id: str = None, tolerance=0.01, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy.dbhelper.validations.validation_class import Validation

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=actual_value,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint,
                                        test_function=lambda: self.compare_floats(actual_value(), expected_value, tolerance)))

    def test_rows(self, actual_value: Callable[[], pyspark.sql.Row], expected_value: pyspark.sql.Row, description: str, *, test_case_id: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy.dbhelper.validations.validation_class import Validation

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=actual_value,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint,
                                        test_function=lambda: self.compare_rows(actual_value(), expected_value)))

    def test_data_frames(self, actual_value: Callable[[], pyspark.sql.DataFrame], expected_value: pyspark.sql.DataFrame, description: str, *, test_case_id: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy.dbhelper.validations.validation_class import Validation

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=actual_value,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint,
                                        test_function=lambda: self.compare_data_frames(actual_value(), expected_value)))

    def test_contains(self, actual_value: Callable[[], Any], expected_values: Iterable[Any], description: str, *, test_case_id: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy.dbhelper.validations.validation_class import Validation

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=actual_value,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint,
                                        test_function=lambda: actual_value() in expected_values))

    def test_sequence(self,
                      actual_value: Callable[[], List[Any]],
                      expected_value: List[Any],
                      test_column_order: bool,
                      description: str,
                      *,
                      test_case_id: str = None,
                      points: int = 1,
                      depends_on: Iterable[str] = None,
                      escape_html: bool = False,
                      hint=None):

        from dbacademy.dbhelper.validations.validation_class import Validation

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=actual_value,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint,
                                        test_function=lambda: self.compare_lists(actual_value(), expected_value, test_column_order=test_column_order)))

    def test_schema_field(self,
                          struct_type: Callable[[], pyspark.sql.types.StructType],
                          expected_name: str,
                          expected_type: str,
                          expected_nullable: str = None,
                          *,
                          description: str = None, test_case_id: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):

        from dbacademy.dbhelper.validations.validation_class import Validation

        def actual_value() -> str:
            schema = struct_type()
            fields = [f for f in schema.fields if f.name == expected_name]
            field = None if len(fields) == 0 else fields[0]

            type_value = str(type(field.dataType)).replace("<class 'pyspark.sql.types.", "").replace("'>", "")
            if expected_nullable is None:
                return None if field is None else ", ".join([str(field.name), type_value, str(field.nullable)])
            else:
                return None if field is None else ", ".join([str(field.name), type_value])

        def test_schema() -> bool:
            schema = struct_type()
            fields = [f for f in schema.fields if f.name == expected_name]
            field = None if len(fields) == 0 else fields[0]

            return field is not None and field.name == expected_name and str(type(field.dataType)) == f"<class 'pyspark.sql.types.{expected_type}'>" and (expected_nullable is None or field.nullable == expected_nullable)

        if description is None:
            if expected_nullable is None:
                description = f"Schema contains \"{expected_name}\" of type {expected_type}"
            else:
                description = f"Schema contains \"{expected_name}\" of type {expected_type} (nullable={expected_nullable})"

        return self.add_test(Validation(suite=self,
                                        test_case_id=test_case_id,
                                        description=description,
                                        actual_value=actual_value,
                                        depends_on=depends_on,
                                        escape_html=escape_html,
                                        points=points,
                                        hint=hint or "Found [[ACTUAL_VALUE]]",
                                        test_function=test_schema))

    def testing(self):
        pass

    @staticmethod
    def compare_lists(value_a: List[Any],
                      value_b: List[Any],
                      test_column_order: bool):

        if value_a is None and value_b is None:
            return True

        if value_a is None or value_b is None:
            return False

        if len(value_a) != len(value_b):
            return False

        if test_column_order:
            return value_a == value_b

        for column in value_a:
            if column not in value_b:
                return False

        for column in value_b:
            if column not in value_a:
                return False

        return True

    @staticmethod
    def compare_floats(value_a: float, value_b: float, tolerance: float = 0.01):
        try:
            if value_a is None and value_b is None:
                return True
            else:
                return abs(float(value_a) - float(value_b)) <= tolerance
        except:
            return False

    @staticmethod
    def compare_rows(row_a: pyspark.sql.Row, row_b: pyspark.sql.Row):
        if row_a is None and row_b is None:
            return True
        elif row_a is None or row_b is None:
            return False

        return row_a.asDict() == row_b.asDict()

    @staticmethod
    def compare_schemas(schema_a: pyspark.sql.types.StructType, schema_b: pyspark.sql.types.StructType, test_column_order: bool):
        from pyspark.sql.types import StructField

        if schema_a is None and schema_b is None:
            return True

        if schema_a is None or schema_b is None:
            return False

        sch_a = [StructField(s.name, s.dataType, nullable=True) for s in schema_a]
        sch_b = [StructField(s.name, s.dataType, nullable=True) for s in schema_b]

        if test_column_order:
            return [sch_a] == [sch_b]
        else:
            return set(sch_a) == set(sch_b)

    @staticmethod
    def compare_data_frames(df_a: pyspark.sql.DataFrame, df_b: pyspark.sql.DataFrame):
        from functools import reduce

        if df_a is None and df_b is None:
            return True
        else:
            n = df_a.count()

            if n != df_b.count():
                return False

            kv1 = df_a.rdd.zipWithIndex().map(lambda t: (t[1], t[0])).collectAsMap()
            kv2 = df_b.rdd.zipWithIndex().map(lambda t: (t[1], t[0])).collectAsMap()

            kv12 = [kv1, kv2]
            d = {}

            for k in kv1.keys():
                d[k] = tuple(d[k] for d in kv12)

            return reduce(lambda a, b: a and b, [ValidationSuite.compare_rows(rowTuple[0], rowTuple[1]) for rowTuple in d.values()])

    @staticmethod
    def compare_row(row_a: pyspark.sql.Row, row_b: pyspark.sql.Row):
        if row_a is None and row_b is None:
            return True
        elif row_a is None or row_b is None:
            return False

        return row_a.asDict() == row_b.asDict()


class ValidationHelper:

    def __init__(self, da):
        from dbacademy.common import validate
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper
        from dbacademy.clients.databricks import DBAcademyRestClient

        self.__da = validate.any_value(da=da, parameter_type=DBAcademyHelper, required=True)
        self.__client = validate.any_value(client=da.client, parameter_type=DBAcademyRestClient, required=True)

    # @property
    # def da(self) -> DBAcademyHelper:
    #     return self.__da

    @property
    def client(self) -> DBAcademyRestClient:
        return self.__client

    # noinspection PyMethodMayBeStatic
    def new(self, name) -> ValidationSuite:
        return ValidationSuite(name)

    @staticmethod
    def monkey_patch(function_ref, delete=True):
        """
        This function "monkey patches" the specified function to the TestHelper class. While not 100% necessary,
        this pattern does allow each function to be defined in its own cell which makes authoring notebooks a little easier.
        """
        import inspect

        signature = inspect.signature(function_ref)
        assert "self" in signature.parameters, f"""Missing the required parameter "self" in the function "{function_ref.__name__}()" """

        setattr(ValidationHelper, function_ref.__name__, function_ref)

        return None if delete else function_ref
