__all__ = ["ValidationResultsAggregator"]

from typing import Dict
from dbacademy.dbhelper.validations.validation_result_class import ValidationResult


class __ValidationResultsAggregator(object):

    test_results: Dict[str, ValidationResult] = dict()

    def update(self, result: ValidationResult):
        self.test_results[result.test.test_case_id] = result
        return result

    @property
    def score(self) -> int:
        return sum(map(lambda result: result.points, self.test_results.values()))

    @property
    def max_score(self) -> int:
        return sum(map(lambda result: result.test.points, self.test_results.values()))

    @property
    def percentage(self) -> int:
        return 0 if self.max_score == 0 else int(100.0 * self.score / self.max_score)

    @property
    def passed(self) -> bool:
        return self.percentage == 100

    def display_results(self):
        from dbacademy import dbgems
        from dbacademy.dbhelper.validations import TEST_RESULTS_STYLE

        dbgems.display_html(TEST_RESULTS_STYLE + f"""
    <table class='results'>
      <tr><th colspan="2">Test Summary</th></tr>
      <tr><td>Number of Passing Tests</td><td style="text-align:right">{self.score}</td></tr>
      <tr><td>Number of Failing Tests</td><td style="text-align:right">{self.max_score - self.score}</td></tr>
      <tr><td>Percentage Passed</td><td style="text-align:right">{self.percentage}%</td></tr>
    </table>
    """)


# Lazy-man's singleton
ValidationResultsAggregator = __ValidationResultsAggregator()
