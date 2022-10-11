class TestHelper:
    from dbacademy_helper import DBAcademyHelper

    def __init__(self, da: DBAcademyHelper):

        self.da = da
        self.client = da.client

    # noinspection PyMethodMayBeStatic
    def new(self, name):
        from dbacademy_helper.tests.test_suite_class import TestSuite
        return TestSuite(name)

    @staticmethod
    def monkey_patch(function_ref, delete=True):
        """
        This function "monkey patches" the specified function to the TestHelper class. While not 100% necessary,
        this pattern does allow each function to be defined in its own cell which makes authoring notebooks a little easier.
        """
        import inspect

        signature = inspect.signature(function_ref)
        assert "self" in signature.parameters, f"""Missing the required parameter "self" in the function "{function_ref.__name__}()" """

        setattr(TestHelper, function_ref.__name__, function_ref)

        return None if delete else function_ref


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


_TEST_RESULTS_STYLE = """
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
