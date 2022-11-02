class TestHelper:
    from ..dbacademy_helper_class import DBAcademyHelper
    from .testing_suite_class import TestSuite

    def __init__(self, da: DBAcademyHelper):

        self.da = da
        self.client = da.client

    # noinspection PyMethodMayBeStatic
    def new(self, name) -> TestSuite:
        from .testing_suite_class import TestSuite
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
