import unittest


class TestModuleAlias(unittest.TestCase):

    def test_module(self):
        pass
        # import sys
        # from pkgutil import iter_modules
        #
        # module = sys.modules["dbacademy"]
        # return None

    def test_dbpublish(self):
        from dbacademy.dbbuild import NotebookDef, BuildConfig
        notebook = NotebookDef(build_config=BuildConfig(name="TestBuildConfig"),
                               version="Unknown",
                               path="Agenda",
                               replacements={},
                               include_solution=False,
                               test_round=2,
                               ignored=False,
                               order=0,
                               i18n=True,
                               ignoring=[],
                               i18n_language="English")
        self.assertIsNotNone(notebook)

    def test_dbtest(self):
        from dbacademy.dbbuild import ResultsEvaluator
        results_evaluator = ResultsEvaluator([], False)
        self.assertIsNotNone(results_evaluator)


if __name__ == '__main__':
    unittest.main()
