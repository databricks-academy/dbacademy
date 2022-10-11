import unittest


class MyTestCase(unittest.TestCase):

    def test_module(self):
        pass
        # import sys
        # from pkgutil import iter_modules
        #
        # module = sys.modules["dbacademy"]
        # return None

    def test_dbpublish(self):
        from dbacademy_courseware.dbpublish.notebook_def_class import NotebookDef
        notebook = NotebookDef(build_config=None,
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
        from dbacademy_courseware.dbtest.results_evaluator import ResultsEvaluator
        results_evaluator = ResultsEvaluator([], False)
        self.assertIsNotNone(results_evaluator)


if __name__ == '__main__':
    unittest.main()
