class Stub:

    # noinspection PyPep8Naming
    def getDbutils(self):
        return self


class MockDBUtils:
    def __init__(self):
        self.fs = None
        self.widgets = None
        self.notebook = None
        self.entry_point = Stub()

    # noinspection PyPep8Naming
    def displayHTML(self, **kwargs): pass

    # noinspection PyPep8Naming
    def display(self, **kwargs): pass
