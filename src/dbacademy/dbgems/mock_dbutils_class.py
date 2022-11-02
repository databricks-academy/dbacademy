class MockOptional:
    def __init__(self, value: str):
        self.value = value

    # noinspection PyPep8Naming
    def getOrElse(self, default_value: str):
        return self.value or default_value


class MockFileSystem:
    def __init__(self):
        pass


class MockEntryPoint:
    # noinspection PyPep8Naming
    @staticmethod
    def getDbutils() -> "MockDBUtils":
        # tags = dbutils.entry_point.getDbutils().notebook().getContext().tags()
        return MockDBUtils()


class MockWidgets:
    def __init__(self):
        pass


class MockNotebook:
    # tags = dbutils.entry_point.getDbutils().notebook().getContext().tags()
    # noinspection PyPep8Naming
    @staticmethod
    def getContext() -> "MockContext":
        return MockContext()


class MockContext:
    @staticmethod
    def tags():
        return {
            "orgId": "mock-00",
            "clusterId": "mock-0",
        }

    # noinspection PyPep8Naming
    @staticmethod
    def notebookPath() -> MockOptional:
        # dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
        path = "/Repos/Temp/mock-course-source/Source/Version Info"
        return MockOptional(path)


class MockSecrets:
    def __init__(self):
        pass


class MockDBUtils:
    def __init__(self):
        self.fs = MockFileSystem()
        self.widgets = MockWidgets()
        self.secrets = MockSecrets()
        self.entry_point = MockEntryPoint()

    # noinspection PyPep8Naming
    def displayHTML(self, **kwargs):
        pass

    def display(self, **kwargs):
        pass

    @staticmethod
    def notebook() -> MockNotebook:
        return MockNotebook()
