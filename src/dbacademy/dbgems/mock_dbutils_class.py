__all__ = ["MockNotebook", "MockOptional", "MockSecrets", "MockWidgets", "MockDBUtils", "MockEntryPointNotebook", "MockEntryPoint", "MockEntryPointDBUtils", "MockEntryPointContext", "MockFileSystem"]

from typing import Dict, Any


class MockOptional:
    def __init__(self, value: str):
        self.value = value

    # noinspection PyPep8Naming
    def getOrElse(self, default_value: str):
        return self.value or default_value


class MockFileSystem:
    def __init__(self):
        pass


class MockEntryPointContext:

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
        path = "/Repos/Examples/example-course-source/Source/Version Info"
        return MockOptional(path)


class MockEntryPointNotebook:
    # noinspection PyPep8Naming
    @staticmethod
    def getContext():
        return MockEntryPointContext()


class MockEntryPointDBUtils:
    @staticmethod
    def notebook():
        return MockEntryPointNotebook()


class MockEntryPoint:
    # noinspection PyPep8Naming
    @staticmethod
    def getDbutils() -> "MockEntryPointDBUtils":
        # tags = dbutils.entry_point.getDbutils().notebook().getContext().tags()
        return MockEntryPointDBUtils()


class MockWidgets:
    def __init__(self):
        pass


class MockNotebook:
    @staticmethod
    def run(path: str, timeout_seconds: int, arguments: Dict[str, Any]):
        pass


class MockSecrets:

    SECRETS = dict()

    def __init__(self):
        pass

    @classmethod
    def get(cls, scope: str, key: str) -> str:
        return cls.SECRETS.get(f"{scope}-{key}")


class MockDBUtils:
    def __init__(self):
        self.fs = MockFileSystem()
        self.widgets = MockWidgets()
        self.secrets = MockSecrets()
        self.entry_point = MockEntryPoint()

        # Supports dbutils.notebook
        self.notebook = MockNotebook()

    # noinspection PyPep8Naming
    def displayHTML(self, **kwargs):
        pass

    def display(self, **kwargs):
        pass
