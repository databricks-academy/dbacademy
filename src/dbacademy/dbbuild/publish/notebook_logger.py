__all__ = ["NotebookError", "NotebookLogger"]

from typing import Callable, List
from dbacademy.common import validate


class NotebookError:
    def __init__(self, message: str):
        self.__message = validate(message=message).required.str()

    @property
    def message(self) -> str:
        return self.__message

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.message


class NotebookLogger:

    def __init__(self):
        self.__errors: List[NotebookError] = list()
        self.__warnings: List[NotebookError] = list()

    @property
    def errors(self) -> List[NotebookError]:
        return self.__errors

    @property
    def warnings(self) -> List[NotebookError]:
        return self.__warnings

    def reset(self):
        self.__errors = list()
        self.__warnings = list()

    def warn(self, assertion: Callable[[], bool], message: str) -> bool:
        if assertion is None or not assertion():
            self.warnings.append(NotebookError(message))
            return False
        else:
            return True

    def test(self, assertion: Callable[[], bool], message: str) -> bool:
        if assertion is None or not assertion():
            self.errors.append(NotebookError(message))
            return False
        else:
            return True
