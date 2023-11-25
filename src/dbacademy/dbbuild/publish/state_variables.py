__all__ = ["StateVariables"]

from typing import List, Dict
from dbacademy.common import validate


class StateVariables:
    def __init__(self):
        self.__i18n_guid_map: Dict[str, str] = dict()

        # Stateful values that get reset during publishing.
        self.__students_commands = list()
        self.__solutions_commands = list()

        # Reset counters
        self.__todo_count = 0
        self.__answer_count = 0
        self.__skipped = 0

        # Reset header flags
        self.__include_header = False
        self.__found_header_directive = False

        # Reset footer flags
        self.__include_footer = False
        self.__found_footer_directive = False

    @property
    def i18n_guid_map(self) -> Dict[str, str]:
        return self.__i18n_guid_map

    @i18n_guid_map.setter
    def i18n_guid_map(self, i18n_guid_map: Dict[str, str]) -> None:
        self.__i18n_guid_map = validate(i18n_guid_map=i18n_guid_map).required.dict(str, str)

    @property
    def students_commands(self) -> List[str]:
        return self.__students_commands

    @property
    def solutions_commands(self) -> List[str]:
        return self.__solutions_commands

    @property
    def todo_count(self) -> int:
        return self.__todo_count

    @todo_count.setter
    def todo_count(self, todo_count: int) -> None:
        self.__todo_count = validate(todo_count=todo_count).required.int()

    @property
    def answer_count(self) -> int:
        return self.__answer_count

    @answer_count.setter
    def answer_count(self, answer_count: int) -> None:
        self.__answer_count = validate(answer_count=answer_count).required.int()

    @property
    def skipped(self) -> int:
        return self.__skipped

    @skipped.setter
    def skipped(self, skipped: int) -> None:
        self.__skipped = validate(skipped=skipped).required.int()

    @property
    def include_header(self) -> bool:
        return self.__include_header

    @include_header.setter
    def include_header(self, include_header: bool) -> None:
        self.__include_header = validate(include_header=include_header).required.bool()

    @property
    def found_header_directive(self) -> bool:
        return self.__found_header_directive

    @found_header_directive.setter
    def found_header_directive(self, found_header_directive: bool) -> None:
        self.__found_header_directive = validate(found_header_directive=found_header_directive).required.bool()

    @property
    def include_footer(self) -> bool:
        return self.__include_footer

    @include_footer.setter
    def include_footer(self, include_footer: bool) -> None:
        self.__include_footer = validate(include_footer=include_footer).required.bool()

    @property
    def found_footer_directive(self) -> bool:
        return self.__found_footer_directive

    @found_footer_directive.setter
    def found_footer_directive(self, found_footer_directive: bool) -> None:
        self.__found_footer_directive = validate(found_footer_directive=found_footer_directive).required.bool()
