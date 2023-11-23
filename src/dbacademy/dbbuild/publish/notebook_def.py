__all__ = ["NotebookDef", "NotebookDefData", "StateVariables"]

from typing import Union, List, Dict, Any, Optional
from abc import ABC, abstractmethod

from dbacademy.clients.darest import DBAcademyRestClient
from dbacademy.dbbuild.publish.notebook_logger import NotebookLogger


class StateVariables:
    def __init__(self):
        self.i18n_guid_map: Dict[str, str] = dict()

        # Stateful values that get reset during publishing.
        self.students_commands = list()
        self.solutions_commands = list()

        # Reset counters
        self.todo_count = 0
        self.answer_count = 0
        self.skipped = 0

        # Reset header flags
        self.include_header = False
        self.found_header_directive = False

        # Reset footer flags
        self.include_footer = False
        self.found_footer_directive = False


class NotebookDefData(ABC):
    def __init__(self, *,
                 client: DBAcademyRestClient,
                 path: str,
                 replacements: Dict[str, Any],
                 include_solution: bool,
                 test_round: int,
                 ignored: bool,
                 order: int,
                 i18n: bool,
                 i18n_language: Union[None, str],
                 ignoring: List[str],
                 version: str):

        from dbacademy.common import validate

        self.__logger: NotebookLogger = NotebookLogger()

        # assert type(build_config) == BuildConfig, f"""Expected the parameter "build_config" to be of type "BuildConfig", found "{type(build_config)}" """
        # self.__build_config = validate(build_config=build_config).required.as_type(BuildConfig)

        self.__client = validate(client=client).required.as_type(DBAcademyRestClient)

        assert type(replacements) == dict, f"""Expected the parameter "replacements" to be of type "dict", found "{type(replacements)}" """
        self.__replacements = validate(replacements=replacements).dict(str, auto_create=True)

        self.__ignoring = validate(ignoring=ignoring).list(str, auto_create=True)

        self.__version = validate(version=version).required.str()

        assert type(path) == str, f"""Expected the parameter "path" to be of type "str", found "{type(path)}" """
        self.__path = validate(path=path).required.str()

        assert type(include_solution) == bool, f"""Expected the parameter "include_solution" to be of type "bool", found "{type(include_solution)}" """
        self.__include_solution = validate(include_solution=include_solution).required.bool()

        self.__test_round = validate(test_round=test_round).required.int()
        self.__ignored = validate(ignored=ignored).required.bool()
        self.__order = validate(order=order).required.int()

        self.__i18n: bool = validate(i18n=i18n).required.bool()
        self.__i18n_language: Union[None, str] = validate(i18n_language=i18n_language).str()
        self.__i18n_guids: List[str] = list()  # Defaults to an empty list

    @property
    def logger(self) -> NotebookLogger:
        return self.__logger

    # @property
    # def build_config(self) -> BuildConfig:
    #     return self.__build_config

    @property
    def client(self) -> DBAcademyRestClient:
        return self.__client

    @property
    def path(self) -> str:
        return self.__path

    @property
    def replacements(self) -> Dict[str, Any]:
        return self.__replacements

    @property
    def include_solution(self) -> bool:
        return self.__include_solution

    @include_solution.setter
    def include_solution(self, include_solution: bool) -> None:
        self.__include_solution = include_solution

    @property
    def test_round(self) -> int:
        return self.__test_round

    @test_round.setter
    def test_round(self, test_round: int) -> None:
        self.__test_round = test_round

    @property
    def ignored(self) -> bool:
        return self.__ignored

    @ignored.setter
    def ignored(self, ignored: bool) -> None:
        self.__ignored = ignored

    @property
    def order(self) -> int:
        return self.__order

    @order.setter
    def order(self, order: int) -> None:
        self.__order = order

    @property
    def i18n(self) -> bool:
        return self.__i18n

    @i18n.setter
    def i18n(self, i18n: bool) -> None:
        self.__i18n: bool = i18n

    @property
    def i18n_language(self) -> Optional[str]:
        return self.__i18n_language

    @i18n_language.setter
    def i18n_language(self, i18n_language: Optional[str]) -> None:
        self.__i18n_language = i18n_language

    @property
    def i18n_guids(self) -> List[str]:
        return self.__i18n_guids

    @property
    def ignoring(self) -> List[str]:
        return self.__ignoring

    @property
    def version(self) -> str:
        return self.__version


class NotebookDef(NotebookDefData, ABC):

    def __init__(self, *,
                 client: DBAcademyRestClient,
                 path: str,
                 replacements: Dict[str, Any],
                 include_solution: bool,
                 test_round: int,
                 ignored: bool,
                 order: int,
                 i18n: bool,
                 i18n_language: Union[None, str],
                 ignoring: List[str],
                 version: str):

        super().__init__(client=client,
                         path=path,
                         replacements=replacements,
                         include_solution=include_solution,
                         test_round=test_round,
                         ignored=ignored,
                         order=order,
                         i18n=i18n,
                         i18n_language=i18n_language,
                         ignoring=ignoring,
                         version=version)

    @abstractmethod
    def publish(self,
                source_dir: str,
                target_dir: str,
                i18n_resources_dir: str,
                verbose: bool,
                debugging: bool,
                other_notebooks: List[NotebookDefData]) -> None:
        pass
