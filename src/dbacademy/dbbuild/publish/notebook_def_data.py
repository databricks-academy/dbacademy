__all__ = ["NotebookDefData"]

from typing import Union, List, Dict, Any, Optional
from dbacademy.common import validate
from dbacademy.clients.darest import DBAcademyRestClient
from dbacademy.dbbuild.publish.notebook_logger import NotebookLogger


class NotebookDefData:
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

        self.__client = validate(client=client).required.as_type(DBAcademyRestClient)

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

    @property
    def client(self) -> DBAcademyRestClient:
        return self.__client

    @property
    def path(self) -> str:
        return self.__path

    @property
    def replacements(self) -> Dict[str, Any]:
        return self.__replacements

    @replacements.setter
    def replacements(self, replacements: Dict[str, Any]) -> None:
        self.__replacements = validate(replacements=replacements).required.dict(str)

    @property
    def include_solution(self) -> bool:
        return self.__include_solution

    @include_solution.setter
    def include_solution(self, include_solution: bool) -> None:
        self.__include_solution = validate(include_solution=include_solution).required.bool()

    @property
    def test_round(self) -> int:
        return self.__test_round

    @test_round.setter
    def test_round(self, test_round: int) -> None:
        self.__test_round = validate(test_round=test_round).required.int()

    @property
    def ignored(self) -> bool:
        return self.__ignored

    @ignored.setter
    def ignored(self, ignored: bool) -> None:
        self.__ignored = validate(ignored=ignored).required.bool()

    @property
    def order(self) -> int:
        return self.__order

    @order.setter
    def order(self, order: int) -> None:
        self.__order = validate(order=order).required.int()

    @property
    def i18n(self) -> bool:
        return self.__i18n

    @i18n.setter
    def i18n(self, i18n: bool) -> None:
        self.__i18n: bool = validate(i18n=i18n).required.bool()

    @property
    def i18n_language(self) -> Optional[str]:
        return self.__i18n_language

    @i18n_language.setter
    def i18n_language(self, i18n_language: Optional[str]) -> None:
        self.__i18n_language = validate(i18n_language=i18n_language).str()

    @property
    def i18n_guids(self) -> List[str]:
        return self.__i18n_guids

    @property
    def ignoring(self) -> List[str]:
        return self.__ignoring

    @ignoring.setter
    def ignoring(self, ignored_errors: List[str]) -> None:
        self.__ignoring.clear()  # Remove any ignored errors because we are adding new ones now.
        self.__ignoring.extend(validate(ignored_errors=ignored_errors).required.list(str))

    @property
    def version(self) -> str:
        return self.__version
