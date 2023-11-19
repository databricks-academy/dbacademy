__all__ = ["NotebookDefData"]

from typing import Union, List, Dict, Any, Optional

from dbacademy.clients.databricks import DBAcademyRestClient
from dbacademy.dbbuild.build_config_class import BuildConfig
from dbacademy.dbbuild.publish.notebook_logger import NotebookLogger


class NotebookDefData:
    def __init__(self,
                 *,
                 build_config: BuildConfig,
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

        from dbacademy.common import validator

        assert type(build_config) == BuildConfig, f"""Expected the parameter "build_config" to be of type "BuildConfig", found "{type(build_config)}" """
        assert type(path) == str, f"""Expected the parameter "path" to be of type "str", found "{type(path)}" """
        assert type(replacements) == dict, f"""Expected the parameter "replacements" to be of type "dict", found "{type(replacements)}" """
        assert type(include_solution) == bool, f"""Expected the parameter "include_solution" to be of type "bool", found "{type(include_solution)}" """

        self.__logger: NotebookLogger = NotebookLogger()

        self.__build_config = validate.any_value_required(build_config=build_config, parameter_type=BuildConfig)
        self.__client = self.build_config.client

        self.__replacements = validate.dict_of_key_str_required(replacements=replacements, auto_create=True)
        self.__ignoring = validate.list_of_strings_required(ignoring=ignoring, auto_create=True)

        self.__version = validate.str_value_required(version=version)
        self.__path = validate.str_value_required(path=path)
        self.__include_solution = validate.bool_value_required(include_solution=include_solution)
        self.__test_round = validate.int_value_required(test_round=test_round)
        self.__ignored = validate.bool_value_required(ignored=ignored)
        self.__order = validate.int_value_required(order=order)

        self.__i18n: bool = validate.bool_value_required(i18n=i18n)
        self.__i18n_language: Union[None, str] = validate.str_value(i18n_language=i18n_language, required=False)
        self.__i18n_guids: List[str] = list()  # Defaults to an empty list

    @property
    def logger(self) -> NotebookLogger:
        return self.__logger

    @property
    def build_config(self) -> BuildConfig:
        return self.__build_config

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
