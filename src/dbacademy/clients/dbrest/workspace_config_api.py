__all__ = ["WorkspaceConfigApi"]
# Code Review: JDP on 11-26-2023

from typing import Dict, Any, Union, List, Iterable
from dbacademy.common import validate
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class WorkspaceConfigApi(ApiContainer):

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        self.__base_url = f"{self.__client.endpoint}/api/2.0/workspace-conf"

    def get_config(self, property_names: Union[str, Iterable[str]], *and_property_names: str) -> Dict[str, Any]:
        from dbacademy.common import combine_var_args

        properties: List[str] = combine_var_args(property_names, and_property_names)
        properties: List[str] = validate(properties=properties).required.list(str)
        return self.__client.api("GET", self.__base_url, keys=",".join(properties))

    def patch_config(self, config: Dict[str, Any]) -> None:

        params: Dict[str, str] = dict()

        for key, value in validate(config=config).required.dict(str).items():
            params[key] = str(value).lower()

        self.__client.api("PATCH", self.__base_url, _expected=204, _data=params)
