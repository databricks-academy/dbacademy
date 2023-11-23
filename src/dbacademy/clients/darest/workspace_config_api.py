__all__ = ["WorkspaceConfigApi"]

from typing import Dict, Any
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class WorkspaceConfigApi(ApiContainer):
    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        self.base_url = f"{self.__client.endpoint}/api/2.0/workspace-conf"

    def get_config(self, *property_names) -> Dict[str, Any]:
        properties = list()

        for property_name in property_names:
            if type(property_name) == type({}.keys()):
                properties.extend(list(property_name))
            elif type(property_name) == list:
                properties.extend(property_name)
            else:
                properties.append(str(property_name))

        keys = ",".join(properties)
        return self.__client.api("GET", self.base_url, keys=keys)

    def patch_config(self, config: Dict[str, Any]) -> None:

        params = dict()
        for key, value in config.items():
            if type(value) == bool:
                value = str(value).lower()
            params[key] = str(value)

        self.__client.api("PATCH", self.base_url, _expected=204, _data=params)
        return None
