__all__ = ["WorkspaceConfigClient"]

from typing import Dict, Any
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class WorkspaceConfigClient(ApiContainer):
    def __init__(self, client: ApiClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/workspace-conf"

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
        return self.client.api("GET", self.base_url, keys=keys)

    def patch_config(self, config: Dict[str, Any]) -> None:

        params = dict()
        for key, value in config.items():
            if type(value) == bool:
                value = str(value).lower()
            params[key] = str(value)

        self.client.api("PATCH", self.base_url, _expected=204, _data=params)
        return None
