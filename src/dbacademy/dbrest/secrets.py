__all__ = ["SecretsClient"]

from typing import List, Any
from dbacademy.clients.rest.common import ApiContainer


class SecretsClient(ApiContainer):
    from dbacademy.dbrest import DBAcademyRestClient

    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/secrets"

    def scopes_list(self) -> List[str, Any]:
        response = self.client.api("GET", self.base_url)
        return response.get("endpoints", list())
