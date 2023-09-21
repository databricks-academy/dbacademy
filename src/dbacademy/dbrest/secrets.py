__all__ = ["SecretsClient"]

from typing import List, Any, Dict
from dbacademy.clients.rest.common import ApiContainer


class SecretsClient(ApiContainer):
    from dbacademy.dbrest import DBAcademyRestClient

    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/secrets"

    def scopes_list(self) -> List[Dict[str, Any]]:
        scopes = self.client.api("GET", f"{self.base_url}/scopes/list")
        return scopes or list()

    def secrets_list(self, scope: str) -> List[Dict[str, Any]]:
        secrets = self.client.api("GET", f"{self.base_url}/secrets/list", scope=scope)
        return secrets or list()
