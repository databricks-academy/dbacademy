__all__ = ["SecretsClient"]

from typing import List, Any, Dict, Literal
from dbacademy.clients.rest.common import ApiContainer


class SecretsClient(ApiContainer):
    from dbacademy.dbrest import DBAcademyRestClient

    SCOPE_BACKEND_TYPE = Literal["DATABRICKS", "AZURE_KEYVAULT"]

    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/secrets"

    def scopes_list(self) -> List[Dict[str, Any]]:
        response = self.client.api("GET", f"{self.base_url}/scopes/list")
        return response.get("scopes", list())

    def scopes_create(self, scope: str, initial_manage_principal: str = None, scope_backend_type: SCOPE_BACKEND_TYPE = "DATABRICKS") -> None:
        return self.client.api("POST", f"{self.base_url}/scopes/create", scope=scope, initial_manage_principal=initial_manage_principal, scope_backend_type=scope_backend_type)

    def scopes_delete(self, scope: str) -> None:
        return self.client.api("POST", f"{self.base_url}/scopes/delete", scope=scope)

    def secrets_list(self, scope: str) -> List[Dict[str, Any]]:
        response = self.client.api("GET", f"{self.base_url}/secrets/list", scope=scope)
        return response.get("secrets", list())
