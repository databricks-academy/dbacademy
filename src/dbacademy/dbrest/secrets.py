__all__ = ["SecretsClient"]

from typing import List, Any, Dict, Literal, Optional
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

    def scopes_get_by_name(self, scope) -> Optional[Dict[str, Any]]:
        for scope_data in self.scopes_list():
            if scope_data.get("name") == scope:
                return scope

        return None

    def scopes_create(self, scope: str, initial_manage_principal: str = None, scope_backend_type: SCOPE_BACKEND_TYPE = "DATABRICKS") -> Dict[str, Any]:
        existing_scope = self.scopes_get_by_name(scope)
        if existing_scope is not None:
            return existing_scope
        else:
            self.client.api("POST", f"{self.base_url}/scopes/create", scope=scope, initial_manage_principal=initial_manage_principal, scope_backend_type=scope_backend_type)
            return self.scopes_get_by_name(scope)

    def scopes_delete(self, scope: str) -> None:
        return self.client.api("POST", f"{self.base_url}/scopes/delete", scope=scope)

    def secrets_list(self, scope: str) -> List[Dict[str, Any]]:
        response = self.client.api("GET", f"{self.base_url}/secrets/list", scope=scope)
        return response.get("secrets", list())
