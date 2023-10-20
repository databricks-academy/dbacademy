__all__ = ["SecretsClient"]

from typing import List, Any, Dict, Literal, Optional
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class _ScopesClient(ApiContainer):

    SCOPE_BACKEND_TYPE = Literal["DATABRICKS", "AZURE_KEYVAULT"]

    def __init__(self, client: ApiClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/secrets/scopes"

    def list(self) -> List[Dict[str, Any]]:
        response = self.client.api("GET", f"{self.base_url}/list")
        return response.get("scopes", list())

    def get_by_name(self, scope: str) -> Optional[Dict[str, Any]]:
        for s in self.list():
            if s.get("name") == scope:
                return s

        return None

    def create(self, scope: str, initial_manage_principal: str = None, scope_backend_type: SCOPE_BACKEND_TYPE = "DATABRICKS") -> Dict[str, Any]:
        existing = self.get_by_name(scope)
        if existing is not None:
            return existing
        else:
            self.client.api("POST", f"{self.base_url}/create", scope=scope, initial_manage_principal=initial_manage_principal, scope_backend_type=scope_backend_type)
            return self.get_by_name(scope)

    def delete_by_name(self, scope: str) -> None:
        return self.client.api("POST", f"{self.base_url}/delete", scope=scope)


class _AclsClient(ApiContainer):

    SCOPE_PERMISSIONS = Literal["READ", "WRITE", "MANAGE"]

    def __init__(self, client: ApiClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/secrets/acls"

    def list(self, scope: str) -> List[Dict[str, Any]]:
        response = self.client.api("GET", f"{self.base_url}/list", scope=scope)
        return response.get("scopes", list())

    def get_by_name(self, scope: str, principal: str) -> Optional[Dict[str, Any]]:
        response = self.client.api("GET", f"{self.base_url}/list", scope=scope, principal=principal)
        return response.get("scopes")

    def create_or_update(self, scope: str, principal: str, permission: SCOPE_PERMISSIONS) -> None:
        self.client.api("POST", f"{self.base_url}/put", scope=scope, principal=principal, permission=permission)
        return None

    def delete_by_name(self, scope: str, principal: str) -> None:
        return self.client.api("POST", f"{self.base_url}/delete", scope=scope, principal=principal)


class SecretsClient(ApiContainer):

    def __init__(self, client: ApiClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/secrets"
        self.scopes = _ScopesClient(client)
        self.acls = _AclsClient(client)

    def list(self, scope: str) -> List[Dict[str, Any]]:
        response = self.client.api("GET", f"{self.base_url}/list", scope=scope)
        return response.get("secrets", list())

    def get_by_name(self, scope: str, key: str) -> Optional[Dict[str, Any]]:
        return self.client.api("GET", f"{self.base_url}/get", scope=scope, key=key, _expected=[200, 404])

    def create(self, scope: str, key: str, string_value: str, bytes_value: bytes = None) -> Dict[str, Any]:
        if bytes_value is not None:
            raise Exception("bytes_values is currently not supported")

        if self.get_by_name(scope, key) is not None:
            self.delete(scope, key)

        self.client.api("POST", f"{self.base_url}/put", scope=scope, key=key, string_value=string_value)
        return self.get_by_name(scope, key)

    def delete(self, scope: str, key: str) -> None:
        return self.client.api("POST", f"{self.base_url}/delete", scope=scope, key=key)
