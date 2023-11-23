__all__ = ["TokenManagementApi"]

from typing import Optional, List, Dict, Any
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class TokenManagementApi(ApiContainer):
    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        self.base_url = f"{self.__client.endpoint}/api/2.0/token-management"

    def create_on_behalf_of_service_principal(self, application_id: str, comment: str, lifetime_seconds: Optional[int]) -> Dict[str, Any]:
        params = {
            "application_id": application_id,
            "comment": comment,
            "lifetime_seconds": lifetime_seconds
        }
        return self.__client.api("POST", f"{self.base_url}/on-behalf-of/tokens", params)

    def list(self) -> List[Dict[str, Any]]:
        results = self.__client.api("GET", f"{self.base_url}/tokens")
        return results.get("token_infos", list())

    def delete_by_id(self, token_id: str) -> None:
        self.__client.api("DELETE", f"{self.base_url}/tokens/{token_id}", _expected=(200, 404))
        return None

    def get_by_id(self, token_id: str) -> Dict[str, Any]:
        return self.__client.api("GET", f"{self.base_url}/tokens/{token_id}")
