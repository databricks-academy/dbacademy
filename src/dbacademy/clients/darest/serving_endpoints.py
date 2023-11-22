__all__ = ["ServingEndpointsApi"]

from typing import List, Dict, Any
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class ServingEndpointsApi(ApiContainer):
    def __init__(self, client: ApiClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/serving-endpoints"

    def list(self) -> List[Dict[str, Any]]:
        response = self.client.api("GET", self.base_url)
        return response.get("endpoints", list())

    def get_by_name(self, name):
        return self.client.api("GET", f"{self.base_url}/{name}")

    def delete_by_name(self, name):
        self.client.api("DELETE", f"{self.base_url}/{name}")
        return None
