__all__ = ["WorkspacesClient"]

from typing import Dict, Any, List, Optional
from dbacademy.clients.rest.common import ApiContainer


class WorkspacesClient(ApiContainer):
    from dbacademy.clients.rest.common import ApiClient

    def __init__(self, client: ApiClient, account_id: str):
        super().__init__(client)

        self.account_id = account_id
        self.client = client
        self.base_url = f"{self.client.url}/api/2.0/accounts/{self.account_id}/workspace"

    def list(self) -> Optional[List[Dict[str, Any]]]:
        return self.client.api("GET", f"{self.base_url}")
