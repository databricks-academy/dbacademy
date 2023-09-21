__all__ = ["WorkspacesClient"]

from typing import Dict, Any, List, Optional
from dbacademy.clients.rest.common import ApiContainer


class WorkspacesClient(ApiContainer):
    from dbacademy.clients.rest.common import ApiClient

    def __init__(self, client: ApiClient, account_id: str):
        self.account_id = account_id
        self.client = client
        self.base_url = f"{self.client.url}/2.0/accounts/{self.account_id}/workspaces"

    def list(self) -> Optional[List[Dict[str, Any]]]:
        return self.client.api("GET", f"{self.base_url}")

    def get_by_id(self, id: str) -> Dict[str, Any]:
        return self.client.api("GET", f"{self.base_url}/{id}")

    def get_by_name(self, name: str):
        for workspace in self.list():
            if name == workspace.get("workspace_name"):
                return workspace
