from typing import Union, Dict, Any, List, Optional
from dbacademy.dbrest.client import DBAcademyRestClient
from dbacademy.clients.rest.common import ApiContainer


class WorkspacesClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient, account_id: str):
        super().__init__(client)

        self.account_id = account_id
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/accounts/{self.account_id}/workspace"

    def list(self) -> Optional[List[Dict[str, Any]]]:
        return self.client.api("GET", f"{self.base_url}")
