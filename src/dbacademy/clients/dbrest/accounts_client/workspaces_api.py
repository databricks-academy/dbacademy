__all__ = ["WorkspacesApi"]

from typing import Dict, Any, List, Optional
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class WorkspacesApi(ApiContainer):

    def __init__(self, client: ApiClient, account_id: str):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        self.__account_id = validate(account_id=account_id).required.str()

        self.__base_url = f"{self.__client.endpoint}/api/2.0/accounts/{self.account_id}/workspaces"

    @property
    def account_id(self) -> str:
        return self.__account_id

    def list(self) -> Optional[List[Dict[str, Any]]]:
        return self.__client.api("GET", f"{self.__base_url}")

    def get_by_id(self, workspace_id: str) -> Dict[str, Any]:
        return self.__client.api("GET", f"{self.__base_url}/{workspace_id}")

    def get_by_name(self, name: str):
        for workspace in self.list():
            if name == workspace.get("workspace_name"):
                return workspace
