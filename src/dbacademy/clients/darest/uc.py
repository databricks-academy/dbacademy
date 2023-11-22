__all__ = ["UcApi"]

from typing import Optional
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class MetastoresApi(ApiContainer):
    def __init__(self, client: ApiClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.1/unity-catalog/metastores"

    def list_metastores(self):
        return self.client.api("GET", self.base_url)

    def create_metastore(self):
        # return self.client.api("PATCH", f"{self.base_url}")
        raise Exception("Not implemented")

    def get_metastore_by_id(self, object_id: str):
        return self.client.api("GET", f"{self.base_url}/{object_id}")

    def update_metastore(self, object_id: str):
        # return self.client.api("PATCH", f"{self.base_url}/{object_id}")
        raise Exception(f"Not implemented for {object_id}")

    def delete_metastore_by_id(self, object_id: str):
        return self.client.api("DELETE", f"{self.base_url}/{object_id}")


class WorkspaceApi(ApiContainer):
    def __init__(self, client: ApiClient, workspace_id: str):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore"

    def assign_metastore(self, catalog_name, metastore_id) -> dict:
        payload = {
            "default_catalog_name": catalog_name,
            "metastore_id": metastore_id
        }
        return self.client.api("PUT", self.base_url, payload)

    def update_assignment(self, default_catalog_name: Optional[str], metastore_id: Optional[str]):
        payload = dict()

        if metastore_id is not None:
            payload["metastore_id"] = metastore_id

        if default_catalog_name is not None:
            payload["default_catalog_name"] = default_catalog_name

        return self.client.api("PATCH", self.base_url, payload)

    def delete_assignment_id(self):
        return self.client.api("DELETE", self.base_url)


class UcApi(ApiContainer):
    def __init__(self, client: ApiClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.1/unity-catalog"

    @property
    def metastores(self) -> MetastoresApi:
        return MetastoresApi(self.client)

    def workspace(self, workspace_id: str) -> WorkspaceApi:
        return WorkspaceApi(self.client, workspace_id)

    def metastore_summary(self):
        return self.client.api("GET", f"{self.base_url}/metastore_summary")

    def get_current_metastore_assignment(self):
        return self.client.api("GET", f"{self.base_url}/current-metastore-assignment")
