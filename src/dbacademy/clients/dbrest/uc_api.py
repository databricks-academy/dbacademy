__all__ = ["UcApi"]
# Code Review: JDP on 11-26-2023

from typing import Optional, Dict, Any
from dbacademy.common import validate
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class MetastoresApi(ApiContainer):
    def __init__(self, client: ApiClient):
        self.__client = validate(client=client).required.as_type(ApiClient)
        self.base_url = f"{self.__client.endpoint}/api/2.1/unity-catalog/metastores"

    def list_metastores(self):
        return self.__client.api("GET", self.base_url)

    # def create_metastore(self):
    #     # return self.__client.api("PATCH", f"{self.base_url}")
    #     import inspect
    #     raise NotImplementedError(f"{self.__class__.__name__}.{inspect.stack()[0].function}(..) is not implemented")

    def get_metastore_by_id(self, object_id: str):
        return self.__client.api("GET", f"{self.base_url}/{validate(object_id=object_id).required.str()}")

    # def update_metastore(self, object_id: str):
    #     # return self.__client.api("PATCH", f"{self.base_url}/{object_id}")
    #     import inspect
    #     raise NotImplementedError(f"{self.__class__.__name__}.{inspect.stack()[0].function}(..) is not implemented")

    def delete_metastore_by_id(self, object_id: str):
        return self.__client.api("DELETE", f"{self.base_url}/{validate(object_id=object_id).required.str()}")


class WorkspaceApi(ApiContainer):

    def __init__(self, client: ApiClient):
        self.__client = validate(client=client).required.as_type(ApiClient)

    def __base_url(self, workspace_id: str) -> str:
        return f"{self.__client.endpoint}/api/2.1/unity-catalog/workspaces/{validate(workspace_id=workspace_id).required.str()}/metastore"

    def assign_metastore(self, workspace_id: str, catalog_name: str, metastore_id: str) -> Dict[str, Any]:
        payload = {
            "default_catalog_name": validate(catalog_name=catalog_name).required.str(),
            "metastore_id": validate(metastore_id=metastore_id).required.str()
        }
        return self.__client.api("PUT", self.__base_url(workspace_id), payload)

    def update_assignment(self, workspace_id: str, default_catalog_name: Optional[str], metastore_id: Optional[str]):
        payload = dict()

        if metastore_id is not None:
            payload["metastore_id"] = validate(metastore_id=metastore_id).required.str()

        if default_catalog_name is not None:
            payload["default_catalog_name"] = validate(default_catalog_name=default_catalog_name).required.str()

        return self.__client.api("PATCH", self.__base_url(workspace_id), payload)

    def delete_assignment_id(self, workspace_id: str, ):
        return self.__client.api("DELETE", self.__base_url(workspace_id))


class UcApi(ApiContainer):
    def __init__(self, client: ApiClient):
        self.__client = validate(client=client).required.as_type(ApiClient)
        self.base_url = f"{self.__client.endpoint}/api/2.1/unity-catalog"

    @property
    def metastores(self) -> MetastoresApi:
        return MetastoresApi(self.__client)

    @property
    def workspace(self) -> WorkspaceApi:
        return WorkspaceApi(client=self.__client)

    def metastore_summary(self):
        return self.__client.api("GET", f"{self.base_url}/metastore_summary")

    def get_current_metastore_assignment(self):
        return self.__client.api("GET", f"{self.base_url}/current-metastore-assignment")
