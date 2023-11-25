__all__ = ["ClustersPermissionsApi"]

from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients.dbrest.permissions_api.permission_crud_api import PermissionsCrudApi


class ClusterPoliciesPermissionsApi(PermissionsCrudApi):
    valid_permissions = [None, "CAN_USE"]

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        super().__init__(client, "/api/2.0/permissions/cluster-policies", "cluster_policy")


class ClustersPermissionsApi(PermissionsCrudApi):
    valid_permissions = ["CAN_ATTACH_TO", "CAN_RESTART", "CAN_MANAGE"]

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        super().__init__(client, "/api/2.0/permissions/clusters", "cluster")

    @property
    def policies(self) -> ClusterPoliciesPermissionsApi:
        return ClusterPoliciesPermissionsApi(self.__client)
