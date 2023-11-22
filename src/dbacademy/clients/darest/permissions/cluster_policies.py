__all__ = ["ClusterPolicies"]

from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients.databricks.permissions.crud import PermissionsCrud


class ClusterPolicies(PermissionsCrud):
    valid_permissions = ["CAN_USE"]

    def __init__(self, client: ApiClient):
        super().__init__(client, "/api/2.0/preview/permissions/cluster-policies", "cluster-policies")
