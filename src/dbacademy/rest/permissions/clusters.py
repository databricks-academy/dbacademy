from dbacademy.rest.common import ApiClient
from dbacademy.rest.permissions.crud import PermissionsCrud

__all__ = ["Clusters"]


class Clusters(PermissionsCrud):
    valid_permissions = ["CAN_ATTACH_TO", "CAN_RESTART", "CAN_MANAGE"]

    def __init__(self, client: ApiClient):
        super().__init__(client, "2.0/permissions/clusters", "cluster")
        self.policies = ClusterPolicies(client)


class ClusterPolicies(PermissionsCrud):
    valid_permissions = [None, "CAN_USE"]

    def __init__(self, client: ApiClient):
        super().__init__(client, "2.0/permissions/cluster-policies", "cluster_policy")
