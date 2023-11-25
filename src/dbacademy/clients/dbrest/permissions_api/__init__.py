__all__ = ["PermissionsApi"]

from dbacademy.clients.rest.common import ApiContainer, ApiClient
from dbacademy.clients.dbrest.permissions_api.clusters_permissions_api import ClustersPermissionsApi
from dbacademy.clients.dbrest.permissions_api.directories_permissions_api import DirectoriesPermissionsApi
from dbacademy.clients.dbrest.permissions_api.jobs_permissions_api import JobsPermissionsApi
from dbacademy.clients.dbrest.permissions_api.pools_permissions_api import PoolsPermissionsApi
from dbacademy.clients.dbrest.permissions_api.sql import SqlPermissionsApi
from dbacademy.clients.dbrest.permissions_api.cluster_policies_permissions_api import ClusterPoliciesPermissionsApi
from dbacademy.clients.dbrest.permissions_api.warehouses_permissions_api import WarehousesPermissionsApi
from dbacademy.clients.dbrest.permissions_api.authorization_tokens_permissions_api import AuthTokensPermissionsApi


class Authorization:

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)

    @property
    def tokens(self) -> AuthTokensPermissionsApi:
        return AuthTokensPermissionsApi(self.__client)


class PermissionsApi(ApiContainer):

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)

    @property
    def clusters(self) -> ClustersPermissionsApi:
        return ClustersPermissionsApi(self.__client)

    @property
    def directories(self) -> DirectoriesPermissionsApi:
        return DirectoriesPermissionsApi(self.__client)

    @property
    def jobs(self) -> JobsPermissionsApi:
        return JobsPermissionsApi(self.__client)

    @property
    def pools(self) -> PoolsPermissionsApi:
        return PoolsPermissionsApi(self.__client)

    @property
    def sql(self) -> SqlPermissionsApi:
        return SqlPermissionsApi(self.__client)

    @property
    def cluster_policies(self) -> ClusterPoliciesPermissionsApi:
        return ClusterPoliciesPermissionsApi(self.__client)

    @property
    def warehouses(self) -> WarehousesPermissionsApi:
        return WarehousesPermissionsApi(self.__client)

    @property
    def authorizations(self) -> Authorization:
        return Authorization(self.__client)
