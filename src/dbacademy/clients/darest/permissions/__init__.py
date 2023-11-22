__all__ = ["Permissions"]

from dbacademy.clients.rest.common import ApiContainer
from dbacademy.clients.darest import DBAcademyRestClient
from dbacademy.clients.darest.permissions.clusters import Clusters
from dbacademy.clients.darest.permissions.directories import Directories
from dbacademy.clients.darest.permissions.jobs import Jobs
from dbacademy.clients.darest.permissions.pools import Pools
from dbacademy.clients.darest.permissions.sql import Sql
from dbacademy.clients.darest.permissions.cluster_policies import ClusterPolicies
from dbacademy.clients.darest.permissions.warehouses import Warehouses
from dbacademy.clients.darest.permissions.authorization_tokens import Tokens


class Authorization:
    def __init__(self, client: DBAcademyRestClient):
        self.__client = client

    @property
    def client(self) -> DBAcademyRestClient:
        return self.__client

    @property
    def tokens(self) -> Tokens:
        return Tokens(self.client)


class Permissions(ApiContainer):

    def __init__(self, client: DBAcademyRestClient):
        self.__client = client

    @property
    def client(self) -> DBAcademyRestClient:
        return self.__client

    @property
    def clusters(self) -> clusters:
        return Clusters(self.client)

    @property
    def directories(self) -> directories:
        return Directories(self.client)

    @property
    def jobs(self) -> jobs:
        return Jobs(self.client)

    @property
    def pools(self) -> pools:
        return Pools(self.client)

    @property
    def sql(self) -> sql:
        return Sql(self.client)

    @property
    def cluster_policies(self) -> cluster_policies:
        return ClusterPolicies(self.client)

    @property
    def warehouses(self) -> warehouses:
        return Warehouses(self.client)

    @property
    def authorizations(self) -> Authorization:
        return Authorization(self.client)
