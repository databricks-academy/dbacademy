__all__ = ["Permissions"]

from dbacademy.clients.rest.common import ApiClient, ApiContainer


class Permissions(ApiContainer):

    def __init__(self, client: ApiClient):
        self.client = client

        from dbacademy.clients.darest.permissions.clusters import Clusters
        self.clusters = Clusters(client)

        from dbacademy.clients.darest.permissions.directories import Directories
        self.directories = Directories(client)

        from dbacademy.clients.darest.permissions.jobs import Jobs
        self.jobs = Jobs(client)

        from dbacademy.clients.darest.permissions.pools import Pools
        self.pools = Pools(client)

        from dbacademy.clients.darest.permissions.sql import Sql
        self.sql = Sql(client)

        from dbacademy.clients.darest.permissions.cluster_policies import ClusterPolicies
        self.cluster_policies = ClusterPolicies(client)

        from dbacademy.clients.darest.permissions.warehouses import Warehouses
        self.warehouses = Warehouses(client)

        class Authorization:
            def __init__(self):
                from dbacademy.clients.darest.permissions.authorization_tokens import Tokens
                self.tokens = Tokens(client)

        self.authorizations = Authorization()
