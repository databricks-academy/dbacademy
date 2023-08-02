from dbacademy.clients.rest.common import ApiContainer, ApiClient

__all__ = ["Permissions"]


class Permissions(ApiContainer):
    def __init__(self, client: ApiClient):
        self.client = client

        from dbacademy.clients.rest.permissions.clusters import Clusters
        self.clusters = Clusters(client)

        from dbacademy.clients.rest.permissions.directories import Directories
        self.directories = Directories(client)

        from dbacademy.clients.rest.permissions.jobs import Jobs
        self.jobs = Jobs(client)

        from dbacademy.clients.rest.permissions.pools import Pools
        self.pools = Pools(client)

        from dbacademy.clients.rest.permissions.sql import Sql
        self.sql = Sql(client)

        from dbacademy.clients.rest.permissions.cluster_policies import ClusterPolicies
        self.cluster_policies = ClusterPolicies(client)

        from dbacademy.clients.rest.permissions.warehouses import Warehouses
        self.warehouses = Warehouses(client)

        class Authorization:
            def __init__(self):
                from dbacademy.clients.rest.permissions.authorization_tokens import Tokens
                self.tokens = Tokens(client)

        self.authorizations = Authorization()
