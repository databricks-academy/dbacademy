from dbacademy.clients.rest.common import ApiContainer

__all__ = ["Permissions"]


class Permissions(ApiContainer):
    from dbacademy.dbrest.client import DBAcademyRestClient

    def __init__(self, client: DBAcademyRestClient):
        self.client = client

        from dbacademy.dbrest.permissions.clusters import Clusters
        self.clusters = Clusters(client)

        from dbacademy.dbrest.permissions.directories import Directories
        self.directories = Directories(client)

        from dbacademy.dbrest.permissions.jobs import Jobs
        self.jobs = Jobs(client)

        from dbacademy.dbrest.permissions.pools import Pools
        self.pools = Pools(client)

        from dbacademy.dbrest.permissions.sql import Sql
        self.sql = Sql(client)

        from dbacademy.dbrest.permissions.cluster_policies import ClusterPolicies
        self.cluster_policies = ClusterPolicies(client)

        from dbacademy.dbrest.permissions.warehouses import Warehouses
        self.warehouses = Warehouses(client)

        class Authorization:
            def __init__(self):
                from dbacademy.dbrest.permissions.authorization_tokens import Tokens
                self.tokens = Tokens(client)

        self.authorizations = Authorization()
