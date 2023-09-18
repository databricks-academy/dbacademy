__all__ = ["DBAcademyRestClient"]

from dbacademy.clients.rest.common import ApiClient


class DBAcademyRestClient(ApiClient):
    """Databricks Academy REST API client."""
    from dbacademy.dbrest.accounts import AccountsClient

    @classmethod
    def __get_notebook_endpoint(cls):
        from dbacademy import dbgems
        return dbgems.get_notebooks_api_endpoint()

    @classmethod
    def __get_notebook_token(cls):
        from dbacademy import dbgems
        return dbgems.get_notebooks_api_token()

    def __init__(self,
                 token: str = None,
                 endpoint: str = None,
                 throttle_seconds: int = 0,
                 *,
                 user: str = None,
                 password: str = None,
                 authorization_header: str = None,
                 client: ApiClient = None,
                 verbose: bool = False):
        """
        Create a Databricks REST API client.

        This is similar to ApiClient.__init__ except the parameter order is different to ensure backwards compatibility.

        Args:
            endpoint: The common base URL to the API endpoints.  e.g. https://workspace.cloud.databricks.com/API/
            token: The API authentication token.  Defaults to None.
            user: The authentication username.  Defaults to None.
            password: The authentication password.  Defaults to None.
            authorization_header: The header to use for authentication.
                By default, it's generated from the token or password.
            client: A parent ApiClient from which to clone settings.
            throttle_seconds: Number of seconds to sleep between requests.
        """

        if client is not None:
            # We have a valid client, use it to initialize from.
            authorization_header = authorization_header or client.authorization_header
            endpoint = endpoint or client.url

            if authorization_header is None:
                user = user or client.user
                password = password or client.password

                if user is None and password is None:
                    token = token or client.token

        endpoint = endpoint or self.__get_notebook_endpoint()

        if not any([authorization_header, user, password]):
            token = token or self.__get_notebook_token()

        # Cleanup the API URL
        if endpoint is None:
            url = None
        elif endpoint.endswith("/api/"):
            url = endpoint
        elif endpoint.endswith("/api"):
            url = endpoint + "/"
        else:
            url = endpoint.rstrip("/") + "/api/"

        super().__init__(url,
                         token=token,
                         user=user,
                         password=password,
                         authorization_header=authorization_header,
                         client=client,
                         throttle_seconds=throttle_seconds,
                         verbose=verbose)

        self.endpoint = endpoint

        from dbacademy.dbrest.clusters import ClustersClient
        self.clusters = ClustersClient(self)

        from dbacademy.dbrest.cluster_policies import ClustersPolicyClient
        self.cluster_policies = ClustersPolicyClient(self)

        from dbacademy.dbrest.instance_pools import InstancePoolsClient
        self.instance_pools = InstancePoolsClient(self)

        from dbacademy.dbrest.jobs import JobsClient
        self.jobs = JobsClient(self)

        from dbacademy.dbrest.ml import MlClient
        self.ml = MlClient(self)

        from dbacademy.dbrest.permissions import Permissions
        self.permissions = Permissions(self)

        from dbacademy.dbrest.pipelines import PipelinesClient
        self.pipelines = PipelinesClient(self)

        from dbacademy.dbrest.repos import ReposClient
        self.repos = ReposClient(self)

        from dbacademy.dbrest.runs import RunsClient
        self.runs = RunsClient(self)

        from dbacademy.dbrest.scim import ScimClient
        self.scim = ScimClient(self)

        from dbacademy.dbrest.sql import SqlClient
        self.sql = SqlClient(self)

        from dbacademy.dbrest.tokens import TokensClient
        self.tokens = TokensClient(self)

        from dbacademy.dbrest.token_management import TokenManagementClient
        self.token_management = TokenManagementClient(self)

        from dbacademy.dbrest.uc import UcClient
        self.uc = UcClient(self)

        from dbacademy.dbrest.workspace import WorkspaceClient
        self.workspace = WorkspaceClient(self)

        from dbacademy.dbrest.workspace_config import WorkspaceConfigClient
        self.workspace_config = WorkspaceConfigClient(self)

        from dbacademy.dbrest.serving_endpoints import ServingEndpointsClient
        self.serving_endpoints = ServingEndpointsClient(self)

    def accounts(self, account_id: str) -> AccountsClient:
        from dbacademy.dbrest.accounts import AccountsClient
        return AccountsClient(self, account_id)

    def vprint(self, what):
        if self.verbose:
            print(what)
