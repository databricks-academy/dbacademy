__all__ = ["DBAcademyRestClient", "from_args", "from_workspace", "from_auth_header", "from_client", "from_token", "from_username"]

from typing import Optional
from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients import ClientErrorHandler

DEFAULT_SCOPE = "DBACADEMY"


# noinspection PyPep8Naming
class Constants:
    def __init__(self):
        pass

    @property
    def AUTH_HEADER(self):
        return "AUTH_HEADER"

    @property
    def TOKEN(self):
        return "TOKEN"

    @property
    def ENDPOINT(self):
        return "ENDPOINT"

    @property
    def USERNAME(self):
        return "USERNAME"

    @property
    def PASSWORD(self):
        return "PASSWORD"


constants = Constants()


class DBAcademyRestClient(ApiClient):
    """Databricks Academy REST API client."""

    def __init__(self, *,
                 token: Optional[str],
                 endpoint:  Optional[str],
                 username:  Optional[str],
                 password:  Optional[str],
                 authorization_header:  Optional[str],
                 client:  Optional[ApiClient],
                 verbose: Optional[bool],
                 throttle_seconds: Optional[int],
                 error_handler: Optional[ClientErrorHandler]):
        """
        Create a Databricks REST API client.

        This is similar to ApiClient.__init__ except the parameter order is different to ensure backwards compatibility.

        Args:
            endpoint: The common base URL to the API endpoints.  e.g. https://workspace.cloud.databricks.com/API/
            token: The API authentication token.  Defaults to None.
            username: The authentication username.  Defaults to None.
            password: The authentication password.  Defaults to None.
            authorization_header: The header to use for authentication.
                By default, it's generated from the token or password.
            client: A parent ApiClient from which to clone settings.
            throttle_seconds: Number of seconds to sleep between requests.
        """
        from dbacademy.common import validate

        if client is not None:
            # We have a valid client, use it to initialize from.
            authorization_header = authorization_header or client.authorization_header
            endpoint = endpoint or client.endpoint

            if authorization_header is None:
                username = username or client.username
                password = password or client.password

                if username is None and password is None:
                    token = token or client.token

        # endpoint = endpoint or self.__get_notebook_endpoint()
        #
        # if not any([authorization_header, user, password]):
        #     token = token or self.__get_notebook_token()

        endpoint = validate.str_value(endpoint=endpoint, required=True)
        if endpoint is not None:
            endpoint = endpoint.rstrip("/")
            endpoint = endpoint.rstrip("/api")

        # Cleanup the API URL
        # if endpoint is None:
        #     endpoint = None
        # elif endpoint.endswith("/api/"):
        #     endpoint = endpoint
        # elif endpoint.endswith("/api"):
        #     endpoint = endpoint + "/"
        # else:
        #     endpoint = endpoint.rstrip("/") + "/api/"

        super().__init__(endpoint,
                         token=token,
                         username=username,
                         password=password,
                         authorization_header=authorization_header,
                         client=client,
                         verbose=verbose,
                         throttle_seconds=throttle_seconds,
                         error_handler=error_handler)

        from dbacademy.clients.databricks.clusters.cluster_client_class import ClustersClient
        self.clusters = ClustersClient(self)

        from dbacademy.clients.databricks.cluster_policies import ClustersPolicyClient
        self.cluster_policies = ClustersPolicyClient(self)

        from dbacademy.clients.databricks.instance_pools import InstancePoolsClient
        self.instance_pools = InstancePoolsClient(self)

        from dbacademy.clients.databricks.jobs.jobs_client_class import JobsClient
        self.jobs = JobsClient(self)

        from dbacademy.clients.databricks.ml import MlClient
        self.ml = MlClient(self)

        from dbacademy.clients.databricks.permissions import Permissions
        self.permissions = Permissions(self)

        from dbacademy.clients.databricks.pipelines import PipelinesClient
        self.pipelines = PipelinesClient(self)

        from dbacademy.clients.databricks.repos import ReposClient
        self.repos = ReposClient(self)

        from dbacademy.clients.databricks.runs import RunsClient
        self.runs = RunsClient(self)

        from dbacademy.clients.databricks.scim import ScimClient
        self.scim = ScimClient(self)

        from dbacademy.clients.databricks.sql import SqlClient
        self.sql = SqlClient(self)

        from dbacademy.clients.databricks.tokens import TokensClient
        self.tokens = TokensClient(self)

        from dbacademy.clients.databricks.token_management import TokenManagementClient
        self.token_management = TokenManagementClient(self)

        from dbacademy.clients.databricks.uc import UcClient
        self.uc = UcClient(self)

        from dbacademy.clients.databricks.workspace import WorkspaceClient
        self.workspace = WorkspaceClient(self)

        from dbacademy.clients.databricks.workspace_config import WorkspaceConfigClient
        self.workspace_config = WorkspaceConfigClient(self)

        from dbacademy.clients.databricks.serving_endpoints import ServingEndpointsApi
        self.serving_endpoints = ServingEndpointsApi(self)

        from dbacademy.clients.databricks.secrets import SecretsClient
        self.secrets = SecretsClient(self)


def none_reference() -> Optional[DBAcademyRestClient]:
    """Returns a None instance of DBAcademyRestClient, used to set an initial value to None while retaining the type information."""
    return None


def __load(name: str, value: str, scope: str) -> str:
    import os

    if value:
        return value
    elif os.environ.get(f"{scope}_{name}"):
        return os.environ.get(f"{scope}_{name}")
    else:
        return os.environ.get(name)


def from_args(*,
              token: str = None,
              endpoint: str = None,
              username: str = None,
              password: str = None,
              authorization_header: str = None,
              client: ApiClient = None,
              # Common parameters
              verbose: bool = False,
              throttle_seconds: int = 0,
              error_handler: ClientErrorHandler = ClientErrorHandler()) -> DBAcademyRestClient:

    return DBAcademyRestClient(token=token,
                               endpoint=endpoint,
                               username=username,
                               password=password,
                               authorization_header=authorization_header,
                               client=client,
                               verbose=verbose,
                               throttle_seconds=throttle_seconds,
                               error_handler=error_handler)


def from_username(*,
                  endpoint: str = None,
                  username: str = None,
                  password: str = None,
                  # Common parameters
                  scope: str = DEFAULT_SCOPE,
                  verbose: bool = False,
                  throttle_seconds: int = 0,
                  error_handler: ClientErrorHandler = ClientErrorHandler()) -> DBAcademyRestClient:

    return from_args(endpoint=__load(constants.ENDPOINT, endpoint, scope),
                     username=__load(constants.USERNAME, username, scope),
                     password=__load(constants.PASSWORD, password, scope),
                     verbose=verbose,
                     throttle_seconds=throttle_seconds,
                     error_handler=error_handler)


def from_token(token: str = None,
               endpoint: str = None,
               # Common parameters
               scope: str = DEFAULT_SCOPE,
               verbose: bool = False,
               throttle_seconds: int = 0,
               error_handler: ClientErrorHandler = ClientErrorHandler()) -> DBAcademyRestClient:

    return from_args(endpoint=__load(constants.ENDPOINT, endpoint, scope),
                     token=__load(constants.TOKEN, token, scope),
                     verbose=verbose,
                     throttle_seconds=throttle_seconds,
                     error_handler=error_handler)


def from_auth_header(*,
                     endpoint: str = None,
                     authorization_header: str = None,
                     # Common parameters
                     scope: str = DEFAULT_SCOPE,
                     verbose: bool = False,
                     throttle_seconds: int = 0,
                     error_handler: ClientErrorHandler = ClientErrorHandler()) -> DBAcademyRestClient:

    return from_args(endpoint=__load(constants.ENDPOINT, endpoint, scope),
                     authorization_header=__load(constants.AUTH_HEADER, authorization_header, scope),
                     verbose=verbose,
                     throttle_seconds=throttle_seconds,
                     error_handler=error_handler)


def from_client(client: ApiClient) -> DBAcademyRestClient:

    return from_args(client=client,
                     # Common parameters
                     verbose=client.verbose,
                     throttle_seconds=client.throttle_seconds,
                     error_handler=client.error_handler)


def from_workspace(*,
                   verbose: bool = False,
                   throttle_seconds: int = 0,
                   error_handler: ClientErrorHandler = ClientErrorHandler()) -> DBAcademyRestClient:
    from dbacademy import dbgems

    return from_args(token=dbgems.get_notebooks_api_token(),
                     endpoint=dbgems.get_notebooks_api_endpoint(),
                     verbose=verbose,
                     throttle_seconds=throttle_seconds,
                     error_handler=error_handler)
