__all__ = ["DBAcademyRestClient", "from_args", "from_notebook", "from_auth_header", "from_client", "from_token", "from_username"]

from typing import Optional
from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients import ClientErrorHandler
from dbacademy.clients.dbrest.secrets_api import SecretsApi
from dbacademy.clients.dbrest.clusters_api import ClustersApi
from dbacademy.clients.dbrest.cluster_policies_api import ClustersPolicyApi
from dbacademy.clients.dbrest.instance_pools_api import InstancePoolsApi
from dbacademy.clients.dbrest.jobs_api import JobsApi
from dbacademy.clients.dbrest.ml_api import MlApi
from dbacademy.clients.dbrest.permissions_api import PermissionsApi
from dbacademy.clients.dbrest.pipelines_api import PipelinesApi
from dbacademy.clients.dbrest.repos_api import ReposApi
from dbacademy.clients.dbrest.runs_api import RunsApi
from dbacademy.clients.dbrest.scim_api import ScimApi
from dbacademy.clients.dbrest.sql_api import SqlApi
from dbacademy.clients.dbrest.tokens_api import TokensApi
from dbacademy.clients.dbrest.token_management_api import TokenManagementApi
from dbacademy.clients.dbrest.uc_api import UcApi
from dbacademy.clients.dbrest.workspace_api import WorkspaceApi
from dbacademy.clients.dbrest.workspace_config_api import WorkspaceConfigApi
from dbacademy.clients.dbrest.serving_endpoints_api import ServingEndpointsApi


# noinspection PyPep8Naming
class Constants:

    def __init__(self):
        pass

    @property
    def DEFAULT_SCOPE(self) -> str:
        return "DBACADEMY"

    @property
    def AUTH_HEADER(self) -> str:
        return "AUTH_HEADER"

    @property
    def TOKEN(self) -> str:
        return "TOKEN"

    @property
    def ENDPOINT(self) -> str:
        return "ENDPOINT"

    @property
    def USERNAME(self) -> str:
        return "USERNAME"

    @property
    def PASSWORD(self) -> str:
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

        validate(endpoint=endpoint).required.str()
        endpoint = endpoint.rstrip("/")
        endpoint = endpoint.rstrip("/api")

        super().__init__(endpoint,
                         token=token,
                         username=username,
                         password=password,
                         authorization_header=authorization_header,
                         client=client,
                         verbose=verbose,
                         throttle_seconds=throttle_seconds,
                         error_handler=error_handler)

    @property
    def clusters(self) -> ClustersApi:
        return ClustersApi(self)

    @property
    def cluster_policies(self) -> ClustersPolicyApi:
        return ClustersPolicyApi(self)

    @property
    def instance_pools(self) -> InstancePoolsApi:
        return InstancePoolsApi(self)

    @property
    def jobs(self) -> JobsApi:
        return JobsApi(self)

    @property
    def ml(self) -> MlApi:
        return MlApi(self)

    @property
    def permissions(self) -> PermissionsApi:
        return PermissionsApi(self)

    @property
    def pipelines(self) -> PipelinesApi:
        return PipelinesApi(self)

    @property
    def repos(self) -> ReposApi:
        return ReposApi(self)

    @property
    def runs(self) -> RunsApi:
        return RunsApi(self)

    @property
    def scim(self) -> ScimApi:
        return ScimApi(self)

    @property
    def sql(self) -> SqlApi:
        return SqlApi(self)

    @property
    def tokens(self) -> TokensApi:
        return TokensApi(self)

    @property
    def token_management(self) -> TokenManagementApi:
        return TokenManagementApi(self)

    @property
    def uc(self) -> UcApi:
        return UcApi(self)

    @property
    def workspace(self) -> WorkspaceApi:
        return WorkspaceApi(self)

    @property
    def workspace_config(self) -> WorkspaceConfigApi:
        return WorkspaceConfigApi(self)

    @property
    def serving_endpoints(self) -> ServingEndpointsApi:
        return ServingEndpointsApi(self)

    @property
    def secrets(self) -> SecretsApi:
        return SecretsApi(self)


# def none_reference() -> Optional[DBAcademyRestClient]:
#     """Returns a None instance of DBAcademyRestClient, used to set an initial value to None while retaining the type information."""
#     return None


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
                  scope: str = constants.DEFAULT_SCOPE,
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
               scope: str = constants.DEFAULT_SCOPE,
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
                     scope: str = constants.DEFAULT_SCOPE,
                     verbose: bool = False,
                     throttle_seconds: int = 0,
                     error_handler: ClientErrorHandler = ClientErrorHandler()) -> DBAcademyRestClient:

    return from_args(endpoint=__load(constants.ENDPOINT, endpoint, scope),
                     authorization_header=__load(constants.AUTH_HEADER, authorization_header, scope),
                     verbose=verbose,
                     throttle_seconds=throttle_seconds,
                     error_handler=error_handler)


def from_client(client: ApiClient) -> DBAcademyRestClient:
    from dbacademy.common import validate

    return from_args(client=validate(client=client).required.as_type(ApiClient),
                     # Common parameters
                     verbose=client.verbose,
                     throttle_seconds=client.throttle_seconds,
                     error_handler=client.error_handler)


def from_notebook(*,
                  verbose: bool = False,
                  throttle_seconds: int = 0,
                  error_handler: ClientErrorHandler = ClientErrorHandler()) -> DBAcademyRestClient:
    from dbacademy import dbgems

    return from_args(token=dbgems.get_notebooks_api_token(),
                     endpoint=dbgems.get_notebooks_api_endpoint(),
                     verbose=verbose,
                     throttle_seconds=throttle_seconds,
                     error_handler=error_handler)
