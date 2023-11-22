__all__ = ["DBAcademyRestClient", "from_args", "from_workspace", "from_auth_header", "from_client", "from_token", "from_username"]

from typing import Optional
from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients import ClientErrorHandler

from dbacademy.clients.darest.secrets import SecretsClient
from dbacademy.clients.darest.clusters.cluster_client_class import ClustersClient
from dbacademy.clients.darest.cluster_policies import ClustersPolicyClient
from dbacademy.clients.darest.instance_pools import InstancePoolsClient
from dbacademy.clients.darest.jobs.jobs_client_class import JobsClient
from dbacademy.clients.darest.ml import MlClient
from dbacademy.clients.darest.permissions import Permissions
from dbacademy.clients.darest.pipelines import PipelinesClient
from dbacademy.clients.darest.repos import ReposClient
from dbacademy.clients.darest.runs import RunsClient
from dbacademy.clients.darest.scim import ScimClient
from dbacademy.clients.darest.sql import SqlClient
from dbacademy.clients.darest.tokens import TokensClient
from dbacademy.clients.darest.token_management import TokenManagementClient
from dbacademy.clients.darest.uc import UcApi
from dbacademy.clients.darest.workspace import WorkspaceClient
from dbacademy.clients.darest.workspace_config import WorkspaceConfigClient
from dbacademy.clients.darest.serving_endpoints import ServingEndpointsApi

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
    def clusters(self) -> ClustersClient:
        return ClustersClient(self)

    @property
    def cluster_policies(self) -> ClustersPolicyClient:
        return ClustersPolicyClient(self)

    @property
    def instance_pools(self) -> InstancePoolsClient:
        return InstancePoolsClient(self)

    @property
    def jobs(self) -> JobsClient:
        return JobsClient(self)

    @property
    def ml(self) -> MlClient:
        return MlClient(self)

    @property
    def permissions(self) -> Permissions:
        return Permissions(self)

    @property
    def pipelines(self) -> PipelinesClient:
        return PipelinesClient(self)

    @property
    def repos(self) -> ReposClient:
        return ReposClient(self)

    @property
    def runs(self) -> RunsClient:
        return RunsClient(self)

    @property
    def scim(self) -> ScimClient:
        return ScimClient(self)

    @property
    def sql(self) -> SqlClient:
        return SqlClient(self)

    @property
    def tokens(self) -> TokensClient:
        return TokensClient(self)

    @property
    def token_management(self) -> TokenManagementClient:
        return TokenManagementClient(self)

    @property
    def uc(self) -> UcApi:
        return UcApi(self)

    @property
    def workspace(self) -> WorkspaceClient:
        return WorkspaceClient(self)

    @property
    def workspace_config(self) -> WorkspaceConfigClient:
        return WorkspaceConfigClient(self)

    @property
    def serving_endpoints(self) -> ServingEndpointsApi:
        return ServingEndpointsApi(self)

    @property
    def secrets(self) -> SecretsClient:
        return SecretsClient(self)


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
