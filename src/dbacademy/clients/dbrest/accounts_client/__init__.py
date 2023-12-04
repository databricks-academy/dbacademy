__all__ = ["AccountsClient", "from_args"]

from typing import Union

from dbacademy.clients.dbrest.accounts_client.metastores_api import MetastoresApi
from dbacademy.common import validate
from dbacademy.common import Cloud
from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients import ClientErrorHandler
from dbacademy.clients.dbrest.accounts_client.scim import AccountScimApi
from dbacademy.clients.dbrest.accounts_client.workspaces_api import WorkspacesApi


# noinspection PyPep8Naming
class Constants:
    def __init__(self):
        pass

    @property
    def DEFAULT_SCOPE(self):
        return "DBACADEMY"

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

    @property
    def ACCOUNT_ID(self) -> str:
        return "ACCOUNT_ID"


constants = Constants()


class AccountsClient(ApiClient):

    def __init__(self, *,
                 endpoint: str,
                 account_id: str,
                 username: str,
                 password: str,
                 verbose: bool,
                 throttle_seconds: int,
                 error_handler: ClientErrorHandler):

        # Validation is done in the call to ApiClient.__init__().
        super().__init__(endpoint=endpoint,
                         token=None,
                         username=username,
                         password=password,
                         authorization_header=None,
                         client=None,
                         verbose=verbose,
                         throttle_seconds=throttle_seconds,
                         error_handler=error_handler)

        from dbacademy.common import validate

        self.__account_id = validate(account_id=account_id).required.str()

    @property
    def account_id(self) -> str:
        return self.__account_id

    @property
    def metastores(self) -> MetastoresApi:
        return MetastoresApi(self, self.account_id)

    @property
    def scim(self) -> AccountScimApi:
        return AccountScimApi(self, self.account_id)

    @property
    def workspaces(self) -> WorkspacesApi:
        return WorkspacesApi(self, self.account_id)


def __load(name: str, value: str, scope: str) -> str:
    import os

    if value:
        return value
    elif os.environ.get(f"{scope}_{name}"):
        return os.environ.get(f"{scope}_{name}")
    else:
        return os.environ.get(name)


def from_args(*,
              scope: str = constants.DEFAULT_SCOPE,
              cloud: Union[str, Cloud],
              account_id: str = None,
              username: str = None,
              password: str = None,
              verbose: bool = False,
              throttle_seconds: int = 0,
              error_handler: ClientErrorHandler = ClientErrorHandler()) -> AccountsClient:

    cloud = validate(cloud=cloud).required.enum(Cloud, auto_convert=True)

    if cloud == Cloud.AWS:
        endpoint = f'https://accounts.cloud.databricks.com'
    elif cloud == Cloud.GCP:
        endpoint = f'https://accounts.gcp.databricks.com'
    elif cloud == Cloud.MSA:
        endpoint = f'https://accounts.azuredatabricks.net'
    else:
        raise ValueError(f"""No API endpoint defined for the cloud {cloud}.""")

    # Validation is done in the call to ApiClient.__init__().
    return AccountsClient(endpoint=endpoint,
                          account_id=__load(constants.ACCOUNT_ID, account_id, scope),
                          username=__load(constants.USERNAME, username, scope),
                          password=__load(constants.PASSWORD, password, scope),
                          verbose=verbose,
                          throttle_seconds=throttle_seconds,
                          error_handler=error_handler)
