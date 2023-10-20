__all__ = ["from_args", "from_args_aws"]

from typing import Union
from dbacademy.common import Cloud
from dbacademy.clients.rest.common import ApiClient


class AccountsClient(ApiClient):

    def __init__(self, *,
                 endpoint: str,
                 account_id: str,
                 username: str,
                 password: str,
                 verbose: bool,
                 throttle_seconds: int):

        super().__init__(endpoint=endpoint,
                         token=None,
                         username=username,
                         password=password,
                         authorization_header=None,
                         client=None,
                         verbose=verbose,
                         throttle_seconds=throttle_seconds)

        self.__account_id = account_id

        from dbacademy.clients.databricks.accounts.scim import AccountScimClient
        self.scim = AccountScimClient(self, account_id=account_id)

        from dbacademy.clients.databricks.accounts.workspaces import WorkspacesClient
        self.workspaces = WorkspacesClient(self, self.account_id)

    @property
    def account_id(self) -> str:
        return self.__account_id


def from_args(*,
              cloud: Union[str, Cloud],
              account_id: str,
              username: str,
              password: str,
              verbose: bool = False,
              throttle_seconds: int = 0) -> AccountsClient:

    if type(cloud) is str:
        cloud = Cloud[cloud]

    if cloud == Cloud.AWS:
        endpoint = f'https://accounts.cloud.databricks.com/api/2.0/accounts/{account_id}'
    elif cloud == Cloud.GCP:
        endpoint = f'https://accounts.gcp.databricks.com/api/2.0/accounts/{account_id}'
    elif cloud == Cloud.MSA:
        endpoint = f'https://accounts.azuredatabricks.net/api/2.0/accounts/{account_id}'
    else:
        raise ValueError(f"""The parameter "cloud" must be of type dbacademy.common.Cloud, found: "{cloud}".""")

    return AccountsClient(endpoint=endpoint,
                          account_id=account_id,
                          username=username,
                          password=password,
                          verbose=verbose,
                          throttle_seconds=throttle_seconds)


def from_args_aws(*,
                  account_id: str,
                  username: str,
                  password: str,
                  verbose: bool = False,
                  throttle_seconds: int = 0) -> AccountsClient:

    return AccountsClient(endpoint="https://accounts.cloud.databricks.com",
                          account_id=account_id,
                          username=username,
                          password=password,
                          verbose=verbose,
                          throttle_seconds=throttle_seconds)


def from_args_msa(*,
                  account_id: str,
                  username: str,
                  password: str,
                  verbose: bool = False,
                  throttle_seconds: int = 0) -> AccountsClient:

    return AccountsClient(endpoint="https://accounts.azuredatabricks.net",
                          account_id=account_id,
                          username=username,
                          password=password,
                          verbose=verbose,
                          throttle_seconds=throttle_seconds)


def from_args_gcp(*,
                  account_id: str,
                  username: str,
                  password: str,
                  verbose: bool = False,
                  throttle_seconds: int = 0) -> AccountsClient:

    return AccountsClient(endpoint="https://accounts.gcp.databricks.com/api/2.0/accounts",
                          account_id=account_id,
                          username=username,
                          password=password,
                          verbose=verbose,
                          throttle_seconds=throttle_seconds)
