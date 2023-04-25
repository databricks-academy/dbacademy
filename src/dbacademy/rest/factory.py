__all__ = ["dbrest_factory", "dougrest_factory"]

from functools import cache
from typing import Dict, Generic, Type, TypeVar, Union, Optional

from dbacademy.dbrest.client import DBAcademyRestClient
from dbacademy.dougrest import AccountsApi, DatabricksApi

ApiType = TypeVar('ApiType', bound=Union[DatabricksApi, DBAcademyRestClient])


class ApiClientFactory(Generic[ApiType]):

    PROFILE_TEST = "TEST"
    PROFILE_ENVIRONMENT_TEST = "TEST_ENVIRONMENT"
    PROFILE_DEFAULT = "DEFAULT"
    PROFILE_ENVIRONMENT = "ENVIRONMENT"

    ENV_DATABRICKS_HOST = "DATABRICKS_HOST"
    ENV_DATABRICKS_TOKEN = "DATABRICKS_TOKEN"
    ENV_DATABRICKS_ACCOUNT_ID = "DATABRICKS_ACCOUNT_ID"
    ENV_DATABRICKS_ACCOUNT_NAME = "DATABRICKS_ACCOUNT_NAME"
    ENV_DATABRICKS_ACCOUNT_PASS = "DATABRICKS_ACCOUNT_PASS"

    ENV_DATABRICKS_HOST_TEST = "DATABRICKS_HOST_TEST"
    ENV_DATABRICKS_TOKEN_TEST = "DATABRICKS_TOKEN_TEST"
    ENV_DATABRICKS_ACCOUNT_ID_TEST = "DATABRICKS_ACCOUNT_ID_TEST"
    ENV_DATABRICKS_ACCOUNT_NAME_TEST = "DATABRICKS_ACCOUNT_NAME_TEST"
    ENV_DATABRICKS_ACCOUNT_PASS_TEST = "DATABRICKS_ACCOUNT_PASS_TEST"

    SECTION_HOST = "host"
    SECTION_TOKEN = "token"

    def __init__(self, api_type: Type[ApiType]):
        self.api_type = api_type

    @cache
    def test_client(self) -> ApiType:
        know_clients = self.known_clients()  # Minimize file hits
        result = know_clients.get(ApiClientFactory.PROFILE_ENVIRONMENT_TEST)
        result = result or know_clients.get(ApiClientFactory.PROFILE_TEST)
        if result:
            return result
        raise ValueError(f"Unable to determine the test_client() hostname and token; "
                         f"Please configure the [TEST] section of the databricks config file"
                         f" or specify the {ApiClientFactory.ENV_DATABRICKS_HOST_TEST} and "
                         f"{ApiClientFactory.ENV_DATABRICKS_TOKEN_TEST} environment variables.")

    @cache
    def current_workspace(self) -> Optional[ApiType]:
        """
        If run inside a Databricks workspace, return an ApiClient for the current workspace.
        Otherwise, return None.
        """
        from dbacademy import dbgems
        from dbacademy.dbgems.mock_dbutils_class import MockDBUtils

        if isinstance(dbgems.dbutils, MockDBUtils):
            return self.test_client()

        token = dbgems.get_notebooks_api_token()
        endpoint = dbgems.get_notebooks_api_endpoint()
        return self.token_auth(endpoint, token)

    @staticmethod
    def extract_hostname(url: str) -> str:
        url = url.lower()
        if url.startswith("https://"):
            url = url[8:]
        if "/" in url:
            url = url[:url.find("/")]
        return url

    def token_auth(self, hostname: str, token: str) -> ApiType:
        hostname = ApiClientFactory.extract_hostname(hostname)
        endpoint = f"https://{hostname}"
        if self.api_type == DatabricksApi:
            return DatabricksApi(hostname, token=token)
        elif self.api_type == DBAcademyRestClient:
            return DBAcademyRestClient(token, endpoint)
        else:
            raise ValueError(f"Unknown ApiClient class: " + str(ApiType))

    def password_auth(self, hostname: str, username: str, password: str) -> ApiType:
        hostname = ApiClientFactory.extract_hostname(hostname)
        endpoint = f"https://{hostname}"
        if self.api_type == DatabricksApi:
            return DatabricksApi(hostname=hostname, user=username, password=password)
        elif self.api_type == DBAcademyRestClient:
            return DBAcademyRestClient(endpoint=endpoint, user=username, password=password)
        else:
            raise ValueError(f"Unknown ApiClient class: " + str(ApiType))

    # TODO Refactor to avoid hitting the file system at all by hinting that we want ENV over CFG
    @cache
    def known_clients(self) -> Dict[str, ApiType]:
        import configparser
        import os

        clients = {}

        self.__load_env_config(clients=clients,
                               profile=ApiClientFactory.PROFILE_ENVIRONMENT,
                               host_key=ApiClientFactory.ENV_DATABRICKS_HOST,
                               token_key=ApiClientFactory.ENV_DATABRICKS_TOKEN)

        self.__load_env_config(clients=clients,
                               profile=ApiClientFactory.PROFILE_ENVIRONMENT_TEST,
                               host_key=ApiClientFactory.ENV_DATABRICKS_HOST_TEST,
                               token_key=ApiClientFactory.ENV_DATABRICKS_TOKEN_TEST)

        for path in ('~/.databrickscfg', '.databrickscfg'):
            path = os.path.expanduser(path)
            if not os.path.exists(path):
                continue

            # Read the config from path first, then from environment.
            config = configparser.ConfigParser()
            config.read(path)

            for section_name, section in config.items():
                api_type = section.get('api_type', 'workspace')
                if api_type != 'workspace':
                    continue

                host = section[ApiClientFactory.SECTION_HOST]
                token = section[ApiClientFactory.SECTION_TOKEN]
                clients[section_name] = self.token_auth(hostname=host, token=token)

        return clients

    def __load_env_config(self, *, clients: Dict[str, str], profile: str, host_key: str, token_key: str):
        import os
        host = os.getenv(host_key)
        token = os.getenv(token_key)

        if host and token:
            clients[profile] = self.token_auth(hostname=host, token=token)

    # TODO Refactor to avoid hitting the file system at all by hinting that we want ENV over CFG
    @cache
    def test_account(self) -> AccountsApi:
        known_accounts = self.known_accounts()  # Minimize file hits
        result = known_accounts.get(ApiClientFactory.PROFILE_ENVIRONMENT)
        result = result or known_accounts.get(ApiClientFactory.PROFILE_TEST)
        result = result or known_accounts.get(ApiClientFactory.PROFILE_DEFAULT)

        if result is not None:
            return result

        raise ValueError("No account entries found in .databricks_cfg")

    @cache
    def known_accounts(self) -> Dict[str, AccountsApi]:
        import configparser
        import os

        clients = {}
        default = None

        acct_id = os.getenv(ApiClientFactory.ENV_DATABRICKS_ACCOUNT_ID)
        username = os.getenv(ApiClientFactory.ENV_DATABRICKS_ACCOUNT_NAME)
        password = os.getenv(ApiClientFactory.ENV_DATABRICKS_ACCOUNT_PASS)
        if acct_id and username and password:
            clients[ApiClientFactory.PROFILE_ENVIRONMENT] = AccountsApi(acct_id, user=username, password=password)

        for path in ('.databrickscfg', '~/.databrickscfg'):
            path = os.path.expanduser(path)
            if not os.path.exists(path):
                continue

            config = configparser.ConfigParser()
            config.read(path)

            for section_name, section in config.items():
                if not section_name.lower().startswith("e2:"):
                    continue
                section_name = section_name[3:]
                account_id = section['id']
                user = section['username']
                password = section['password']
                clients[section_name] = AccountsApi(account_id, user=user, password=password)
                if default is None:
                    default = clients[section_name]

        if default is not None:
            clients.setdefault(ApiClientFactory.PROFILE_DEFAULT, default)

        return clients

    @classmethod
    def azure_token(cls, directory_id: str, principal_id: str, secret: str) -> str:
        """Do Azure Sign-In with Service Principal"""
        import requests
        azure_databricks_scope = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
        token = requests.post(f"https://login.microsoftonline.com/{directory_id}/oauth2/v2.0/token", data={
            'client_id': principal_id,
            'grant_type': 'client_credentials',
            'scope': azure_databricks_scope,
            'client_secret': secret
        }).json()["access_token"]
        return token

    @classmethod
    def azure_account(cls, account_id: str, directory_id: str,
                      principal_id: str, secret: str) -> AccountsApi:
        """Return an AccountsApi client on Azure using a service principal"""
        token = cls.azure_token(directory_id, principal_id, secret)
        return AccountsApi(account_id, token=token, cloud="MSA")


dbrest_factory = ApiClientFactory[DBAcademyRestClient](DBAcademyRestClient)
dougrest_factory = ApiClientFactory[DatabricksApi](DatabricksApi)
