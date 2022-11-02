__all__ = ["dbrest_factory", "dougrest_factory"]

from functools import cache
from typing import Dict, Generic, Type, TypeVar, Union, Optional

from dbacademy.dbrest.client import DBAcademyRestClient
from dbacademy.dougrest import AccountsApi, DatabricksApi

ApiType = TypeVar('ApiType', bound=Union[DatabricksApi, DBAcademyRestClient])


class ApiClientFactory(Generic[ApiType]):

    def __init__(self, api_type: Type[ApiType]):
        self.api_type = api_type

    @cache
    def default_client(self) -> ApiType:
        result = self.known_clients().get("DEFAULT")
        if result:
            return result
        result = self.current_workspace()
        if result:
            return result
        raise ValueError("Unable to determine the default_client hostname and token.")

    @cache
    def current_workspace(self) -> Optional[ApiType]:
        """
        If run inside a Databricks workspace, return an ApiClient for the current workspace.
        Otherwise, return None.
        """
        from dbacademy import dbgems
        if dbgems.dbutils is None:
            return None

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

    @cache
    def known_clients(self) -> Dict[str, ApiType]:
        clients = {}
        import os
        import configparser
        for path in ('~/.databrickscfg', '.databrickscfg'):
            path = os.path.expanduser(path)
            if not os.path.exists(path):
                continue
            config = configparser.ConfigParser()
            config.read(path)
            for section_name, section in config.items():
                api_type = section.get('api_type', 'workspace')
                if api_type != 'workspace':
                    continue
                host = section['host']
                token = section['token']
                clients[section_name] = self.token_auth(host, token=token)
        return clients

    @cache
    def default_account(self) -> AccountsApi:
        result = self.known_accounts().get("DEFAULT")
        if result is not None:
            return result
        raise ValueError("No account entries found in .databricks_cfg")

    @cache
    def known_accounts(self) -> Dict[str, AccountsApi]:
        clients = {}
        import os
        import configparser
        default = None
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
                clients[section_name] = AccountsApi(account_id, user, password)
                if default is None:
                    default = clients[section_name]
        if default is not None:
            clients["DEFAULT"] = default
        return clients


dbrest_factory = ApiClientFactory[DBAcademyRestClient](DBAcademyRestClient)
dougrest_factory = ApiClientFactory[DatabricksApi](DatabricksApi)
