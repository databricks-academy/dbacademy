from __future__ import annotations

__all__ = ["AccountsApi"]

from typing import Union
from dbacademy.clients.rest.common import ApiClient
from dbacademy.common import Cloud
from dbacademy.common import validate


class AccountsApi(ApiClient):

    def __init__(self, account_id: str, *,
                 username: str = None, password: str = None,
                 token: str = None,
                 cloud: Union[str, Cloud] = Cloud.AWS) -> None:

        cloud = validate(cloud=cloud).required.enum(Cloud, auto_convert=True)

        if cloud == Cloud.AWS:
            endpoint = f'https://accounts.cloud.databricks.com/api/2.0/accounts/{account_id}'

        elif cloud == Cloud.GCP:
            endpoint = f'https://accounts.gcp.databricks.com/api/2.0/accounts/{account_id}'

        elif cloud == Cloud.MSA:
            endpoint = f'https://accounts.azuredatabricks.net/api/2.0/accounts/{account_id}'

        else:
            raise ValueError(f"Cloud must be AWS, GCP, or MSA.  Found: {cloud!r}")

        super().__init__(endpoint, username=username, password=password, token=token)

        self.session.headers["X-Databricks-Account-Console-API-Version"] = "2.0"

        self.account_id = account_id

        from dbacademy.clients.dougrest.accounts.budgets import Budgets
        self.budgets = Budgets(self)

        from dbacademy.clients.dougrest.accounts.credentials import Credentials
        self.credentials = Credentials(self)

        from dbacademy.clients.dougrest.accounts.keys import CustomerManagedKeys
        self.keys = CustomerManagedKeys(self)

        from dbacademy.clients.dougrest.accounts.logs import LogDeliveryConfigurations
        self.logs = LogDeliveryConfigurations(self)

        from dbacademy.clients.dougrest.accounts.metastores import Metastores
        self.metastores = Metastores(self)

        from dbacademy.clients.dougrest.accounts.network import NetworkConfigurations
        self.networks = NetworkConfigurations(self)

        from dbacademy.clients.dougrest.accounts.private_access import PrivateAccessSettings
        self.private_access = PrivateAccessSettings(self)

        from dbacademy.clients.dougrest.accounts.storage import StorageConfigurations
        self.storage = StorageConfigurations(self)

        from dbacademy.clients.dougrest.accounts.users import Users
        self.users = Users(self)

        from dbacademy.clients.dougrest.accounts.vpc import VpcEndpoints
        self.vpc = VpcEndpoints(self)

        from dbacademy.clients.dougrest.accounts.workspaces import Workspaces
        self.workspaces = Workspaces(self)
