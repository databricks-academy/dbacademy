from __future__ import annotations

from dbacademy.clients.dougrest.accounts.budgets import Budgets
from dbacademy.clients.dougrest.accounts.credentials import Credentials
from dbacademy.clients.dougrest.accounts.keys import CustomerManagedKeys
from dbacademy.clients.dougrest.accounts.logs import LogDeliveryConfigurations
from dbacademy.clients.dougrest.accounts.metastores import Metastores
from dbacademy.clients.dougrest.accounts.network import NetworkConfigurations
from dbacademy.clients.dougrest.accounts.private_access import PrivateAccessSettings
from dbacademy.clients.dougrest.accounts.storage import StorageConfigurations
from dbacademy.clients.dougrest.accounts.users import Users
from dbacademy.clients.dougrest.accounts.vpc import VpcEndpoints
from dbacademy.clients.dougrest.accounts.workspaces import Workspaces
from dbacademy.clients.rest.common import *

__all__ = ["AccountsApi"]


class AccountsApi(ApiClient):

    def __init__(self, account_id: str, *,
                 user: str = None, password: str = None,
                 token: str = None,
                 cloud: Cloud = "AWS") -> None:
        if cloud == "AWS":
            url = f'https://accounts.cloud.databricks.com/api/2.0/accounts/{account_id}'
        elif cloud == "GCP":
            url = f'https://accounts.gcp.databricks.com/api/2.0/accounts/{account_id}'
        elif cloud == "MSA":
            url = f'https://accounts.azuredatabricks.net/api/2.0/accounts/{account_id}'
        else:
            raise ValueError(f"Cloud must be AWS, GCP, or MSA.  Found: {cloud!r}")
        super().__init__(url, user=user, password=password, token=token)
        self.session.headers["X-Databricks-Account-Console-API-Version"] = "2.0"
        self.user = user
        self.account_id = account_id
        self.budgets = Budgets(self)
        self.credentials = Credentials(self)
        self.keys = CustomerManagedKeys(self)
        self.logs = LogDeliveryConfigurations(self)
        self.metastores = Metastores(self)
        self.networks = NetworkConfigurations(self)
        self.private_access = PrivateAccessSettings(self)
        self.storage = StorageConfigurations(self)
        self.users = Users(self)
        self.vpc = VpcEndpoints(self)
        self.workspaces = Workspaces(self)
