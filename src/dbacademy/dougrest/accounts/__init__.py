from __future__ import annotations

from dbacademy.dougrest.accounts.budgets import Budgets
from dbacademy.dougrest.accounts.credentials import Credentials
from dbacademy.dougrest.accounts.keys import CustomerManagedKeys
from dbacademy.dougrest.accounts.logs import LogDeliveryConfigurations
from dbacademy.dougrest.accounts.metastores import Metastores
from dbacademy.dougrest.accounts.network import NetworkConfigurations
from dbacademy.dougrest.accounts.private_access import PrivateAccessSettings
from dbacademy.dougrest.accounts.storage import StorageConfigurations
from dbacademy.dougrest.accounts.users import Users
from dbacademy.dougrest.accounts.vpc import VpcEndpoints
from dbacademy.dougrest.accounts.workspaces import Workspaces
from dbacademy.rest.common import *

__all__ = ["AccountsApi"]


class AccountsApi(ApiClient):

    def __init__(self, account_id: str, *,
                 user: str = None, password: str = None,
                 token: str = None, cloud: Cloud) -> None:
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
