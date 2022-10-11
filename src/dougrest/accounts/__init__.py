from __future__ import annotations

from typing import Dict

from dbacademy.dougrest.accounts.budgets import Budgets
from dbacademy.dougrest.accounts.credentials import Credentials
from dbacademy.dougrest.accounts.keys import CustomerManagedKeys
from dbacademy.dougrest.accounts.logs import LogDeliveryConfigurations
from dbacademy.dougrest.accounts.network import NetworkConfigurations
from dbacademy.dougrest.accounts.private_access import PrivateAccessSettings
from dbacademy.dougrest.accounts.storage import StorageConfigurations
from dbacademy.dougrest.accounts.vpc import VpcEndpoints
from dbacademy.dougrest.accounts.workspaces import Workspaces
from dbacademy.rest.common import *

__all__ = ["AccountsApi"]


class AccountsApi(ApiClient):

    @CachedStaticProperty
    def default_account() -> Dict[str, AccountsApi]:
        result = AccountsApi.known_accounts.get("DEFAULT")
        if result is not None:
            return result
        raise ValueError("No account entries found in .databricks_cfg")

    @CachedStaticProperty
    def known_accounts() -> Dict[str, AccountsApi]:
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

    def __init__(self, account_id: str, user: str, password: str) -> None:
        url = f'https://accounts.cloud.databricks.com/api/2.0/accounts/{account_id}'
        super().__init__(url, user=user, password=password)
        self.user = user
        self.account_id = account_id
        self.credentials = Credentials(self)
        self.storage = StorageConfigurations(self)
        self.networks = NetworkConfigurations(self)
        self.keys = CustomerManagedKeys(self)
        self.logs = LogDeliveryConfigurations(self)
        self.vpc = VpcEndpoints(self)
        self.budgets = Budgets(self)
        self.workspaces = Workspaces(self)
        self.private_access = PrivateAccessSettings(self)
