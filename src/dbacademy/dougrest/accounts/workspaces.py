from typing import Union, Container, Dict

from overrides import overrides

import time

from dbacademy.dougrest.accounts.crud import AccountsCRUD, IfNotExists, IfExists
from dbacademy.dougrest.client import DatabricksApi, DatabricksApiException


class Workspace(DatabricksApi):
    def __init__(self, data_dict, accounts_api):
        hostname = data_dict.get("deployment_name")
        auth = accounts_api.session.headers["Authorization"]
        self.accounts = accounts_api
        self.user = accounts_api.user
        super().__init__(hostname + ".cloud.databricks.com",
                         user=self.user,
                         authorization_header=auth)
        self.update(data_dict)

    def wait_until_ready(self, timeout_seconds=30*60):
        start = time.time()
        while self["workspace_status"] == "PROVISIONING":
            workspace_id = self["workspace_id"]
            data = self.accounts.workspaces.get_by_id(workspace_id)
            self.update(data)
            if time.time() - start > timeout_seconds:
                raise TimeoutError(f"Workspace not ready after waiting {timeout_seconds} seconds")
            if self["workspace_status"] == "PROVISIONING":
                time.sleep(15)

    @overrides
    def api(self, http_method: str, endpoint_path: str, data=None, *,
            expected: Union[int, Container[int]] = None) -> Union[str, Dict]:
        self.wait_until_ready()
        return super().api(http_method, endpoint_path, data)


class Workspaces(AccountsCRUD):
    def __init__(self, client):
        super().__init__(client, "/workspaces", "workspace")

    @overrides
    def _wrap(self, item: dict) -> Workspace:
        return Workspace(item, self.client)

    def get_by_deployment_name(self, name, if_not_exists: IfNotExists = "error"):
        """
        Returns the first {singular} found that with the given deployment_name.
        Raises exception if not found.
        """
        result = next((item for item in self._list() if item["deployment_name"] == name), None)
        if result is None and if_not_exists == "error":
            raise DatabricksApiException(f"{self.singular} with deployment_name '{name}' not found", 404)
        return self._wrap(result)

    @overrides(check_signature=False)
    def create(self, workspace_name, *, deployment_name=None, region, pricing_tier=None,
               credentials=None, credentials_id=None, credentials_name=None,
               storage_configuration=None, storage_configuration_id=None, storage_configuration_name=None,
               network=None, network_id=None, network_name=None,
               private_access_settings=None, private_access_settings_id=None, private_access_settings_name=None,
               services_encryption_key=None, services_encryption_key_id=None, services_encryption_key_name=None,
               storage_encryption_key=None, storage_encryption_key_id=None, storage_encryption_key_name=None,
               fetch: bool = None, if_exists: IfExists = "error"):

        if credentials_id:
            pass
        elif credentials:
            credentials_id = credentials[f"credentials_id"]
        elif credentials_name:
            credentials_id = self.client.credentials.get_by_name(credentials_name)["credentials_id"]
        else:
            raise DatabricksApiException("Must provide one of credentials, credentials_id, or credentials_name")

        if storage_configuration_id:
            pass
        elif storage_configuration:
            storage_configuration_id = storage_configuration[f"storage_configuration_id"]
        elif storage_configuration_name:
            storage_configuration_id = self.client.storage.get_by_name(storage_configuration_name)[
                "storage_configuration_id"]
        else:
            raise DatabricksApiException("Must provide one of credentials, credentials_id, or credentials_name")

        if network_id:
            pass
        elif network:
            network_id = network[f"network_id"]
        elif network_name:
            network_id = self.client.networks.get_by_name(network_name)["network_id"]

        if private_access_settings_id:
            pass
        elif private_access_settings:
            private_access_settings_id = private_access_settings[f"private_access_settings_id"]
        elif private_access_settings_name:
            private_access_settings_id = self.client.private_access.get_by_name(private_access_settings_name)[
                "private_access_settings_id"]

        if services_encryption_key_id:
            pass
        elif services_encryption_key:
            services_encryption_key_id = services_encryption_key[f"customer_managed_key_id"]
        elif services_encryption_key_name:
            services_encryption_key_id = self.client.keys.get_by_name(services_encryption_key_name)[
                "customer_managed_key_id"]

        if storage_encryption_key_id:
            pass
        elif storage_encryption_key:
            storage_encryption_key_id = storage_encryption_key[f"customer_managed_key_id"]
        elif storage_encryption_key_name:
            storage_encryption_key_id = self.client.keys.get_by_name(storage_encryption_key_name)[
                "customer_managed_key_id"]

        spec = {
            "workspace_name": workspace_name,
            "deployment_name": deployment_name,
            "aws_region": region,
            "pricing_tier": pricing_tier,
            "credentials_id": credentials_id,
            "storage_configuration_id": storage_configuration_id,
            "network_id": network_id,
            "private_access_settings_id": private_access_settings_id,
            "managed_services_customer_managed_key_id": services_encryption_key_id,
            "storage_customer_managed_key_id": storage_encryption_key_id,
        }
        for key, value in list(spec.items()):
            if value is None or value == "":
                del spec[key]
        return self.create_by_example(spec, fetch=fetch, if_exists=if_exists)
