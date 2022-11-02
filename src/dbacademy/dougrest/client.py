from __future__ import annotations

from typing import Dict

from dbacademy.common import CachedStaticProperty
from dbacademy.rest.common import *

__all__ = ["DatabricksApi", "DatabricksApiException"]


class DatabricksApi(dict, ApiClient):
    from configparser import ConfigParser

    PROFILE_DEFAULT = "DEFAULT"
    PROFILE_ENVIRONMENT = "ENVIRONMENT"

    ENV_DATABRICKS_HOST = "DATABRICKS_HOST"
    ENV_DATABRICKS_TOKEN = "DATABRICKS_TOKEN"

    SECTION_HOST = "host"
    SECTION_TOKEN = "token"

    default_machine_types = {
        "AWS": "i4.xlarge",
        "Azure": "Standard_DS4_v5",
        "GCP": "n1-standard-4",
    }

    # noinspection PyMethodParameters
    @CachedStaticProperty
    def default_client() -> DatabricksApi:

        # Give precedence to config defined in the environment.
        result = DatabricksApi.known_clients.get(DatabricksApi.PROFILE_ENVIRONMENT)
        result = result or DatabricksApi.known_clients.get(DatabricksApi.PROFILE_DEFAULT)

        if result is None:
            result = DatabricksApi()

        return result

    # noinspection PyMethodParameters
    @CachedStaticProperty
    def known_clients() -> Dict[str, DatabricksApi]:
        import os, configparser

        clients = {}

        for path in ('.databrickscfg', '~/.databrickscfg'):
            path = os.path.expanduser(path)
            if not os.path.exists(path):
                continue

            config = configparser.ConfigParser()
            config.read(path)
            DatabricksApi.read_env(config)

            for section_name, section in config.items():
                api_type = section.get('api_type', 'workspace')
                if api_type != 'workspace':
                    continue
                host = section[DatabricksApi.SECTION_HOST].rstrip("/")[8:]
                token = section[DatabricksApi.SECTION_TOKEN]
                clients[section_name] = DatabricksApi(host, token=token)

        return clients

    @staticmethod
    def read_env(config: ConfigParser):
        import os

        host = os.getenv(DatabricksApi.ENV_DATABRICKS_HOST)
        token = os.getenv(DatabricksApi.ENV_DATABRICKS_TOKEN)

        if host and token:
            config.read_dict({
                DatabricksApi.PROFILE_ENVIRONMENT: {
                    DatabricksApi.SECTION_HOST: host,
                    DatabricksApi.SECTION_TOKEN: token
                }
            })

    def __init__(self, hostname=None, *, token=None, user=None, password=None, authorization_header=None, deployment_name=None):
        from dbacademy import dbgems
        if hostname:
            url = f'https://{hostname}/api/'
        else:
            url = dbgems.get_notebooks_api_endpoint() + "/api/"
        if not any((authorization_header, token, password)):
            token = dbgems.get_notebooks_api_token()
        super(dict, self).__init__(url, token=token, user=user, password=password,
                                   authorization_header=authorization_header)
        if deployment_name is None:
            deployment_name = hostname[0:hostname.find(".")]
        self["deployment_name"] = deployment_name
        if hostname.endswith(".cloud.databricks.com"):
            self.cloud = "AWS"
        elif hostname.endswith(".gcp.databricks.com"):
            self.cloud = "GCP"
        elif hostname.endswith(".azuredatabricks.net"):
            self.cloud = "Azure"
        else:
            raise ValueError(f"Unknown cloud for hostname: {hostname}")
        self.default_machine_type = DatabricksApi.default_machine_types[self.cloud]
        self.default_preloaded_versions = ["11.3.x-cpu-ml-scala2.12", "11.3.x-cpu-scala2.12"]
        self.default_spark_version = self.default_preloaded_versions[0]
        from dbacademy.dougrest.clusters import Clusters
        self.clusters = Clusters(self)
        from dbacademy.dougrest.groups import Groups
        self.groups = Groups(self)
        from dbacademy.dougrest.jobs import Jobs
        self.jobs = Jobs(self)
        from dbacademy.dougrest.mlflow import MLFlow
        self.mlflow = MLFlow(self)
        from dbacademy.dougrest.pools import Pools
        self.pools = Pools(self)
        from dbacademy.dougrest.repos import Repos
        self.repos = Repos(self)
        from dbacademy.dougrest.scim import SCIM, Users
        self.scim = SCIM(self)
        self.users = Users(self)
        from dbacademy.dougrest.sql import Sql
        self.sql = Sql(self)
        from dbacademy.dougrest.workspace import Workspace
        self.workspace = Workspace(self)
        from dbacademy.rest.permissions import Permissions
        self.permissions = Permissions(self)
