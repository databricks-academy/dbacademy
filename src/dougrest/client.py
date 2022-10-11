from __future__ import annotations

from typing import Dict

from dbacademy.rest.common import *

__all__ = ["DatabricksApi", "DatabricksApiException"]


class DatabricksApi(dict, ApiClient):

    default_machine_types = {
        "AWS": "i3.xlarge",
        "Azure": "Standard_DS3_v2",
        "GCP": "n1-standard-4",
    }

    @CachedStaticProperty
    def default_client() -> DatabricksApi:
        result = DatabricksApi.known_clients.get("DEFAULT")
        if result is None:
            result = DatabricksApi()
        return result

    @CachedStaticProperty
    def known_clients() -> Dict[str, DatabricksApi]:
        clients = {}
        import os
        import configparser
        for path in ('.databrickscfg', '~/.databrickscfg'):
            path = os.path.expanduser(path)
            if not os.path.exists(path):
                continue
            config = configparser.ConfigParser()
            config.read(path)
            for section_name, section in config.items():
                api_type = section.get('api_type', 'workspace')
                if api_type != 'workspace':
                    continue
                host = section['host'].rstrip("/")[8:]
                token = section['token']
                clients[section_name] = DatabricksApi(host, token=token)
        return clients

    def __init__(self, hostname=None, *, token=None, user=None, password=None, authorization_header=None, cloud="AWS",
                 deployment_name=None):
        from dbacademy.rest.dbgems_fix import dbgems
        if hostname:
            url = f'https://{hostname}/api/'
        else:
            url = dbgems.get_notebooks_api_endpoint() + "/api/"
        if not any((authorization_header, token, password)):
            token = dbgems.get_notebooks_api_token()
        super(dict, self).__init__(url, token=token, user=user, password=password,
                                   authorization_header=authorization_header)
        self["deployment_name"] = deployment_name
        self.cloud = cloud
        self.default_machine_type = DatabricksApi.default_machine_types[self.cloud]
        self.default_preloaded_versions = ["10.4.x-cpu-ml-scala2.12", "10.4.x-cpu-scala2.12"]
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
