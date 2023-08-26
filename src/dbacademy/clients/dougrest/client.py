from __future__ import annotations

from dbacademy.clients.rest.common import *

__all__ = ["DatabricksApi", "DatabricksApiException"]


class DatabricksApi(dict, ApiClient):

    default_machine_types = {
        "AWS": "i4.xlarge",
        "Azure": "Standard_DS4_v5",
        "GCP": "n1-standard-4",
    }

    def __init__(self, hostname=None, *, token=None, user=None, password=None, authorization_header=None, deployment_name=None):
        from dbacademy import dbgems

        if hostname:
            url = f'https://{hostname}/api/'
        else:
            url = dbgems.get_notebooks_api_endpoint() + "/api/"

        if not any((authorization_header, token, password)):
            token = dbgems.get_notebooks_api_token()

        super(dict, self).__init__(url,
                                   token=token,
                                   user=user,
                                   password=password,
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

        from dbacademy.clients.dougrest.clusters import Clusters
        self.clusters = Clusters(self)

        from dbacademy.clients.dougrest.groups import Groups
        self.groups = Groups(self)

        from dbacademy.clients.dougrest.jobs import Jobs
        self.jobs = Jobs(self)

        from dbacademy.clients.dougrest.mlflow import MLFlow
        self.mlflow = MLFlow(self)

        from dbacademy.clients.dougrest.pools import Pools
        self.pools = Pools(self)

        from dbacademy.clients.dougrest.repos import Repos
        self.repos = Repos(self)

        from dbacademy.clients.dougrest.scim import SCIM, Users
        self.scim = SCIM(self)
        self.users = Users(self)

        from dbacademy.clients.dougrest.sql import Sql
        self.sql = Sql(self)

        from dbacademy.clients.dougrest.workspace import Workspace
        self.workspace = Workspace(self)

        from dbacademy.dbrest.permissions import Permissions
        from dbacademy.dbrest.client import DBAcademyRestClient

        dbrest_client = DBAcademyRestClient(client=self)
        self.permissions = Permissions(client=dbrest_client)
