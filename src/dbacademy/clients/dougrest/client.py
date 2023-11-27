__all__ = ["DatabricksApiClient"]

from typing import List

from dbacademy.clients.rest.common import *
from dbacademy.clients.dougrest.clusters import Clusters
from dbacademy.clients.dougrest.groups import Groups
from dbacademy.clients.dougrest.jobs import Jobs
from dbacademy.clients.dougrest.mlflow import MLFlow
from dbacademy.clients.dougrest.pools import Pools
from dbacademy.clients.dougrest.repos import Repos
from dbacademy.clients.dougrest.scim import SCIM, Users
from dbacademy.clients.dougrest.sql import Sql
from dbacademy.clients.dougrest.workspace import Workspace
from dbacademy.clients.dbrest.permissions_api import PermissionsApi


class DatabricksApiClient(dict, ApiClient):

    default_machine_types = {
        "AWS": "i4.xlarge",
        "Azure": "Standard_DS4_v5",
        "GCP": "n1-standard-4",
    }

    def __init__(self, hostname=None, *, token=None, username=None, password=None, authorization_header=None, deployment_name=None):
        from dbacademy import dbgems

        if hostname:
            self.__url = f'https://{hostname}'
        else:
            self.__url = dbgems.get_notebooks_api_endpoint()

        if not any((authorization_header, token, password)):
            token = dbgems.get_notebooks_api_token()

        super(dict, self).__init__(self.__url,
                                   token=token,
                                   username=username,
                                   password=password,
                                   authorization_header=authorization_header)

        if deployment_name is None:
            deployment_name = hostname[0:hostname.find(".")]

        self["deployment_name"] = deployment_name

        if hostname.endswith(".cloud.databricks.com"):
            self.__cloud = "AWS"
        elif hostname.endswith(".gcp.databricks.com"):
            self.__cloud = "GCP"
        elif hostname.endswith(".azuredatabricks.net"):
            self.__cloud = "Azure"
        else:
            raise ValueError(f"Unknown cloud for hostname: {hostname}")

        self.__default_machine_type = self.default_machine_types[self.cloud]
        self.__default_preloaded_versions = ["11.3.x-cpu-ml-scala2.12", "11.3.x-cpu-scala2.12"]
        self.__default_spark_version = self.default_preloaded_versions[0]

    @property
    def cloud(self) -> str:
        return self.__cloud

    @property
    def default_machine_type(self) -> str:
        return self.__default_machine_type

    @property
    def default_preloaded_versions(self) -> List[str]:
        return self.__default_preloaded_versions

    @property
    def default_spark_version(self) -> str:
        return self.__default_spark_version

    @property
    def url(self) -> str:
        return self.__url

    @property
    def clusters(self) -> Clusters:
        return Clusters(self)

    @property
    def groups(self) -> Groups:
        return Groups(self)

    @property
    def jobs(self) -> Jobs:
        return Jobs(self)

    @property
    def mlflow(self) -> MLFlow:
        return MLFlow(self)

    @property
    def pools(self) -> Pools:
        return Pools(self)

    @property
    def repos(self) -> Repos:
        return Repos(self)

    @property
    def scim(self) -> SCIM:
        return SCIM(self)

    @property
    def users(self) -> Users:
        return Users(self)

    @property
    def sql(self) -> Sql:
        return Sql(self)

    @property
    def workspace(self) -> Workspace:
        return Workspace(self)

    @property
    def permissions(self) -> PermissionsApi:
        from dbacademy.clients import dbrest

        dbrest_client = dbrest.from_client(client=self)
        return PermissionsApi(client=dbrest_client)
