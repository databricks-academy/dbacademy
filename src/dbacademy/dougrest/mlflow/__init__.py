from dbacademy.dougrest.mlflow.models import RegisteredModels
from dbacademy.dougrest.mlflow.versions import ModelVersions
from dbacademy.rest.common import ApiContainer


class MLFlow(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks
        self.registered_models = RegisteredModels(databricks)
        self.models = RegisteredModels(databricks)
        self.model_versions = ModelVersions(databricks)
        self.versions = ModelVersions(databricks)
