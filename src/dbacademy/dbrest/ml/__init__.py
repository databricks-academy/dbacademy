from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.clients.rest.common import ApiContainer


class MlClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class

        from dbacademy.dbrest.ml.feature_store import FeatureStoreClient
        self.feature_store = FeatureStoreClient(self.client)

        from dbacademy.dbrest.ml.mlflow_endpoints import MLflowEndpointsClient
        self.mlflow_endpoints = MLflowEndpointsClient(self.client)

        from dbacademy.dbrest.ml.mlflow_models import MLflowModelsClient
        self.mlflow_models = MLflowModelsClient(self.client)

        from dbacademy.dbrest.ml.mlflow_model_versions import MLflowModelVersionsClient
        self.mlflow_model_versions = MLflowModelVersionsClient(self.client)
