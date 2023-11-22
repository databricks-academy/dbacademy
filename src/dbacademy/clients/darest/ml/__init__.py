__all__ = ["MlClient"]

from dbacademy.clients.rest.common import ApiClient, ApiContainer


class MlClient(ApiContainer):
    def __init__(self, client: ApiClient):
        self.client = client      # Client API exposing other operations to this class

        from dbacademy.clients.databricks.ml.feature_store import FeatureStoreClient
        self.feature_store = FeatureStoreClient(self.client)

        from dbacademy.clients.databricks.ml.mlflow_endpoints import MLflowEndpointsClient
        self.mlflow_endpoints = MLflowEndpointsClient(self.client)

        from dbacademy.clients.databricks.ml.mlflow_models import MLflowModelsClient
        self.mlflow_models = MLflowModelsClient(self.client)

        from dbacademy.clients.databricks.ml.mlflow_model_versions import MLflowModelVersionsClient
        self.mlflow_model_versions = MLflowModelVersionsClient(self.client)
