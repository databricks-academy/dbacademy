from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class MlClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class

        from dbacademy.dbrest.ml.feature_store import FeatureStoreClient
        self.feature_store = FeatureStoreClient(self.client)

        from dbacademy.dbrest.ml.mlflow import MLflowClient
        self.mlflow = MLflowClient(self.client)
