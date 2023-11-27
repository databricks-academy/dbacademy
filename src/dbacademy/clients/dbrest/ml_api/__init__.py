__all__ = ["MlApi"]
# Code Review: JDP on 11-27-2023

from dbacademy.common import validate
from dbacademy.clients.rest.common import ApiClient, ApiContainer
from dbacademy.clients.dbrest.ml_api.feature_store_api import FeatureStoreApi
from dbacademy.clients.dbrest.ml_api.mlflow_endpoints_api import MLflowEndpointsApi
from dbacademy.clients.dbrest.ml_api.mlflow_models_api import MLflowModelsApi
from dbacademy.clients.dbrest.ml_api.mlflow_model_versions_api import MLflowModelVersionsApi


class MlApi(ApiContainer):
    def __init__(self, client: ApiClient):
        self.__client = validate(client=client).required.as_type(ApiClient)

    @property
    def feature_store(self) -> FeatureStoreApi:
        return FeatureStoreApi(self.__client)

    @property
    def mlflow_endpoints(self) -> MLflowEndpointsApi:
        return MLflowEndpointsApi(self.__client)

    @property
    def mlflow_models(self) -> MLflowModelsApi:
        return MLflowModelsApi(self.__client)

    @property
    def mlflow_model_versions(self) -> MLflowModelVersionsApi:
        return MLflowModelVersionsApi(self.__client)
