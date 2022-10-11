from dbacademy.dougrest.accounts import AccountsApi
from dbacademy.dougrest.client import *

__all__ = ["AccountsApi", "DatabricksApi", "DatabricksApiException", "databricks"]

databricks = DatabricksApi.default_client
