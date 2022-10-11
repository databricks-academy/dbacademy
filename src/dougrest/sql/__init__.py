from dbacademy.dougrest.sql.endpoints import Endpoints
from dbacademy.rest.common import ApiContainer


class Sql(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks
        self.endpoints = Endpoints(databricks)
