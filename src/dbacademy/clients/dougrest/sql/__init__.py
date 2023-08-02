from dbacademy.clients.dougrest.sql.warehouses import Warehouses
from dbacademy.rest.common import ApiContainer


class Sql(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks
        self.endpoints = Warehouses(databricks)
        self.warehouses = Warehouses(databricks)
