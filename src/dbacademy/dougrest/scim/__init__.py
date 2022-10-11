from dbacademy.dougrest.scim.groups import Groups
from dbacademy.dougrest.scim.users import Users
from dbacademy.rest.common import ApiContainer


class SCIM(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks
        self.users = Users(databricks)
        self.groups = Groups(databricks)
