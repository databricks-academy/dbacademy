from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class ScimClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class

        from dbacademy.dbrest.scim.users import ScimUsersClient
        self.users = ScimUsersClient(self.client)

        from dbacademy.dbrest.scim.service_principals import ScimServicePrincipalsClient
        self.service_principals = ScimServicePrincipalsClient(self.client)

        from dbacademy.dbrest.scim.groups import ScimGroupsClient
        self.groups = ScimGroupsClient(self.client)

    @property
    def me(self):
        raise Exception("The me() client is not yet supported.")
        # from dbacademy.dbrest.scim.me import ScimMeClient
        # return ScimMeClient(self, self)
