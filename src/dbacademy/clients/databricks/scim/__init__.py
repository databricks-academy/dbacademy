__all__ = ["ScimClient"]

from dbacademy.clients.rest.common import ApiClient, ApiContainer


class ScimClient(ApiContainer):
    def __init__(self, client: ApiClient):
        self.client = client      # Client API exposing other operations to this class

        from dbacademy.clients.databricks.scim.users import ScimUsersClient
        self.users = ScimUsersClient(self.client)

        from dbacademy.clients.databricks.scim.service_principals import ScimServicePrincipalsClient
        self.service_principals = ScimServicePrincipalsClient(self.client)

        from dbacademy.clients.databricks.scim.groups import ScimGroupsClient
        self.groups = ScimGroupsClient(self.client)

    @property
    def me(self):
        raise Exception("The me() client is not yet supported.")
        # from dbacademy.clients.databricks.scim.me import ScimMeClient
        # return ScimMeClient(self, self)
