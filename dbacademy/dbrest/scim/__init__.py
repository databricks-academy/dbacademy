from dbacademy.dbrest import DBAcademyRestClient

class ScimClient():
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client      # Client API exposing other operations to this class
        self.token = token        # The authentication token
        self.endpoint = endpoint  # The API endpoint

    def me(self):
        raise Exception("The me() client is not yet supported.")
        # from dbacademy.dbrest.scim.me import ScimMeClient
        # return ScimMeClient(self, self.token, self.endpoint)

    def users(self):
        from dbacademy.dbrest.users import ScimUsersClient
        return ScimUsersClient(self.client, self.token, self.endpoint)

    def service_principals(self):
        raise Exception("The service_principals() client is not yet supported.")
        # from dbacademy.dbrest.scim.sp import ScimServicePrincipalsClient
        # return ScimServicePrincipalsClient(self, self.token, self.endpoint)

    def groups(self):
        raise Exception("The groups() client is not yet supported.")
        # from dbacademy.dbrest.scim.groups import ScimGroupsClient
        # return ScimGroupsClient(self, self.token, self.endpoint)
