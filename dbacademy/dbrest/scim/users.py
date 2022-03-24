from dbacademy.dbrest import DBAcademyRestClient

class ScimUsersClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client      # Client API exposing other operations to this class
        self.token = token        # The authentication token
        self.endpoint = endpoint  # The API endpoint

    def list(self):
        response = self.client.execute_get_json(f"{self.endpoint}/api/2.0/preview/scim/v2/Users")
        users = response.get("Resources", list())
        totalResults = response.get("totalResults")
        assert len(users) == int(totalResults), f"The totalResults ({totalResults}) does not match the number of records ({len(users)}) returned"
        return users

    def get_by_id(self, user_id):
        url = f"{self.endpoint}/api/2.0/preview/scim/v2/Users/{user_id}"
        return self.client.execute_get_json(url)

    def get_by_username(self, username):
        for user in self.list():
            if username == user.get("userName"):
                return user

        return None

    def create(self, username):
        payload = {
            "schemas": [ "urn:ietf:params:scim:schemas:core:2.0:User" ],
            "userName": username,
            "groups": [],
            "entitlements":[]
        }
        url = f"{self.endpoint}/api/2.0/preview/scim/v2/Users"
        return self.client.execute_post_json(url, payload)

    def to_users_list(self, users):
        if users is None:
            users = self.list()
        elif type(users) == str or type(users) == dict:
            users = [users] # Convert single argument users to a list
        else:
            assert type(users) == list, f"Expected the parameter \"users\" to be a list, found {type(users)}"

        new_users = list()

        for user in users:
            if type(user) == dict:
                new_users.append(user)

            elif type(user) == str:
                if "@" in user:
                    new_users.append(self.client.scim().users().get_by_username(user))
                else:
                    new_users.append(self.client.scim().users().get_by_id(user))

        return new_users
    
    def add_entitlement(self, user_id, entitlement):
        payload = {
            "schemas": [ "urn:ietf:params:scim:api:messages:2.0:PatchOp" ],
            "Operations": [
                {
                    "op": "add",
                    "path": "entitlements",
                    "value": [
                        {
                            "value": entitlement
                        }
                    ]
                }
            ]
        }
        url = f"{self.endpoint}/api/2.0/preview/scim/v2/Users/{user_id}"
        return self.client.execute_patch_json(url, payload)    

    def remove_entitlement(self, user_id, entitlement):
        payload = {
            "schemas": [ "urn:ietf:params:scim:api:messages:2.0:PatchOp" ],
            "Operations": [
                {
                    "op": "delete",
                    "path": "entitlements",
                    "value": [
                        {
                            "value": entitlement
                        }
                    ]
                }
            ]
        }
        url = f"{self.endpoint}/api/2.0/preview/scim/v2/Users/{user_id}"
        return self.client.execute_patch_json(url, payload)