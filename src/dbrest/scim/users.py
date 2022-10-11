from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class ScimUsersClient(ApiContainer):

    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class

    def list(self):
        response = self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/preview/scim/v2/Users")
        users = response.get("Resources", list())
        total_results = response.get("totalResults")
        assert len(users) == int(total_results), f"The totalResults ({total_results}) does not match the number of records ({len(users)}) returned"
        return users

    def get_by_id(self, user_id):
        url = f"{self.client.endpoint}/api/2.0/preview/scim/v2/Users/{user_id}"
        return self.client.execute_get_json(url)

    def get_by_username(self, username):
        return self.get_by_name(username)

    def get_by_name(self, name):
        for user in self.list():
            if name == user.get("userName"):
                return user

        return None

    def delete_by_id(self, user_id):
        url = f"{self.client.endpoint}/api/2.0/preview/scim/v2/Users/{user_id}"
        return self.client.execute_delete(url, expected=204)

    def delete_by_username(self, username):
        for user in self.list():
            if username == user.get("userName"):
                return self.delete_by_id(user.get("id"))

        return None

    def create(self, username):
        payload = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
            "userName": username,
            "groups": [],
            "entitlements": []
        }
        url = f"{self.client.endpoint}/api/2.0/preview/scim/v2/Users"
        return self.client.execute_post_json(url, payload, expected=[200, 201])

    def to_users_list(self, users):

        # One way or the other, we will use the full list
        all_users = self.list()

        if users is None:
            users = all_users
        elif type(users) == str or type(users) == dict:
            users = [users]  # Convert single argument users to a list
        else:
            assert type(users) == list, f"Expected the parameter \"users\" to be a list, found {type(users)}"

        new_users = list()

        for user in users:
            if type(user) == dict:
                new_users.append(user)

            elif type(user) == str:
                if "@" in user:
                    for u in all_users:
                        if u.get("userName") == user: 
                            new_users.append(u)
                else:
                    for u in all_users:
                        if u.get("id") == user: 
                            new_users.append(u)

        return new_users
    
    def add_entitlement(self, user_id, entitlement):
        payload = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
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
        url = f"{self.client.endpoint}/api/2.0/preview/scim/v2/Users/{user_id}"
        return self.client.execute_patch_json(url, payload)    

    def remove_entitlement(self, user_id, entitlement):
        payload = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
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
        url = f"{self.client.endpoint}/api/2.0/preview/scim/v2/Users/{user_id}"
        return self.client.execute_patch_json(url, payload)
