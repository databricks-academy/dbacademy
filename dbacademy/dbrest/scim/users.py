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

    def get_by_name(self, username):
        for user in self.list():
            if username == user.get("userName"):
                return user

        return None
    
    def add_entitlement(self, user_id, entitlement):
        payload = {
            # "schemas": [ "urn:ietf:params:scim:api:messages:2.0:PatchOp" ],
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