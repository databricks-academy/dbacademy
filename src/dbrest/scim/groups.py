from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class ScimGroupsClient(ApiContainer):

    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class
        self.base_uri = f"{self.client.endpoint}/api/2.0/preview/scim/v2/Groups"

    def list(self):
        response = self.client.execute_get_json(f"{self.base_uri}")
        users = response.get("Resources", list())
        total_results = response.get("totalResults")
        assert len(users) == int(total_results), f"The totalResults ({total_results}) does not match the number of records ({len(users)}) returned"
        return users

    def get_by_id(self, id):
        url = f"{self.base_uri}/{id}"
        return self.client.execute_get_json(url)

    def get_by_name(self, name):
        for group in self.list():
            if name == group.get("displayName"):
                return group

        return None

    def delete_by_id(self, id):
        url = f"{self.base_uri}/{id}"
        return self.client.execute_delete(url, expected=204)

    def delete_by_name(self, name):
        for group in self.list():
            if name == name.get("name"):
                return self.delete_by_id(group.get("id"))

        return None

    def add_member(self, group_id:str, member_id: str):
        data = {
                  "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                  "Operations": [
                    {
                      "op": "add",
                      "value": {
                        "members": [
                          {
                            "value": member_id
                          }
                        ]
                      }
                    }
                  ]
               }
        self.client.execute_patch_json(f"{self.base_uri}/{group_id}", params=data)

    # def create(self, name):
    #     payload = {
    #         "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
    #         "name": name,
    #         "groups": [],
    #         "entitlements": []
    #     }
    #     url = f"{self.client.endpoint}/api/2.0/preview/scim/v2/Groups"
    #     return self.client.execute_post_json(url, payload, expected=[200, 201])

    def add_entitlement(self, group_id, entitlement):
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
        url = f"{self.base_uri}/{group_id}"
        return self.client.execute_patch_json(url, payload)

    def remove_entitlement(self, group_id, entitlement):
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
        url = f"{self.base_uri}/{group_id}"
        return self.client.execute_patch_json(url, payload)
