from typing import List, Dict, Any
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class ScimGroupsClient(ApiContainer):

    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class
        self.base_uri = f"{self.client.endpoint}/api/2.0/preview/scim/v2/Groups"

    def list(self) -> List[Dict[str, Any]]:
        response = self.client.api("GET", f"{self.base_uri}")
        users = response.get("Resources", list())
        total_results = response.get("totalResults")
        assert len(users) == int(total_results), f"The totalResults ({total_results}) does not match the number of records ({len(users)}) returned"
        return users

    def get_by_id(self, id_value: str) -> Dict[str, Any]:
        url = f"{self.base_uri}/{id_value}"
        return self.client.api("GET", url, _expected=[200, 404])

    def get_by_name(self, name: str) -> Dict[str, Any]:
        for group in self.list():
            if name == group.get("displayName"):
                return group

        return None

    def delete_by_id(self, id_value: str) -> None:
        url = f"{self.base_uri}/{id_value}"
        self.client.api("DELETE", url, _expected=204)
        return None

    def delete_by_name(self, name: str) -> None:
        for group in self.list():
            if name == group.get("displayName"):
                return self.delete_by_id(group.get("id"))

        return None

    def add_member(self, group_id: str, member_id: str) -> Dict[str, Any]:
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
        self.client.api("PATCH", f"{self.base_uri}/{group_id}", data)

    def create(self, name: str, *, members: List[str] = None, entitlements: List[str] = None) -> Dict[str, Any]:

        members = members or list()
        members_list: List[Dict[str, str]] = list()

        entitlements = entitlements or list()
        entitlements_list: List[Dict[str, str]] = list()

        params = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
            "displayName": name,
            "members": members_list,
            "entitlements": entitlements_list
        }

        for member in members:
            members_list.append({"value": member})

        for entitlement in entitlements:
            entitlements_list.append({"value": entitlement})

        return self.client.api("POST", self.base_uri, params)

    def add_entitlement(self, group_id: str, entitlement: str) -> Dict[str, Any]:
        params = {
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
        return self.client.api("PATCH", url, params)

    def remove_entitlement(self, group_id: str, entitlement: str) -> Dict[str, Any]:
        params = {
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
        return self.client.api("PATCH", url, params)
