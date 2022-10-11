from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class ScimServicePrincipalsClient(ApiContainer):

    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class
        self.base_url = f"{self.client.endpoint}/api/2.0/preview/scim/v2/ServicePrincipals"

    def list(self):
        response = self.client.execute_get_json(f"{self.base_url}")
        all_items = response.get("Resources", [])

        total = response.get("totalResults")
        while len(all_items) != total:
            response = self.client.execute_get_json(f"{self.base_url}")
            total = response.get("totalResults")
            items = response.get("Resources", [])
            all_items.extend(items)

        return all_items

    def get_by_id(self, service_principle_id: str):
        return self.client.execute_get_json(f"{self.base_url}/{service_principle_id}")

    def get_by_name(self, display_name):
        all_items = self.list()
        for item in all_items:
            if item.get("displayName") == display_name:
                return item
        return None

    def create(self, display_name: str, group_ids: list = [], entitlements: list = []):
        params = {
            "displayName": display_name,
            "entitlements": [],
            "groups": [],
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
            "active": True
        }

        group_ids = group_ids if group_ids is not None else list()
        for group_id in group_ids:
            value = {"value": group_id}
            params["groups"].append(value)

        entitlements = entitlements if entitlements is not None else list()
        for entitlement in entitlements:
            value = {"value": entitlement}
            params["entitlements"].append(value)

        return self.client.execute_post_json(f"{self.base_url}", params, expected=201)
