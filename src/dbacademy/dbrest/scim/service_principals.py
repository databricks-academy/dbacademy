from typing import List
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.clients.rest.common import ApiContainer


class ScimServicePrincipalsClient(ApiContainer):

    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class
        self.base_url = f"{self.client.endpoint}/api/2.0/preview/scim/v2/ServicePrincipals"

    def list(self):
        response = self.client.api("GET", f"{self.base_url}")
        all_items = response.get("Resources", [])

        total = response.get("totalResults")
        while len(all_items) != total:
            response = self.client.api("GET", f"{self.base_url}")
            total = response.get("totalResults")
            items = response.get("Resources", [])
            all_items.extend(items)

        return all_items

    def get_by_id(self, service_principle_id: str):
        return self.client.api("GET", f"{self.base_url}/{service_principle_id}")

    def get_by_name(self, display_name):
        all_items = self.list()
        for item in all_items:
            if item.get("displayName") == display_name:
                return item
        return None

    def create(self, display_name: str, group_ids: List = None, entitlements: List = None):
        from uuid import uuid4
        entitlements = entitlements or list()

        params = {
            "displayName": display_name,
            "entitlements": [],
            "groups": [],
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
            "active": True,
        }

        if not self.client.endpoint.endswith("gcp.databricks.com"):
            params["applicationId"] = str(uuid4())

        group_ids = group_ids or list()
        group_ids = group_ids if group_ids is not None else list()

        for group_id in group_ids:
            value = {"value": group_id}
            params["groups"].append(value)

        entitlements = entitlements if entitlements is not None else list()
        for entitlement in entitlements:
            value = {"value": entitlement}
            params["entitlements"].append(value)

        return self.client.api("POST", f"{self.base_url}", params, _expected=201)
