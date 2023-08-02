from typing import List, Dict

from dbacademy.rest.common import ApiContainer, DatabricksApiException, IfExists


class Users(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks
        self.path = "2.0/preview"

    def list(self, start=1, count=1000, *, exclude_roles=False):
        kwargs = {"excludedAttributes": "roles"} if exclude_roles else {}
        return self.databricks.api("GET", f"{self.path}/scim/v2/Users",
                                   startIndex=start, count=count, **kwargs).get("Resources", [])

    def list_usernames(self):
        return sorted([u["userName"] for u in self.list(exclude_roles=True)])

    def list_by_username(self):
        return {u["userName"]: u for u in self.list()}

    def get_by_id(self, user_id):
        return self.databricks.api("GET", f"{self.path}/scim/v2/Users/{user_id}")

    def get_by_username(self, username, if_not_exists="ignore"):
        for u in self.list():
            if u["userName"] == username:
                return u
        if if_not_exists == "error":
            raise DatabricksApiException(f"User({username!r}) not found", 404)

    def overwrite(self, user: dict):
        user_id = user["id"]
        return self.databricks.api("PUT", f"{self.path}/scim/v2/Users/{user_id}", _data=user)

    def patch(self, user: dict, operations: List[Dict]):
        user_id = user["id"]
        data = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": operations
        }
        return self.databricks.api("PATCH", f"{self.path}/scim/v2/Users/{user_id}", _data=data)

    def set_entitlements(self, user: dict, entitlements: Dict[str, bool]):
        adds = []
        removes = []
        for entitlement_name, entitlement_value in entitlements.items():
            if entitlement_value is None:
                pass
            elif entitlement_value:
                adds.append(entitlement_name)
            else:
                removes.append(entitlement_name)
        operations = []
        if adds:
            operations.append({
                "op": "add",
                "path": "entitlements",
                "value": [{"value": entitlement_name} for entitlement_name in adds],
            })
        if removes:
            query = " or " .join([f"value eq \"{entitlement_name}\"" for entitlement_name in removes])
            operations.append({
                "op": "remove",
                "path": f"entitlements[{query}]",
            })
        if operations:
            return self.patch(user, operations)

    def set_cluster_create(self, user: dict, cluster_create: bool = None, pool_create: bool = None):
        entitlements = {
            "allow-cluster-create": cluster_create,
            "allow-instance-pool-create": pool_create,
        }
        return self.set_entitlements(user, entitlements)

    # TODO - remove allow_cluster_create, use entitlements instead
    def create(self, username, *, allow_cluster_create=True, if_exists: IfExists = "error", entitlements: List[str] = None):

        entitlements_data = list()
        entitlements = entitlements or list()

        for entitlement in entitlements:
            entitlements_data.append({"value": entitlement})

        if allow_cluster_create:
            entitlements_data.append({"value": "allow-cluster-create"})
            entitlements_data.append({"value": "allow-instance-pool-create"})

        data = {
            "schemas": [
                "urn:ietf:params:scim:schemas:core:2.0:User"
            ],
            "userName": username,
            "entitlements": entitlements_data
        }
        expected = 409 if if_exists in ["ignore", "overwrite"] else None
        return self.databricks.api("POST", f"{self.path}/scim/v2/Users", _data=data, _expected=expected)

    # TODO Rename parameter "id" to "user_id"
    # noinspection PyShadowingBuiltins
    def delete_by_id(self, id):
        return self.databricks.api("DELETE", f"{self.path}/scim/v2/Users/{id}")

    def delete_by_username(self, *usernames):
        user_id_map = {u['userName']: u['id'] for u in self.list()["Resources"]}
        for u in usernames:
            if u in user_id_map:
                self.delete_by_id(user_id_map[u])
