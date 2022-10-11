from dbacademy.rest.common import ApiContainer


class Groups(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks

    def list(self):
        return self.databricks.api("GET", "2.0/preview/scim/v2/Groups")["Resources"]

    def list_by_name(self):
        return {g["displayName"]: g for g in self.list()}

    def get(self, group_id=None, group_name=None):
        groups = self.list_by_name()
        if group_name:
            return groups.get(group_name)
        elif id:
            next((g for g in groups if g["id"] == group_id), None)
        else:
            raise Exception("Must specify group_id or group_name")

    def add_entitlement(self, entitlement, group=None, group_id=None, group_name=None):
        if group_id:
            pass
        elif group:
            group_id = group["id"]
        elif group_name:
            group_id = self.get(group_name=group_name)["id"]
        else:
            raise Exception("Must provide group, group_id, or group_name")
        return self.databricks.api("PATCH", f"2.0/preview/scim/v2/Groups/{group_id}", data={
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "add", "path": "entitlements", "value": [{"value": entitlement}]}]
        })

    def remove_entitlement(self, entitlement, group=None, group_id=None, group_name=None):
        if group_id:
            pass
        elif group:
            group_id = group["id"]
        elif group_name:
            group_id = self.get(group_name=group_name)["id"]
        else:
            raise Exception("Must provide group, group_id, or group_name")
        return self.databricks.api("PATCH", f"2.0/preview/scim/v2/Groups/{group_id}", data={
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "remove", "path": f'entitlements[value eq "{entitlement}"]'}]
        })

    def allow_cluster_create(self, value=True, group=None, group_id=None, group_name=None):
        if value:
            self.add_entitlement("allow-cluster-create", group=group, group_id=group_id, group_name=group_name)
        else:
            self.remove_entitlement("allow-cluster-create", group=group, group_id=group_id,
                                    group_name=group_name)
