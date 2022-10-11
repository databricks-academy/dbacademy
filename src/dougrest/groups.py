from dbacademy.rest.common import DatabricksApiException, ApiContainer


class Groups(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks

    def add_member(self, parent_name, *, user_name=None, group_name=None):
        """Add a user or group to the parent group."""
        data = {"parent_name": parent_name}
        if user_name:
            data["user_name"] = user_name
        elif group_name:
            data["group_name"] = group_name
        else:
            raise DatabricksApiException("Must provide user_name or group_name.")
        return self.databricks.api("POST", "2.0/groups/add-member", data=data)

    def create(self, group_name, *, if_exists="error"):
        data = {"group_name": group_name}
        if if_exists == "ignore":
            try:
                return self.databricks.api("POST", "2.0/groups/create", data=data)
            except DatabricksApiException as e:
                if e.http_code == 400 and e.error_code == 'RESOURCE_ALREADY_EXISTS':
                    return None
                elif e.http_code == 500 and "already exists" in e.message:
                    return None
                else:
                    raise e
        elif if_exists == "overwrite":
            try:
                return self.databricks.api("POST", "2.0/groups/create", data=data)
            except DatabricksApiException as e:
                if e.http_code == 400 and e.error_code == 'RESOURCE_ALREADY_EXISTS':
                    self.delete(group_name)
                    return self.databricks.api("POST", "2.0/groups/create", data=data)
                elif e.http_code == 500 and "already exists" in e.message:
                    self.delete(group_name)
                    return self.databricks.api("POST", "2.0/groups/create", data=data)
                else:
                    raise e
        else:
            return self.databricks.api("POST", "2.0/groups/create", data=data)

    def list(self):
        """List all groups."""
        return self.databricks.api("GET", "2.0/groups/list")["group_names"]

    def list_members(self, group_name=None):
        return self.databricks.api("GET", "2.0/groups/list-members", data={"group_name": group_name})["members"]

    def list_parents(self, *, user_name=None, group_name=None):
        if user_name:
            data = {"user_name": user_name}
        elif group_name:
            data = {"group_name": group_name}
        else:
            raise DatabricksApiException("Must provide user_name or group_name.")
        return self.databricks.api("GET", "2.0/groups/list-parents", data=data)["group_names"]

    def remove_member(self, parent_name, *, user_name=None, group_name=None):
        """Add a user or group to the parent group."""
        data = {"parent_name": parent_name}
        if user_name:
            data["user_name"] = user_name
        elif group_name:
            data["group_name"] = group_name
        else:
            raise DatabricksApiException("Must provide user_name or group_name.")
        return self.databricks.api("POST", "2.0/groups/remove-member", data=data)

    def delete(self, group_name, *, if_not_exists="error"):
        if if_not_exists == "ignore":
            try:
                return self.databricks.api("POST", "2.0/groups/delete", data={"group_name": group_name})
            except DatabricksApiException as e:
                if e.http_code == 404 and e.error_code == 'RESOURCE_DOES_NOT_EXIST':
                    return None
                else:
                    raise e
        else:
            return self.databricks.api("POST", "2.0/groups/delete", data={"group_name": group_name})
