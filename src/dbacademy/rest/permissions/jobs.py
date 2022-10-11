from dbacademy.rest.common import ApiClient
from dbacademy.rest.permissions.crud import PermissionsCrud

__all__ = ["Jobs"]

# noinspection PyProtectedMember
from dbacademy.rest.permissions.crud import What, valid_whats, PermissionLevel


class Jobs(PermissionsCrud):

    valid_permissions = ["IS_OWNER", "CAN_MANAGE_RUN", "CAN_VIEW", "CAN_MANAGE"]

    def __init__(self, client: ApiClient):
        super().__init__(client, "2.0/preview/permissions/jobs", "job")

    def change_owner_user(self, job_id, new_owner_id: str):
        return self.change_owner(job_id, "user_name", new_owner_id)

    def change_owner_group(self, job_id, new_owner_id: str):
        return self.change_owner(job_id, "group_name", new_owner_id)

    def change_owner_service_principal(self, job_id, new_owner_id: str):
        return self.change_owner(job_id, "service_principal_name", new_owner_id)

    def change_owner(self, job_id, owner_type: What, owner_id: str):

        if owner_type == "user": owner_type = "user_name"
        if owner_type == "group": owner_type = "group_name"
        if owner_type == "service_principal": owner_type = "service_principal_name"
        assert owner_type in ["user_name", "group_name", "service_principal_name"], f"Expected owner_type to be one of \"user_name\", \"group_name\", or \"service_principal_name\", found \"{owner_type}\"."

        old_what, old_id = self.get_owner(job_id)

        params = {
            "access_control_list": [
                {
                    owner_type: owner_id,
                    "permission_level": "IS_OWNER"
                },
                {
                    old_what: old_id,
                    "permission_level": "CAN_MANAGE"
                }
            ]
        }
        return self.client.api("PATCH", f"{self.path}/{job_id}", data=params)

    def get_owner(self, job_id):
        results = self.get(job_id)
        for access_control in results.get("access_control_list"):
            for permission in access_control.get("all_permissions"):
                if permission.get("permission_level") == "IS_OWNER":
                    if "user_name" in access_control:
                        return "user_name", access_control.get("user_name")
                    elif "group_name" in access_control:
                        return "group_name", access_control.get("group_name")
                    elif "service_principal_name" in access_control:
                        return "service_principal_name", access_control.get("service_principal_name")
                    else:
                        raise ValueError(f"Could not find user, group or service principal name for job {job_id}")
