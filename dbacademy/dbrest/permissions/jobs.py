from dbacademy.dbrest import DBAcademyRestClient

class JobsPermissionsClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client      # Client API exposing other operations to this class
        self.token = token        # The authentication token
        self.endpoint = endpoint  # The API endpoint

    def get_levels(self, id):
        return self.client.execute_get_json(f"{self.endpoint}/api/2.0/permissions/jobs/{id}/permissionLevels")

    def get(self, id):
        return self.client.execute_get_json(f"{self.endpoint}/api/2.0/permissions/jobs/{id}")

    # def change_owner(self, job_id, username):
    #     from dbacademy import dbgems
    #     params = {
    #         "access_control_list": [
    #             {
    #                 "user_name": dbgems.get_username(),
    #                 "permission_level": "CAN_MANAGE"
    #             },
    #             {
    #                 "user_name": username,
    #                 "permission_level": "IS_OWNER"
    #             }
    #         ]
    #     }
    #     return self.client.execute_patch_json(f"{self.endpoint}/api/2.0/permissions/jobs/{job_id}", params)
