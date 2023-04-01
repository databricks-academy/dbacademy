from typing import List, Optional, Callable, Iterable, Dict, Any

from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import DatabricksApiException


class WorkspaceTrio:
    from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig
    from dbacademy.dougrest.accounts.workspaces import Workspace as WorkspaceAPI
    from dbacademy.classrooms.classroom import Classroom

    def __init__(self, workspace_config: WorkspaceConfig, workspace_api: WorkspaceAPI, classroom: Optional[Classroom]):
        from dbacademy.dbrest import DBAcademyRestClient

        self.__workspace_config = workspace_config
        self.__workspace_api = workspace_api
        self.__classroom = classroom

        self.__name = workspace_config.name
        self.__number = workspace_config.workspace_number
        self.__existing_users: Optional[Dict[str, Any]] = None

        if self.workspace_api.url.endswith("/api/"):
            endpoint = self.workspace_api.url[:-5]
        elif self.workspace_api.url.endswith("/"):
            endpoint = self.workspace_api.url[:-1]
        else:
            endpoint = self.workspace_api.url

        self.__client = DBAcademyRestClient(authorization_header=self.workspace_api.authorization_header, endpoint=endpoint)

    def __str__(self) -> str:
        return f"{self.name} #{self.number}"

    @property
    def client(self) -> DBAcademyRestClient:
        return self.__client

    def get_by_username(self, username) -> Optional[Dict[str, Any]]:
        for user in self.existing_users:
            if username == user.get("userName"):
                return user
        return None

    @property
    def existing_users(self) -> Optional[Dict[str, Any]]:
        if self.__existing_users is None:
            self.__existing_users = self.client.scim.users.list()

        return self.__existing_users

    @existing_users.setter
    def existing_users(self, existing_users: Optional[Dict[str, Any]]) -> None:
        self.__existing_users = existing_users

    @property
    def name(self) -> str:
        return self.__name

    @property
    def number(self) -> int:
        return self.__number

    @property
    def workspace_config(self) -> WorkspaceConfig:
        return self.__workspace_config

    @property
    def workspace_api(self) -> WorkspaceAPI:
        return self.__workspace_api

    @property
    def classroom(self) -> Classroom:
        return self.__classroom


class WorkspaceSetup:
    from dbacademy.workspaces_3_0.account_config_class import AccountConfig

    def __init__(self, account_config: AccountConfig):
        from dbacademy.dougrest import AccountsApi
        from dbacademy.workspaces_3_0.account_config_class import AccountConfig

        assert account_config is not None, f"""The parameter "account_config" must be specified."""
        assert type(account_config) == AccountConfig, f"""The parameter "account_config" must be of type AccountConfig, found {type(account_config)}."""
        self.__account_config = account_config

        self.__accounts_api = AccountsApi(account_id=self.account_config.account_id,
                                          user=self.account_config.username,
                                          password=self.account_config.password)

    @property
    def accounts_api(self):
        return self.__accounts_api

    @property
    def account_config(self) -> AccountConfig:
        return self.__account_config

    @classmethod
    def __for_each_workspace(cls, workspaces: List[WorkspaceTrio], some_action: Callable[[WorkspaceTrio], None]) -> None:
        from multiprocessing.pool import ThreadPool

        print("-"*100)

        with ThreadPool(len(workspaces)) as pool:
            pool.map(some_action, workspaces)

    @staticmethod
    def __remove_metastore(trio: WorkspaceTrio):
        # Unassign the metastore from the workspace
        print(f"""Deleting the metastore for workspace "{trio.workspace_config.name}".""")

        try:
            response = trio.workspace_api.api("GET", f"2.1/unity-catalog/current-metastore-assignment")
        except DatabricksApiException as e:
            if e.error_code == "METASTORE_DOES_NOT_EXIST":
                return  # It doesn't exist, move on.
            else:
                raise e

        metastore_id = response["metastore_id"]
        workspace_id = response["workspace_id"]

        try:
            # Delete the assignment
            trio.workspace_api.api("DELETE", f"2.1/unity-catalog/workspaces/{workspace_id}/metastore", {
                "metastore_id": metastore_id
            })
        except DatabricksApiException as e:
            if e.error_code == "METASTORE_DOES_NOT_EXIST":
                return  # It doesn't exist, move on.
            else:
                raise e

        try:
            # Delete the metastore, but only if there are no other workspaces attached.
            trio.workspace_api.api("DELETE", f"2.1/unity-catalog/metastores/{metastore_id}", {
                "force": True
            })
        except DatabricksApiException as e:
            if e.error_code == "METASTORE_DOES_NOT_EXIST":
                return  # It doesn't exist, move on.
            else:
                raise e

    def remove_metastore(self, workspace_number: int):
        assert workspace_number >= 0, f"Invalid workspace number: {workspace_number}"

        print("\n")
        print("-"*100)

        workspace_config = self.account_config.create_workspace_config(template=self.account_config.workspace_config_template,
                                                                       workspace_number=workspace_number)

        workspace_api = self.accounts_api.workspaces.get_by_name(workspace_config.name, if_not_exists="error")

        trio = WorkspaceTrio(workspace_config, workspace_api, None)

        print(f"Removing the metastore for workspace #{workspace_number}: {trio.name}""")
        self.__remove_metastore(trio)

    def remove_metastores(self):
        print("\n")
        print("-"*100)

        workspaces = self.__utils_create_workspace_trios()

        print(f"Removing {len(workspaces)} metastores.""")
        for trio in workspaces:
            self.__remove_metastore(trio)

    def __utils_create_workspace_trios(self, workspace_number: int = 0) -> List[WorkspaceTrio]:
        workspaces: List[WorkspaceTrio] = list()

        for workspace_config in self.account_config.workspaces:
            if workspace_number == 0 or workspace_number == workspace_config.workspace_number:
                workspace_api = self.accounts_api.workspaces.get_by_name(workspace_config.name, if_not_exists="ignore")

                if workspace_api is not None:
                    trio = WorkspaceTrio(workspace_config, workspace_api, None)
                    workspaces.append(trio)

        return workspaces

    def remove_workspace_setup_jobs(self, *, remove_bootstrap_job: bool, remove_final_job: bool):
        print("\n")
        print("-"*100)

        workspaces = self.__utils_create_workspace_trios()
        print(f"Removing Universal-Workspace-Setup jobs (bootstrap={remove_bootstrap_job}, final={remove_final_job}) for {len(workspaces)} workspaces.""")

        for trio in workspaces:
            self.__utils_remove_workspace_setup_jobs(trio, remove_bootstrap_job=remove_bootstrap_job, remove_final_job=remove_final_job)

    def __delete_workspace(self, trio: WorkspaceTrio):
        try:
            print(f"""Deleting the workspace "{trio.workspace_config.name}".""")
            self.accounts_api.workspaces.delete_by_name(trio.workspace_config.name)

        except DatabricksApiException as e:
            if e.http_code == 404:
                print(f"""Cannot delete workspace "{trio.workspace_config.name}"; it doesn't exist.""")
            else:
                print("-"*80)
                print(f"""Failed to delete the workspace "{trio.workspace_config.name}".\n{e}""")
                print("-"*80)

    def delete_workspace(self, workspace_number: int):
        assert workspace_number >= 0, f"Invalid workspace number: {workspace_number}"

        print("\n")
        print("-"*100)

        workspace_config = self.account_config.create_workspace_config(template=self.account_config.workspace_config_template,
                                                                       workspace_number=workspace_number)

        workspace_api = self.accounts_api.workspaces.get_by_name(workspace_config.name, if_not_exists="error")

        trio = WorkspaceTrio(workspace_config, workspace_api, None)

        print(f"Deleting workspace #{workspace_number}: {trio.workspace_config.name}""")

        self.__remove_metastore(trio)
        self.__delete_workspace(trio)

    def delete_workspaces(self):
        print("\n")
        print("-"*100)

        workspaces = self.__utils_create_workspace_trios()
        print(f"Deleting {len(workspaces)} workspaces.""")

        for trio in workspaces:
            self.__remove_metastore(trio)
            self.__delete_workspace(trio)

    def create_workspaces(self, *, remove_users: bool, create_users: bool, update_entitlements: bool, create_groups: bool, remove_metastore: bool, create_metastore: bool, run_workspace_setup: bool, enable_features: bool, install_courseware: bool, uninstall_courseware: bool = False):
        from dbacademy.classrooms.classroom import Classroom

        print("\n")
        print("-"*100)
        print(f"""Creating {len(self.account_config.workspaces)} workspaces.""")

        workspaces: List[WorkspaceTrio] = list()

        for workspace_config in self.account_config.workspaces:
            print(workspace_config.name)
            workspace_api = self.accounts_api.workspaces.get_by_name(workspace_config.name, if_not_exists="ignore")
            if workspace_api is None:
                # It doesn't exist, so go ahead and create it.
                workspace_api = self.accounts_api.workspaces.create(workspace_name=workspace_config.name,
                                                                    deployment_name=workspace_config.name,
                                                                    region=self.account_config.region,
                                                                    credentials_name=self.account_config.workspace_config_template.credentials_name,
                                                                    storage_configuration_name=self.account_config.workspace_config_template.storage_configuration)

            classroom = Classroom(num_students=len(workspace_config.usernames),
                                  username_pattern=workspace_config.username_pattern,
                                  databricks_api=workspace_api)

            workspaces.append(WorkspaceTrio(workspace_config, workspace_api, classroom))

        #############################################################
        # Workspaces were all created synchronously, blocking here until we confirm that all workspaces are created.
        print("-"*100)
        for workspace in workspaces:
            print(f"""Waiting for workspace "{workspace.workspace_config.name}" to finish provisioning...""")
            workspace.workspace_api.wait_until_ready()

        #############################################################
        # Remove any users before doing anything like adding them.
        if remove_users:
            self.__for_each_workspace(workspaces, self.__remove_users)
        else:
            print("Skipping removal of users")

        #############################################################
        # Processed before adding users to minimize overhead for new workspaces
        # Do to a piece of logic that is loading all users for backwards compatability.
        if update_entitlements:
            self.__for_each_workspace(workspaces, self.__update_entitlements)
        else:
            print("Skipping update of entitlements")

        #############################################################
        if create_users:
            self.__for_each_workspace(workspaces, self.__create_users)
        else:
            print("Skipping creation of users")

        #############################################################
        if create_groups:
            self.__for_each_workspace(workspaces, self.__create_group)
        else:
            print("Skipping creation of groups")

        #############################################################
        if remove_metastore:
            self.__for_each_workspace(workspaces, self.__remove_metastore)
        else:
            print("Skipping removal of metastore")

        #############################################################
        if create_metastore:
            self.__for_each_workspace(workspaces, self.__create_metastore)
        else:
            print("Skipping creation of metastore")

        #############################################################
        if enable_features:
            self.__for_each_workspace(workspaces, self.__enable_features)
        else:
            print("Skipping enablement of workspace features")

        #############################################################
        if uninstall_courseware:
            self.__for_each_workspace(workspaces, self.__uninstall_courseware)
        else:
            print("Skipping uninstall of courseware")

        #############################################################
        if install_courseware:
            self.__for_each_workspace(workspaces, self.__install_courseware)
        else:
            print("Skipping install of courseware")

        #############################################################
        if run_workspace_setup:
            self.__for_each_workspace(workspaces, self.__create_workspaces_run_workspace_setup)
        else:
            print("Skipping run of Universal-Workspace-Setup")

        print(f"""Completed setup for {len(workspaces)} workspaces.""")

    @classmethod
    def __install_courseware(cls, trio: WorkspaceTrio):
        import os
        from dbacademy.dbhelper import WorkspaceHelper

        for course_def in trio.workspace_config.course_definitions:

            for username in trio.workspace_config.usernames:
                print(f"Installing the course {course_def} for {username}")

                url, course, version, artifact, token = WorkspaceHelper.parse_course_args(course_def)
                download_url = WorkspaceHelper.compose_courseware_url(url, course, version, artifact, trio.workspace_config.cds_api_token)

                if trio.workspace_config.courseware_subdirectory is None:
                    install_dir = f"/Users/{username}/{course}"
                else:
                    install_dir = f"/Users/{username}/{trio.workspace_config.courseware_subdirectory}/{course}"

                print(f" - {install_dir}")

                files = trio.client.workspace.ls(install_dir)
                count = 0 if files is None else len(files)
                if count > 0:
                    print(f" - Skipping, course already exists.")
                else:
                    files = [f.lower() for f in os.listdir("/")]
                    if "tmp" in files:
                        local_file_path = "/tmp/download.dbc"
                    elif "temp" in files:
                        local_file_path = "/temp/download.dbc"
                    else:
                        local_file_path = "/download.dbc"

                    trio.client.workspace.import_dbc_files(install_dir, download_url, local_file_path=local_file_path)
                    print(f" - Installed.")

            print("-" * 80)

    @classmethod
    def __uninstall_courseware(cls, trio: WorkspaceTrio):
        from dbacademy.dbhelper import WorkspaceHelper

        usernames = [u.get("userName") for u in trio.client.scim.users.list()]

        for username in usernames:
            print(f"Uninstalling courses for {username}")

            for course_def in trio.workspace_config.course_definitions:
                url, course, version, artifact, token = WorkspaceHelper.parse_course_args(course_def)

                if trio.workspace_config.courseware_subdirectory is None:
                    install_dir = f"/Users/{username}/{course}"
                else:
                    install_dir = f"/Users/{username}/{trio.workspace_config.courseware_subdirectory}/{course}"

                print(install_dir)
                trio.client.workspace.delete_path(install_dir)

            print("-" * 80)

    @classmethod
    def __enable_features(cls, trio: WorkspaceTrio):
        # Enabling serverless endpoints.
        settings = trio.workspace_api.api("GET", "2.0/sql/config/endpoints")
        feature_name = "enable_serverless_compute"
        feature_status = True
        settings[feature_name] = feature_status
        trio.workspace_api.api("PUT", "2.0/sql/config/endpoints", settings)

        new_feature_status = trio.workspace_api.api("GET", "2.0/sql/config/endpoints").get(feature_name)
        assert new_feature_status == feature_status, f"""Expected "{feature_name}" to be "{feature_status}", found "{new_feature_status}"."""

        trio.workspace_api.api("PATCH", "2.0/workspace-conf", {
               "enable-X-Frame-Options": "false",  # Turn off iframe prevention
               "intercomAdminConsent": "false",    # Turn off product welcome
               "enableDbfsFileBrowser": "true",    # Enable DBFS UI
               "enableWebTerminal": "true",        # Enable Web Terminal
               "enableExportNotebook": "true"      # We will disable this in due time
        })

    def __remove_users(self, trio: WorkspaceTrio):
        # Just in case it's not ready
        trio.workspace_api.wait_until_ready()

        name = trio.workspace_config.name
        print(f"""Removing {len(trio.existing_users)} users for "{name}".""")

        for user in trio.existing_users:
            user_id = user.get("id")
            username = user.get("userName")
            if username != self.account_config.username:
                trio.classroom.databricks.users.delete_by_id(user_id)

        trio.existing_users = None

    def __create_users(self, trio: WorkspaceTrio):
        # Just in case it's not ready
        trio.workspace_api.wait_until_ready()

        name = trio.workspace_config.name
        max_users = len(trio.workspace_config.usernames)
        print(f"""Configuring {max_users} users for "{name}", listing existing users...""", end="")

        existing_usernames = [u.get("userName") for u in trio.existing_users]
        print("done.")

        if len(existing_usernames) > 1:
            # More than one because user actual count is max_participants+1
            existing_count = len(existing_usernames)
            remaining_count = max_users-len(existing_usernames)+1
            print(f"""Found {existing_count} users, creating {remaining_count} users for "{name}".""")

        for username in trio.workspace_config.usernames:
            if username in existing_usernames:
                user = trio.get_by_username(username)
            else:
                try:
                    user = trio.client.scim.users.create(username)
                except:
                    raise Exception(f"Failed to create user {username} for {name}")

            if username != self.account_config.username:
                self.__remove_entitlement(trio, user, "allow-cluster-create")
                self.__remove_entitlement(trio, user, "databricks-sql-access")
                self.__remove_entitlement(trio, user, "workspace-access")

    @staticmethod
    def __remove_entitlement(trio: WorkspaceTrio, user: Dict[str, Any], entitlement: str):
        username = user.get("userName")
        entitlements = [e.get("value") for e in user.get("entitlements", list())]
        if entitlement in entitlements:
            try:
                trio.client.scim.users.remove_entitlement(user.get("id"), entitlement)
            except Exception as e:
                raise Exception(f"Exception removing entitlement {entitlement} from {username}") from e

    @staticmethod
    def __update_entitlements(trio: WorkspaceTrio):
        # Just in case it's not ready
        trio.workspace_api.wait_until_ready()

        name = trio.workspace_config.name
        print(f"""Updating all-users entitlements for "{name}".""")

        group = trio.client.scim.groups.get_by_name("users")

        for name, value in trio.workspace_config.entitlements.items():
            if value is True:
                trio.client.scim.groups.add_entitlement(group.get("id"), name)
            else:
                trio.client.scim.groups.remove_entitlement(group.get("id"), name)

    @classmethod
    def __create_group(cls, trio: WorkspaceTrio):
        name = trio.workspace_config.name

        print(f"""Creating {len(trio.workspace_config.workspace_group)} groups for "{name}".""")
        existing_groups = trio.client.scim.groups.list()
        existing_groups_name = [g.get("displayName") for g in existing_groups]

        if len(existing_groups) > 0:
            print(f"""Found {len(existing_groups)} groups for "{name}".""")

        for group_name, usernames in trio.workspace_config.workspace_group.items():
            if group_name not in existing_groups_name:
                trio.client.scim.groups.create(group_name)

            if group_name != "users":
                # We still have to add the specified user to this group (User #0 to "admins" in our case)
                group = trio.client.scim.groups.get_by_name(group_name)
                for username in usernames:
                    # Add the user as a member of that group
                    user = trio.get_by_username(username)
                    trio.client.scim.groups.add_member(group.get("id"), user.get("id"))

    def __create_workspaces_run_workspace_setup(self, trio: WorkspaceTrio):
        try:
            print(f"""Starting Universal-Workspace-Setup for "{trio.workspace_config.name}" """)

            if self.__utils_run_existing_job(trio):
                return  # Secondary run, using Workspace Setup job
            else:
                # First run, create and run Workspace Setup (Bootstrap) job.
                self.__utils_create_workspace_setup_job(trio)

            self.__utils_remove_workspace_setup_jobs(trio, remove_bootstrap_job=True, remove_final_job=False)

            print(f"""Finished Universal-Workspace-Setup for "{trio.workspace_config.name}" """)

        except Exception as e:
            raise Exception(f"""Failed to create the workspace "{trio.workspace_config.name}".""") from e

    def __utils_remove_workspace_setup_jobs(self, trio, *, remove_bootstrap_job: bool, remove_final_job: bool):
        from dbacademy.dbhelper import WorkspaceHelper

        if remove_bootstrap_job:
            job = self.__utils_get_job_by_name(trio, WorkspaceHelper.BOOTSTRAP_JOB_NAME)
            if job is not None:
                # Now that we have ran the job, delete the Bootstrap job.
                trio.workspace_api.api("POST", "2.1/jobs/delete", job_id=job.get("job_id"))

        if remove_final_job:
            job = self.__utils_get_job_by_name(trio, WorkspaceHelper.WORKSPACE_SETUP_JOB_NAME)
            if job is not None:
                trio.workspace_api.api("POST", "2.1/jobs/delete", job_id=job.get("job_id"))

    @staticmethod
    def __utils_create_workspace_setup_job(trio: WorkspaceTrio):
        from dbacademy.classrooms.monitor import Commands

        state = Commands.universal_setup(trio.workspace_api,
                                         node_type_id=trio.workspace_config.default_node_type_id,
                                         spark_version=trio.workspace_config.default_dbr,
                                         datasets=trio.workspace_config.datasets,
                                         lab_id=f"Classroom #{trio.workspace_config.workspace_number}",
                                         description=f"Classroom #{trio.workspace_config.workspace_number}",
                                         courses=trio.workspace_config.dbc_urls)

        assert state == "SUCCESS", f"""Expected the final state of Universal-Workspace-Setup to be "SUCCESS", found "{state}" for "{trio.name}"."""

    @classmethod
    def __utils_get_job_by_name(cls, trio: WorkspaceTrio, name: str) -> Optional[Dict[str, Any]]:
        response = trio.workspace_api.api("GET", "2.1/jobs/list?limit=25&expand_tasks=false")
        jobs = response.get("jobs", dict())

        for job in jobs:
            if job.get("settings", dict()).get("name") == name:
                return job  # Found it.

        return None  # Not found

    def __utils_run_existing_job(self, trio: WorkspaceTrio) -> bool:

        job = self.__utils_get_job_by_name(trio, "DBAcademy Workspace-Setup")
        if job is not None:
            job_id = job.get("job_id")
            response = trio.workspace_api.api("POST", "2.1/jobs/run-now", job_id=job_id)
            response = self.__utils_wait_for_job_run(trio, response.get("run_id"))
            state = response.get("state").get("result_state")
            assert state == "SUCCESS", f"""Expected the final state of Universal-Workspace-Setup to be "SUCCESS", found "{state}" for "{trio.name}"."""
            return True

        return False  # Not found

    def __utils_wait_for_job_run(self, trio: WorkspaceTrio, run_id):
        import time

        wait = 15
        response = trio.workspace_api.api("GET", f"2.1/jobs/runs/get?run_id={run_id}")

        state = response["state"]["life_cycle_state"]

        if state != "TERMINATED" and state != "INTERNAL_ERROR" and state != "SKIPPED":
            if state == "PENDING" or state == "RUNNING":
                time.sleep(wait)
            else:
                time.sleep(5)

            return self.__utils_wait_for_job_run(trio, run_id)

        return response

    def __create_metastore(self, trio: WorkspaceTrio):

        # Just in case it's not ready
        trio.workspace_api.wait_until_ready()

        # No-op if a metastore is already assigned to the workspace
        metastore_id = self.__utils_get_or_create_metastore(trio)

        self.__utils_enable_delta_sharing(trio, metastore_id)

        self.__utils_assign_metastore_to_workspace(trio, metastore_id)

        self.__utils_update_uc_permissions(trio, metastore_id)

        self.__create_storage_credentials(trio, metastore_id)

    def __create_storage_credentials(self, trio: WorkspaceTrio, metastore_id: str):
        # Create a storage credential for the access_connector

        credentials = trio.workspace_api.api("GET", f"2.1/unity-catalog/storage-credentials/{trio.name}", _expected=[200, 404])

        if credentials is None:
            credentials = {
                "name": trio.workspace_config.name,
                "skip_validation": False,
                "read_only": False,
            }

            if self.account_config.uc_storage_config.aws_iam_role_arn is not None:
                credentials["aws_iam_role"] = {
                    "role_arn": self.account_config.uc_storage_config.aws_iam_role_arn
                }

            if self.account_config.uc_storage_config.msa_access_connector_id is not None:
                credentials["azure_managed_identity"] = {
                    "access_connector_id": self.account_config.uc_storage_config.msa_access_connector_id
                }

            credentials = trio.workspace_api.api("POST", "2.1/unity-catalog/storage-credentials", credentials)

        storage_root_credential_id = credentials.get("id")

        # Set storage root credential.
        trio.workspace_api.api("PATCH", f"2.1/unity-catalog/metastores/{metastore_id}", {
            "storage_root_credential_id": storage_root_credential_id
        })

    @classmethod
    def __utils_assign_metastore_to_workspace(cls, trio: WorkspaceTrio, metastore_id: str):
        # Assign the metastore to the workspace
        workspace_id = trio.workspace_api.get("workspace_id")
        trio.workspace_api.api("PUT", f"2.1/unity-catalog/workspaces/{workspace_id}/metastore", {
            "metastore_id": metastore_id,
            "default_catalog_name": "main"
        })

    @classmethod
    def __utils_update_uc_permissions(cls, trio: WorkspaceTrio, metastore_id: str):
        try:
            # Grant all users permission to create resources in the metastore
            trio.workspace_api.api("PATCH", f"2.1/unity-catalog/permissions/metastore/{metastore_id}", {
                "changes": [{
                    "principal": "account users",
                    "add": ["CREATE CATALOG", "CREATE EXTERNAL LOCATION", "CREATE SHARE", "CREATE RECIPIENT", "CREATE PROVIDER"]
                }]
            })
        except Exception as e:
            raise Exception(f"Unexpected exception patching metastore {metastore_id} for {trio.name}") from e

    def __utils_enable_delta_sharing(self, trio: WorkspaceTrio, metastore_id: str):
        try:
            trio.workspace_api.api("PATCH", f"2.1/unity-catalog/metastores/{metastore_id}", {
                "owner": self.account_config.uc_storage_config.meta_store_owner,
                "delta_sharing_scope": "INTERNAL_AND_EXTERNAL",
                "delta_sharing_recipient_token_lifetime_in_seconds": 90 * 24 * 60 * 60,
                "delta_sharing_organization_name": trio.workspace_config.name
            })
        except DatabricksApiException as e:
            if e.error_code == "PRINCIPAL_DOES_NOT_EXIST":
                raise Exception(f"""You must first create the account-level principle "{self.account_config.uc_storage_config.owner}".""")
            else:
                raise e

    @classmethod
    def __utils_find_metastore(cls, trio: WorkspaceTrio) -> Optional[str]:
        # According to the docs, there is no pagination for this call.
        response = trio.workspace_api.api("GET", "2.1/unity-catalog/metastores")
        metastores = response.get("metastores", list())
        for metastore in metastores:
            if metastore.get("name") == trio.workspace_config.name:
                return metastore.get("metastore_id")

        return None

    def __utils_get_or_create_metastore(self, trio: WorkspaceTrio) -> str:
        try:
            # Check to see if a metastore is assigned
            metastore = trio.workspace_api.api("GET", f"2.1/unity-catalog/current-metastore-assignment")
            print(f"""Using previously assigned metastore for "{trio.workspace_config.name}".""")
            return metastore.get("metastore_id")

        except DatabricksApiException as e:
            if e.error_code != "METASTORE_DOES_NOT_EXIST":
                raise Exception(f"""Unexpected exception looking up metastore for workspace "{trio.workspace_config.name}".""") from e

        # No metastore is assigned, check to see if it exists.
        metastore_id = self.__utils_find_metastore(trio)
        if metastore_id is not None:
            print(f"""Found existing metastore for "{trio.workspace_config.name}".""")
            return metastore_id  # Found it.

        # Create a new metastore
        print(f"""Creating metastore for "{trio.workspace_config.name}".""")
        metastore = trio.workspace_api.api("POST", "2.1/unity-catalog/metastores", {
            "name": trio.workspace_config.name,
            "storage_root": self.account_config.uc_storage_config.storage_root,
            "region": self.account_config.uc_storage_config.region
        })

        return metastore.get("metastore_id")
