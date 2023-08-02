from typing import List, Optional, Callable, Dict, Any

from dbacademy.clients.airtable import AirTableClient
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import DatabricksApiException
from dbacademy_jobs.workspaces_3_0.support.workspace_config_classe import WorkspaceConfig


class WorkspaceTrio:
    from dbacademy_jobs.workspaces_3_0.support.workspace_config_classe import WorkspaceConfig
    from dbacademy.dougrest.accounts.workspaces import Workspace as WorkspaceAPI
    from dbacademy.clients.classrooms.classroom import Classroom

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
        self.__client.dns_retry = True

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
    from dbacademy.dougrest.accounts.workspaces import Workspace
    from dbacademy_jobs.workspaces_3_0.support.account_config_class import AccountConfig

    def __init__(self, account_config: AccountConfig, run_workspace_setup: bool):
        import os
        from dbacademy import common
        from dbacademy.dougrest import AccountsApi
        from dbacademy_jobs.workspaces_3_0.support.account_config_class import AccountConfig

        self.__errors: List[str] = list()
        self.__workspaces: List[WorkspaceTrio] = list()
        self.__account_config = common.verify_type(AccountConfig, non_none=True, account_config=account_config)

        self.__accounts_api = AccountsApi(account_id=self.account_config.account_id,
                                          user=self.account_config.username,
                                          password=self.account_config.password)
        # self.__accounts_api.dns_retry = True
        self.__run_workspace_setup = run_workspace_setup

        self.__air_table_records: List[Dict[str, Any]] = list()
        air_table_token = os.environ.get("AIR-TABLE-PERSONAL-ACCESS-TOKEN")
        assert air_table_token is not None, f"""The environment variable "AIR-TABLE-PERSONAL-ACCESS-TOKEN" must be specified, not found."""
        base_id = "appNCMjJ2yMKUrTbo"
        table_id = "tblF3cxlP8gcM9Rqr"
        self.air_table_client = AirTableClient(access_token=air_table_token, base_id=base_id, table_id=table_id)

    @property
    def run_workspace_setup(self) -> bool:
        return self.__run_workspace_setup

    def log_error(self, msg):
        self.__errors.append(msg)
        return msg

    @property
    def errors(self):
        return self.__errors

    @property
    def accounts_api(self):
        return self.__accounts_api

    @property
    def account_config(self) -> AccountConfig:
        return self.__account_config

    @classmethod
    def __for_each_workspace(cls, workspaces: List[WorkspaceTrio], some_action: Callable[[WorkspaceTrio], None]) -> None:
        from multiprocessing.pool import ThreadPool
        with ThreadPool(len(workspaces)) as pool:
            pool.map(some_action, workspaces)

    @classmethod
    def __get_metastore(cls, workspace_api: Workspace, workspace_name: str) -> Optional[Dict[str, Any]]:

        for metastore in workspace_api.api("GET", f"2.1/unity-catalog/metastores").get("metastores", list()):
            if workspace_name == metastore.get("name"):
                return metastore

        return None

    @classmethod
    def __remove_metastore(cls, trio: WorkspaceTrio):
        workspace_api = trio.workspace_api

        workspace_id = workspace_api.get("workspace_id")
        workspace_name = workspace_api.get("workspace_name")
        workspace_number = workspace_name.split("-")[1]

        try:
            # Removed the currently assigned metastore
            assigned_metastore = trio.workspace_api.api("GET", f"2.1/unity-catalog/current-metastore-assignment")
            metastore_id = assigned_metastore.get("metastore_id")
            trio.workspace_api.api("DELETE", f"2.1/unity-catalog/workspaces/{workspace_id}/metastore", {
                "metastore_id": metastore_id
            })
        except DatabricksApiException as e:
            if e.error_code == "METASTORE_DOES_NOT_EXIST":
                print(f"""No metastore is assigned to workspace #{workspace_number} ({workspace_name}).""")
            else:
                raise e

        metastore = cls.__get_metastore(workspace_api, workspace_name)
        if metastore is None:
            print(f"""The metastore "{workspace_name}" does not exist.""")
        else:
            metastore_id = metastore.get("metastore_id")
            trio.workspace_api.api("DELETE", f"2.1/unity-catalog/metastores/{metastore_id}", {
                "force": True
            })

    def remove_metastore(self, workspace_number: int):
        assert workspace_number >= 0, f"Invalid workspace number: {workspace_number}"

        print("\n")
        print("-"*100)

        workspace_config = self.account_config.create_workspace_config(template=self.account_config.workspace_config_template,
                                                                       workspace_number=workspace_number)

        workspace_api = self.accounts_api.workspaces.get_by_name(workspace_config.name, if_not_exists="error")
        workspace_api.dns_retry = True

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
                    # workspace_api.dns_retry = True
                    trio = WorkspaceTrio(workspace_config, workspace_api, None)
                    workspaces.append(trio)

        return workspaces

    def remove_workspace_setup_jobs(self):
        from dbacademy.dbhelper import WorkspaceHelper

        print("\n")
        print("-"*100)

        workspaces = self.__utils_create_workspace_trios()
        print(f"Removing Universal-Workspace-Setup jobs for {len(workspaces)} workspaces.""")

        for trio in workspaces:
            job = trio.client.jobs.get_by_name(WorkspaceHelper.WORKSPACE_SETUP_JOB_NAME)
            if job is not None:
                # Now that we have ran the job, delete the Bootstrap job.
                trio.client.jobs.delete_by_job_id(job.get("job_id"))

    def __delete_workspace(self, trio: WorkspaceTrio):
        import traceback

        try:
            print(f"""Deleting the workspace "{trio.workspace_config.name}".""")
            self.accounts_api.workspaces.delete_by_name(trio.workspace_config.name)

        except DatabricksApiException as e:
            if e.http_code == 404:
                print(f"""Cannot delete workspace "{trio.workspace_config.name}"; it doesn't exist.""")
            else:
                msg = "="*100
                msg += f"""Failed to delete the workspace "{trio.workspace_config.name}".\n"""
                msg += traceback.format_exc()
                msg += "="*100
                print(msg)

    def delete_workspace(self, workspace_number: int):
        assert workspace_number >= 0, f"Invalid workspace number: {workspace_number}"

        workspace_config = self.account_config.create_workspace_config(template=self.account_config.workspace_config_template,
                                                                       workspace_number=workspace_number)
        try:
            workspace_api = self.accounts_api.workspaces.get_by_name(workspace_config.name, if_not_exists="error")
            workspace_api.dns_retry = True

            trio = WorkspaceTrio(workspace_config, workspace_api, None)
            print(f"Destroying workspace #{workspace_number}: {trio.workspace_config.name}""")

            self.__remove_metastore(trio)
            self.__delete_workspace(trio)

        except DatabricksApiException as e:
            if e.http_code == 404:
                print(f"Skipping deletion, workspace #{workspace_number} was not found for {workspace_config.name}""")
            else:
                raise e

    def delete_workspaces(self):
        print("\n")
        print("-"*100)

        workspaces = self.__utils_create_workspace_trios()
        print(f"Deleting {len(workspaces)} workspaces.""")

        for trio in workspaces:
            print("-"*100)
            self.__remove_metastore(trio)
            self.__delete_workspace(trio)

    def create_workspaces(self, *, remove_users: bool, remove_metastore: bool, uninstall_courseware: bool = False):

        self.__air_table_records = self.air_table_client.read()

        naming_pattern = self.account_config.workspace_config_template.workspace_name_pattern
        not_using_classroom = not naming_pattern.startswith("classroom-")

        if not self.run_workspace_setup or remove_metastore or remove_users or uninstall_courseware or not_using_classroom:
            print()
            print("-"*100)
            if not self.run_workspace_setup:
                print("WARNING: Not configured to run the Workspace-Setup job.")
            if remove_metastore:
                print("WARNING: Not configured to run the Workspace-Setup job.")
            if remove_users:
                print("WARNING: Not configured to run the Workspace-Setup job.")
            if uninstall_courseware:
                print("WARNING: Not configured to run the Workspace-Setup job.")
            if not_using_classroom:
                print(f"""WARNING: The workspace naming pattern doesn't start with "classroom", found "{naming_pattern}".""")

            if input(f"\nPlease confirm running with this abnormal configuration (y/n):").lower() not in ["1", "y", "yes"]:
                return print("Execution aborted.")

        print("\n")
        print("-"*100)
        print(f"""Configuring {len(self.account_config.workspaces)} workspaces.""")

        for workspace_config in self.account_config.workspaces:
            print(workspace_config.name)

        #############################################################
        from multiprocessing.pool import ThreadPool
        print("-"*100)
        print("Initializing REST APIs.")

        # Should be empty, reset anyway
        self.__workspaces: List[WorkspaceTrio] = list()

        with ThreadPool(len(self.account_config.workspaces)) as pool:
            pool.map(self.__create_workspace, self.account_config.workspaces)

        #############################################################
        # Workspaces were all created synchronously, blocking here until we confirm that all workspaces are created.
        for workspace in self.__workspaces:
            print(f"""Waiting for the creation of workspace "{workspace.workspace_config.name}" to finish provisioning...""")
            workspace.workspace_api.wait_until_ready()

        #############################################################
        # Remove any users before doing anything like adding them.
        print("-"*100)
        if remove_users:
            print("Removing all users from each workspaces.")
            self.__for_each_workspace(self.__workspaces, self.__remove_users)
        else:
            print("Skipping removal of users")

        #############################################################
        print("-"*100)
        print(f"""Configuring the entitlements for the group "users" in each workspace.""")
        self.__for_each_workspace(self.__workspaces, self.__update_entitlements)

        #############################################################
        print("-"*100)
        print(f"""Configuring users in each workspace.""")
        self.__for_each_workspace(self.__workspaces, self.__create_users)

        #############################################################
        print("-"*100)
        print(f"""Configuring groups in each workspace.""")
        self.__for_each_workspace(self.__workspaces, self.__create_group)

        #############################################################
        print("-"*100)
        if remove_metastore:
            print(f"""Deleting the metastore in each workspace.""")
            self.__for_each_workspace(self.__workspaces, self.__remove_metastore)
        else:
            print("Skipping removal of metastore")

        #############################################################
        print("-"*100)
        print(f"""Configuring the metastore in each workspace.""")
        self.__for_each_workspace(self.__workspaces, self.__create_metastore)

        #############################################################
        print("-"*100)
        print(f"""Configuring features in each workspace.""")
        self.__for_each_workspace(self.__workspaces, self.__enable_features)

        #############################################################
        print("-"*100)
        if uninstall_courseware:
            print(f"""Uninstalling courseware for all users in each workspace.""")
            self.__for_each_workspace(self.__workspaces, self.__uninstall_courseware)
        else:
            print("Skipping uninstall of courseware")

        #############################################################
        print("-"*100)
        print(f"""Installing courseware for all users in each workspace.""")
        self.__for_each_workspace(self.__workspaces, self.__install_courseware)

        #############################################################
        print("-"*100)
        print(f"""Starting the Workspace-Setup job in each workspace.""")
        self.__for_each_workspace(self.__workspaces, self.__run_workspace_setup_job)

        #############################################################
        print("-"*100)
        if self.run_workspace_setup:
            print(f"""Validating select indicators in each workspace.""")
            self.__for_each_workspace(self.__workspaces, self.__validate_workspace_setup)
        else:
            print("Skipping workspace validation, the Workspace-Setup job has not been run.")

        #############################################################
        print("-" * 100)
        if len(self.errors) == 0:
            print(f"""Completed setup for {len(self.__workspaces)} workspaces with no errors.""")
        else:
            print(f"""Completed setup for {len(self.__workspaces)} workspaces with {len(self.errors)} errors.""")
            print("Errors:")
            for error in self.errors:
                print(error)
                print("-"*100)

    def __create_workspace(self, workspace_config: WorkspaceConfig):
        from dbacademy.clients.classrooms.classroom import Classroom

        workspace_api = self.accounts_api.workspaces.get_by_name(workspace_config.name, if_not_exists="ignore")
        if workspace_api is None:
            # It doesn't exist, so go ahead and create it.
            try:
                workspace_api = self.accounts_api.workspaces.create(workspace_name=workspace_config.name,
                                                                    deployment_name=workspace_config.name,
                                                                    region=self.account_config.region,
                                                                    credentials_name=self.account_config.workspace_config_template.credentials_name,
                                                                    storage_configuration_name=self.account_config.workspace_config_template.storage_configuration)
            except DatabricksApiException as e:
                raise Exception from e

        classroom = Classroom(num_students=len(workspace_config.usernames),
                              username_pattern=workspace_config.username_pattern,
                              databricks_api=workspace_api)

        self.__workspaces.append(WorkspaceTrio(workspace_config, workspace_api, classroom))

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
                trio.client.scim.users.delete_by_id(user_id)
                # trio.classroom.databricks.users.delete_by_id(user_id)

        # Force a reload
        trio.existing_users = None

    def __create_users(self, trio: WorkspaceTrio):
        # Just in case it's not ready
        trio.workspace_api.wait_until_ready()

        name = trio.workspace_config.name
        max_users = len(trio.workspace_config.usernames)

        existing_usernames = [u.get("userName") for u in trio.existing_users]
        remaining_count = max_users-len(existing_usernames)+1
        print(f"""Configuring {remaining_count} of {max_users+1} users for "{name}".""")

        for username in trio.workspace_config.usernames:
            if username in existing_usernames:
                user = trio.get_by_username(username)
            else:
                try:
                    user = trio.client.scim.users.create(username)
                except DatabricksApiException as e:
                    user = trio.client.scim.users.get_by_username(username)
                    if user is None:
                        user = trio.client.scim.users.create(username)
                        print(f"| {e.message}|")
                        print("F$@# LIARS !!!")
                    elif e.http_code == 409:
                        existing = username in existing_usernames
                        print("-"*100)
                        print(f"409 creating user {username}, existing: {existing}")
                        print(f"| {e.message}|")
                        print("-"*100)
                    else:
                        raise Exception(f"Failed to create user {username} for {name}") from e

            if username != self.account_config.username:
                self.__remove_entitlement(trio, user, "allow-cluster-create")
                self.__remove_entitlement(trio, user, "databricks-sql-access")
                self.__remove_entitlement(trio, user, "workspace-access")

        # Force a reload
        trio.existing_users = None

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
        try:
            # Just in case it's not ready
            trio.workspace_api.wait_until_ready()

            group = trio.client.scim.groups.get_by_name("users")

            for name, value in trio.workspace_config.entitlements.items():
                if value is True:
                    trio.client.scim.groups.add_entitlement(group.get("id"), name)
                else:
                    trio.client.scim.groups.remove_entitlement(group.get("id"), name)
        except Exception as e:
            raise Exception(f"""Failed to update entitlements for workspace #{trio.number}.""") from e

    @classmethod
    def __create_group(cls, trio: WorkspaceTrio):
        name = trio.workspace_config.name

        existing_groups = trio.client.scim.groups.list()
        existing_groups_name = [g.get("displayName") for g in existing_groups]

        print(f"""Found {len(existing_groups)} groups for "{name}".""")

        for group_name, usernames in trio.workspace_config.workspace_group.items():
            if group_name not in existing_groups_name:
                trio.client.scim.groups.create(group_name)
                print(f"""Created the group "{group_name} for {name}.""")

            if group_name != "users":
                # We still have to add the specified user to this group (User #0 to "admins" in our case)
                group = trio.client.scim.groups.get_by_name(group_name)
                for username in usernames:
                    # Add the user as a member of that group
                    user = trio.client.scim.users.get_by_username(username)
                    if user is None:
                        raise Exception(f"""The user was None for "{name}". | {user}""")
                    elif group is None:
                        raise Exception(f"""The group was None for "{name}". | {group}""")
                    trio.client.scim.groups.add_member(group.get("id"), user.get("id"))

    def __validate_workspace_setup(self, trio: WorkspaceTrio) -> None:
        from datetime import datetime

        errors = list()
        errors.extend(self.__validate_workspace_setup_job(trio))
        if len(errors) == 0:
            errors.extend(self.__validate_pool_and_policies(trio))

        if len(errors) > 0:
            return

        new_url = f"""https://training-{trio.name}.cloud.databricks.com"""

        for record in self.__air_table_records:
            old_url = record.get("fields", dict()).get("AWS Workspace URL")
            if new_url == old_url:
                break
        else:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            comments = f"{now}: Created workspace via automation script."

            self.air_table_client.insert({
                "AWS Workspace URL": new_url,
                "Comments": comments
            })

    def __validate_pool_and_policies(self, trio: WorkspaceTrio) -> List[str]:
        from dbacademy.dbhelper import ClustersHelper

        errors = list()

        if trio.client.instance_pools.get_by_name(ClustersHelper.POOL_DEFAULT_NAME) is None:
            errors.append(self.log_error(f"""The instance pool "{ClustersHelper.POOL_DEFAULT_NAME}" was not found for {trio.name}"""))

        for policy_name in [ClustersHelper.POLICY_ALL_PURPOSE, ClustersHelper.POLICY_JOBS_ONLY, ClustersHelper.POLICY_DLT_ONLY]:
            if trio.client.cluster_policies.get_by_name(policy_name) is None:
                errors.append(self.log_error(f"""The cluster policy "{policy_name}" was not found for {trio.name}"""))

        return errors

    def __validate_workspace_setup_job(self, trio: WorkspaceTrio) -> List[str]:
        from dbacademy.dbhelper import WorkspaceHelper

        # Check the Workspace Setup job should be last
        job = trio.client.jobs.get_by_name(WorkspaceHelper.WORKSPACE_SETUP_JOB_NAME)
        if job is None:
            return [self.log_error(f"Workspace-Setup job not found for {trio.name}")]

        runs = trio.client.runs.list_by_job_id(job.get("job_id"))
        runs = sorted(runs, key=lambda r: r.get("start_time"), reverse=True)
        if len(runs) == 0:
            return [self.log_error(f"Workspace-Setup job has zero runs for {trio.name}")]

        run_id = runs[0].get("run_id")
        state = runs[0].get("state", dict())
        life_cycle_state = state.get("life_cycle_state")
        result_state = state.get("result_state")
        state_message = state.get("state_message")
        if life_cycle_state != "TERMINATED":
            return [self.log_error(f"""Workspace-Setup, run #{run_id} was not TERMINATED, found "{life_cycle_state}" for {trio.name} | {state_message}""")]
        elif result_state != "SUCCESS":
            return [self.log_error(f"""Workspace-Setup, run #{run_id} was not SUCCESS, found "{result_state}" for {trio.name} | {state_message}""")]
        else:
            return list()

    def __run_workspace_setup_job(self, trio: WorkspaceTrio):
        import time
        import traceback
        from dbacademy.dbhelper import WorkspaceHelper

        start = time.time()
        try:
            # If the job exist delete it to address cases where the job is misconfigured
            # and leaving it would only result in re-running the misconfigured job.
            trio.client.jobs.delete_by_name(WorkspaceHelper.WORKSPACE_SETUP_JOB_NAME, success_only=False)

            # We deleted it or it never existed.
            job_id = self.__create_job(trio)

            if self.run_workspace_setup:
                run = trio.client.jobs.run_now(job_id)
                run_id = run.get("run_id")

                response = self.__wait_for_job_run(trio, job_id, run_id)

                # The job has completed, now we need to evaluate the final state.
                state = response.get("state", dict())
                state_message = state.get("state_message", "Unknown")
                if state is None:
                    raise AssertionError("The job's state object is missing.")

                life_cycle_state = state.get("life_cycle_state")
                if life_cycle_state == "SKIPPED":
                    print(f"""Skipped Universal-Workspace-Setup (job #{job_id}, run #{run_id}) for "{trio.name}" | {state_message}.""")
                    return

                elif life_cycle_state != "TERMINATED":
                    raise Exception(f"""Expected the final life cycle state of Universal-Workspace-Setup to be "TERMINATED", found "{life_cycle_state}" for "{trio.name}" | {state_message}""")

                else:
                    result_state = state.get("result_state")
                    if result_state != "SUCCESS":
                        raise Exception(f"""Expected the final state of Universal-Workspace-Setup to be "SUCCESS", found "{result_state}" for "{trio.name}" | {state_message}""")

                duration = int((time.time() - start) / 60)
                print(f"""Finished Universal-Workspace-Setup (job #{job_id}, run #{run_id}) for "{trio.name}" ({duration} minutes).""")

        except Exception as e:
            self.log_error(f"""Failed executing Universal-Workspace-Setup for "{trio.workspace_config.name}".\n{str(e)}\n{traceback.format_exc()}""")

    @staticmethod
    def __create_job(trio: WorkspaceTrio) -> str:
        import requests, json, os

        config_text = requests.get("https://raw.githubusercontent.com/databricks-academy/workspace-setup/main/universal-workspace-setup-job-config.json").text
        config_text = config_text.replace("{{event_id}}", f"{trio.number:03d}")
        config_text = config_text.replace("{{event_description}}", trio.name)
        config_text = config_text.replace("{{deployment_context}}", f"Learning Platforms Workspace {trio.number:03d}")

        config_text = config_text.replace("{{pools_node_type_id}}", "i3.xlarge")
        config_text = config_text.replace("{{default_spark_version}}", "11.3.x-cpu-ml-scala2.12")

        config_text = config_text.replace("aws:node_type_id", "node_type_id")
        config_text = config_text.replace("aws:aws_attributes", "aws_attributes")

        install_all = True
        if install_all:
            config_text = config_text.replace("{{courses}}", str(None))
            config_text = config_text.replace("{{datasets}}", str(None))
        else:
            token = os.environ.get("CDS_DOWNLOAD_TOKEN")
            config_text = config_text.replace("{{courses}}", f"course=example-course&version=v1.3.2&token={token}")
            config_text = config_text.replace("{{datasets}}", f"example-course:v01")

        config = json.loads(config_text)

        job_id = trio.client.jobs.create_from_dict(config)
        return job_id

    def __wait_for_job_run(self, trio: WorkspaceTrio, job_id: str, run_id: str):
        import time

        wait = 15
        new_run = trio.client.runs.get(run_id)

        life_cycle_state = new_run.get("state", dict()).get("life_cycle_state")

        if life_cycle_state == "SKIPPED":
            # For some reason, the job was aborted and then restarted.
            # Rather than simply reporting skipped, we want to get the
            # current run_id and resume monitoring from there.
            runs = trio.client.runs.list_by_job_id(job_id)
            for past_runs in runs:
                life_cycle_state = past_runs.get("state", dict()).get("life_cycle_state")
                if life_cycle_state == "RUNNING":
                    return self.__wait_for_job_run(trio, job_id, past_runs.get("run_id"))

            return new_run

        elif life_cycle_state != "TERMINATED" and life_cycle_state != "INTERNAL_ERROR":
            if life_cycle_state == "PENDING" or life_cycle_state == "RUNNING":
                time.sleep(wait)
            else:
                time.sleep(5)

            return self.__wait_for_job_run(trio, job_id, run_id)

        return new_run

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
