from typing import List, Optional, Callable, Iterable, Dict, Any

from dbacademy.rest.common import DatabricksApiException


class WorkspaceTrio:
    from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig
    from dbacademy.dougrest.accounts.workspaces import Workspace as WorkspaceAPI
    from dbacademy.classrooms.classroom import Classroom

    def __init__(self, workspace_config: WorkspaceConfig, workspace_api: WorkspaceAPI, classroom: Optional[Classroom]):
        self.__workspace_config = workspace_config
        self.__workspace_api = workspace_api
        self.__classroom = classroom

        self.__name = workspace_config.name
        self.__number = workspace_config.workspace_number

    def __str__(self) -> str:
        return f"{self.name} #{self.number}"

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

    def __init__(self, account_config: AccountConfig, max_retries: int):
        from dbacademy.dougrest import AccountsApi
        from dbacademy.workspaces_3_0.account_config_class import AccountConfig

        assert account_config is not None, f"""The parameter "account_config" must be specified."""
        assert type(account_config) == AccountConfig, f"""The parameter "account_config" must be of type AccountConfig, found {type(account_config)}."""
        self.__account_config = account_config

        assert max_retries is not None, f"""The parameter "max_retries" must be specified."""
        assert type(max_retries) == int, f"""The parameter "max_retries" must be of type int, found {type(max_retries)}."""
        self.__max_retries = max_retries

        self.__accounts_api = AccountsApi(account_id=self.account_config.account_id,
                                          user=self.account_config.username,
                                          password=self.account_config.password)

    @property
    def max_retries(self):
        return self.__max_retries

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

        # for trio in workspaces:
        #     some_action(trio)

    def remove_metastores(self):
        print("\n")
        print("-"*100)

        workspaces = self.__utils_create_workspace_trios()

        print(f"Removing {len(workspaces)} metastores.""")
        for trio in workspaces:
            # Unassign the metastore from the workspace
            print(f"""Deleting the metastore for workspace "{trio.workspace_config.name}".""")

            try:
                response = trio.workspace_api.api("GET", f"2.1/unity-catalog/current-metastore-assignment")
            except DatabricksApiException as e:
                if e.error_code == "METASTORE_DOES_NOT_EXIST":
                    continue  # It doesn't exist, move on.
                else:
                    raise e

            metastore_id = response["metastore_id"]
            workspace_id = response["workspace_id"]

            try:
                trio.workspace_api.api("DELETE", f"2.1/unity-catalog/workspaces/{workspace_id}/metastore", {
                  "metastore_id": metastore_id
                })
            except DatabricksApiException as e:
                if e.error_code == "METASTORE_DOES_NOT_EXIST":
                    continue  # It doesn't exist, move on.
                else:
                    raise e

            try:
                # Delete the metastore, but only if there are no other workspaces attached.
                trio.workspace_api.api("DELETE", f"2.1/unity-catalog/metastores/{metastore_id}", {
                    "force": True
                })
            except DatabricksApiException as e:
                if e.error_code == "METASTORE_DOES_NOT_EXIST":
                    continue  # It doesn't exist, move on.
                else:
                    raise e

    def __utils_create_workspace_trios(self) -> List[WorkspaceTrio]:
        workspaces: List[WorkspaceTrio] = list()

        for workspace_config in self.account_config.workspaces:
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

    def delete_workspaces(self):
        print("\n")
        print("-"*100)

        workspaces = self.__utils_create_workspace_trios()
        self.remove_metastores()

        print(f"Destroying {len(workspaces)} workspaces.""")
        for trio in workspaces:
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

    def create_workspaces(self, *, create_users: bool, create_groups: bool, create_metastore: bool, run_workspace_setup: bool, enable_features: bool, install_courseware: bool, uninstall_courseware: bool = False, workspace_numbers: Iterable[int] = None):
        from dbacademy.classrooms.classroom import Classroom

        print("\n")
        print("-"*100)
        print(f"""Creating {len(self.account_config.workspaces)} workspaces.""")

        provisioned_workspaces = [w for w in self.account_config.workspaces if workspace_numbers is None or w.workspace_number in workspace_numbers]
        workspaces: List[WorkspaceTrio] = list()

        for workspace_config in provisioned_workspaces:
            print(f"""Provisioning the workspace "{workspace_config.name}".""")

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
        if create_users:
            self.__for_each_workspace(workspaces, self.__create_workspaces_create_users)
        else:
            print("Skipping creation of users")

        #############################################################
        if create_groups:
            self.__for_each_workspace(workspaces, self.__create_workspaces_create_group)
        else:
            print("Skipping creation of groups")

        #############################################################
        if create_metastore:
            self.__for_each_workspace(workspaces, self.__create_workspaces_create_metastore)
        else:
            print("Skipping creation of metastore")

        #############################################################
        if enable_features:
            self.__for_each_workspace(workspaces, self.__create_workspaces_enable_features)
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
        from dbacademy.dbrest import DBAcademyRestClient
        from dbacademy.dbhelper import WorkspaceHelper

        if trio.workspace_api.url.endswith("/api/"):
            endpoint = trio.workspace_api.url[:-5]
        elif trio.workspace_api.url.endswith("/"):
            endpoint = trio.workspace_api.url[:-1]
        else:
            endpoint = trio.workspace_api.url

        client = DBAcademyRestClient(authorization_header=trio.workspace_api.authorization_header, endpoint=endpoint, throttle_seconds=1)

        for username in trio.workspace_config.usernames:

            print(f"Installing courses for {username}")

            for course_def in trio.workspace_config.course_definitions:
                print()

                url, course, version, artifact, token = WorkspaceHelper.parse_course_args(course_def)
                download_url = WorkspaceHelper.compose_courseware_url(url, course, version, artifact, trio.workspace_config.cds_api_token)

                if trio.workspace_config.courseware_subdirectory is None:
                    install_dir = f"/Users/{username}/{course}"
                else:
                    install_dir = f"/Users/{username}/{trio.workspace_config.courseware_subdirectory}/{course}"

                print(f" - {install_dir}")

                files = client.workspace.ls(install_dir)
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

                    client.workspace.import_dbc_files(install_dir, download_url, local_file_path=local_file_path)
                    print(f" - Installed.")

            print("-" * 80)

    @classmethod
    def __uninstall_courseware(cls, trio: WorkspaceTrio):
        from dbacademy.dbrest import DBAcademyRestClient
        from dbacademy.dbhelper import WorkspaceHelper

        if trio.workspace_api.url.endswith("/api/"):
            endpoint = trio.workspace_api.url[:-5]
        elif trio.workspace_api.url.endswith("/"):
            endpoint = trio.workspace_api.url[:-1]
        else:
            endpoint = trio.workspace_api.url

        client = DBAcademyRestClient(authorization_header=trio.workspace_api.authorization_header, endpoint=endpoint, throttle_seconds=1)
        usernames = [u.get("userName") for u in client.scim.users.list()]

        for username in usernames:
            print(f"Uninstalling courses for {username}")

            for course_def in trio.workspace_config.course_definitions:
                url, course, version, artifact, token = WorkspaceHelper.parse_course_args(course_def)

                if trio.workspace_config.courseware_subdirectory is None:
                    install_dir = f"/Users/{username}/{course}"
                else:
                    install_dir = f"/Users/{username}/{trio.workspace_config.courseware_subdirectory}/{course}"

                print(install_dir)
                client.workspace.delete_path(install_dir)

            print("-" * 80)

    @classmethod
    def __create_workspaces_enable_features(cls, trio: WorkspaceTrio):
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

    def __create_workspaces_create_users(self, trio: WorkspaceTrio):
        # Just in case it's not ready
        trio.workspace_api.wait_until_ready()

        name = trio.workspace_config.name
        max_users = len(trio.workspace_config.usernames)
        print(f"""Creating {max_users} users for "{name}".""")

        existing_users = self.__utils_list_users(trio)

        if len(existing_users) > 1:
            # More than one because "this" account is automatically added
            print(f"""Found {len(existing_users)} users for "{name}".""")

        for i, username in enumerate(trio.workspace_config.usernames):
            if username not in existing_users:
                entitlements = trio.workspace_config.entitlements
                self.__utils_create_user(trio, username, entitlements)

    def __utils_create_user(self, trio: WorkspaceTrio, username: str, entitlements: Dict[str, bool]) -> List[str]:
        import time, random
        from requests.exceptions import HTTPError

        entitlements_list = list()

        for entitlement_name, entitlement_value in entitlements.items():
            if entitlement_value is True:
                entitlements_list.append(entitlement_name)
            else:
                raise Exception("Entitlement removal is not yet supported.")

        for i in range(0, self.__max_retries):
            try:
                return trio.classroom.databricks.users.create(username=username,
                                                              allow_cluster_create=False,
                                                              entitlements=entitlements_list,
                                                              if_exists="ignore")
            except HTTPError as e:
                print(f"""Failed to list users for "{trio.workspace_config.name}", retrying ({i})""")
                data = e.response.json()
                message = data.get("detail", str(e))
                print(f"HTTPError ({e.response.status_code}): {message}")
                time.sleep(random.randint(5, 10))

        raise Exception(f"""Failed to create user "{username}" for "{trio.workspace_config.name}".""")

    def __utils_list_users(self, trio: WorkspaceTrio) -> List[str]:
        import time, random
        from requests.exceptions import HTTPError

        existing_users = None
        for i in range(0, self.__max_retries):
            if existing_users is not None:
                break
            try:
                existing_users = trio.classroom.databricks.users.list_usernames()

            except HTTPError as e:
                print(f"""Failed to list users for "{trio.workspace_config.name}", retrying ({i})""")
                data = e.response.json()
                message = data.get("detail", str(e))
                print(f"HTTPError ({e.response.status_code}): {message}")
                time.sleep(random.randint(5, 10))
                raise e

        if existing_users is None:
            raise Exception(f"""Failed to load existing users for "{trio.workspace_config.name}".""")
        else:
            return existing_users

    @classmethod
    def __create_workspaces_create_group(cls, trio: WorkspaceTrio):
        name = trio.workspace_config.name

        print(f"""Creating {len(trio.workspace_config.groups)} groups for "{name}".""")

        existing_groups = trio.classroom.databricks.groups.list()

        if len(existing_groups) > 0:
            print(f"""Found {len(existing_groups)} groups for "{name}".""")

        for group_name, usernames in trio.workspace_config.groups.items():
            if group_name not in existing_groups:
                # Create the group
                trio.classroom.databricks.groups.create(group_name)

            for username in usernames:
                # Add the user as a member of that group
                trio.classroom.databricks.groups.add_member(group_name, user_name=username)

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

    def __utils_create_workspace_setup_job(self, trio: WorkspaceTrio):
        from dbacademy.classrooms.monitor import Commands

        lab_id = self.account_config.event_config.event_id
        lab_id = lab_id if lab_id > 0 else trio.workspace_config.workspace_number

        description = self.account_config.event_config.description
        description = description if description is not None and len(description.strip()) > 0 else f"Classroom {trio.workspace_config.workspace_number}"

        state = Commands.universal_setup(trio.workspace_api,
                                         node_type_id=trio.workspace_config.default_node_type_id,
                                         spark_version=trio.workspace_config.default_dbr,
                                         datasets=trio.workspace_config.datasets,
                                         lab_id=lab_id,
                                         description=description,
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

    def __create_workspaces_create_metastore(self, trio: WorkspaceTrio):

        # Just in case it's not ready
        trio.workspace_api.wait_until_ready()

        # No-op if a metastore is already assigned to the workspace
        metastore_id = self.__utils_get_or_create_metastore(trio)

        self.__utils_enable_delta_sharing(trio, metastore_id)

        self.__utils_update_uc_permissions(trio, metastore_id)

        self.__utils_assign_metastore_to_workspace(trio, metastore_id)

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
        # Grant all users permission to create resources in the metastore
        trio.workspace_api.api("PATCH", f"2.1/unity-catalog/permissions/metastore/{metastore_id}", {
            "changes": [{
                "principal": "account users",
                "add": ["CREATE CATALOG", "CREATE EXTERNAL LOCATION", "CREATE SHARE", "CREATE RECIPIENT", "CREATE PROVIDER"]
            }]
        })

    def __utils_enable_delta_sharing(self, trio: WorkspaceTrio, metastore_id: str):
        try:
            trio.workspace_api.api("PATCH", f"2.1/unity-catalog/metastores/{metastore_id}", {
                "owner": self.account_config.uc_storage_config.owner,
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
            "storage_root_credential_id": self.account_config.uc_storage_config.storage_root_credential_id,
            "region": self.account_config.uc_storage_config.region
        })
        return metastore.get("metastore_id")
