from typing import List, Optional, Callable, Iterable

from dbacademy.rest.common import DatabricksApiException


class WorkspaceTrio:
    from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig
    from dbacademy.dougrest.accounts.workspaces import Workspace as WorkspaceAPI
    from dbacademy.classrooms.classroom import Classroom

    def __init__(self, workspace_config: WorkspaceConfig, workspace_api: WorkspaceAPI, classroom: Optional[Classroom]):
        self.__workspace_config = workspace_config
        self.__workspace_api = workspace_api
        self.__classroom = classroom

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

        assert account_config is not None, f"""The parameter "account_config" must be specified."""
        self.__account_config = account_config
        # self.__workspaces: List[WorkspaceTrio] = list()

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

    def create_workspaces(self, *, create_users: bool, create_groups: bool, create_metastore: bool, run_workspace_setup: bool, enable_features: bool, workspace_numbers: Iterable[int] = None):
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

            classroom = Classroom(num_students=workspace_config.max_users,
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
            self.for_each_workspace(workspaces, self.__for_workspace_create_users)
        else:
            print("Skipping creation of users")

        #############################################################
        if create_groups:
            self.for_each_workspace(workspaces, self.__for_workspace_create_group)
        else:
            print("Skipping creation of groups")

        #############################################################
        if create_metastore:
            self.for_each_workspace(workspaces, self.__for_workspace_create_metastore)
        else:
            print("Skipping creation of metastore")

        #############################################################
        if enable_features:
            self.for_each_workspace(workspaces, self.__for_workspace_enable_features)
        else:
            print("Skipping enablement of workspace features")

        print(f"""Completed setup for {len(provisioned_workspaces)} workspaces.""")

        #############################################################
        if run_workspace_setup:
            self.for_each_workspace(workspaces, self.__for_workspace_start_universal_workspace_setup)
        else:
            print("Skipping run of Universal-Workspace-Setup")

        print(f"""Completed setup for {len(provisioned_workspaces)} workspaces.""")


    @staticmethod
    def for_each_workspace(workspaces: List[WorkspaceTrio], some_action: Callable[[WorkspaceTrio], None]) -> None:
        from multiprocessing.pool import ThreadPool

        print("-"*100)

        with ThreadPool(len(workspaces)) as pool:
            pool.map(some_action, workspaces)

        # for trio in workspaces:
        #     some_action(trio)

    def delete_workspaces(self):
        print("\n")
        print("-"*100)

        workspaces: List[WorkspaceTrio] = list()

        for workspace_config in self.account_config.workspaces:
            workspace_api = self.accounts_api.workspaces.get_by_name(workspace_config.name, if_not_exists="ignore")

            if workspace_api is not None:
                trio = WorkspaceTrio(workspace_config, workspace_api, None)
                workspaces.append(trio)

        print(f"Destroying {len(workspaces)} workspaces.""")
        for trio in workspaces:
            name = trio.workspace_config.name

            self.__for_workspace_destroy_metastore(trio)

            try:
                print(f"""Deleting the workspace "{name}".""")
                self.accounts_api.workspaces.delete_by_name(name)

            except DatabricksApiException as e:
                if e.http_code == 404:
                    print(f"""Cannot delete workspace "{name}"; it doesn't exist.""")
                else:
                    print("-"*80)
                    print(f"""Failed to delete the workspace "{name}".\n{e}""")
                    print("-"*80)

    @staticmethod
    def __for_workspace_enable_features(trio: WorkspaceTrio):
        # Enabling serverless endpoints.
        settings = trio.workspace_api.api("GET", "2.0/sql/config/endpoints")
        feature_name = "enable_serverless_compute"
        feature_status = True
        settings[feature_name] = feature_status
        trio.workspace_api.api("PUT", "2.0/sql/config/endpoints", settings)

        new_feature_status = trio.workspace_api.api("GET", "2.0/sql/config/endpoints").get(feature_name)
        assert new_feature_status == feature_status, f"""Expected "{feature_name}" to be "{feature_status}", found "{new_feature_status}"."""

    def __for_workspace_create_users(self, trio: WorkspaceTrio):
        # Just in case it's not ready
        trio.workspace_api.wait_until_ready()

        name = trio.workspace_config.name
        max_users = trio.workspace_config.max_users

        print(f"""Creating {max_users} users for "{name}".""")

        existing_users = self.__list_existing_users(trio)

        if len(existing_users) > 1:
            print(f"""Found {len(existing_users)} users for "{name}".""")

        for i, username in enumerate(trio.workspace_config.users):
            if username not in existing_users:
                # TODO parameterize allow_cluster_create
                # trio.classroom.databricks.users.create(username, allow_cluster_create=False)
                self.__create_user(trio, username)

                if i == 0:  # User 0 gets admin rights.
                    trio.classroom.databricks.groups.add_member("admins", user_name=username)
                    # TODO add user zero to the instructors group which may need to be created first
                    # trio.classroom.databricks.groups.add_member("instructors", user_name=username)

    def __create_user(self, trio: WorkspaceTrio, username) -> List[str]:
        import time, random
        from requests.exceptions import HTTPError

        for i in range(0, self.__max_retries):
            try:
                return trio.classroom.databricks.users.create(username, allow_cluster_create=False)

            except HTTPError as e:
                print(f"""Failed to list users for "{trio.workspace_config.name}", retrying ({i})""")
                data = e.response.json()
                message = data.get("detail", str(e))
                print(f"HTTPError ({e.response.status_code}): {message}")
                time.sleep(random.randint(5, 10))

        raise Exception(f"""Failed to create user "{username}" for "{trio.workspace_config.name}".""")

    def __list_existing_users(self, trio: WorkspaceTrio) -> List[str]:
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

        if existing_users is None:
            raise Exception(f"""Failed to load existing users for "{trio.workspace_config.name}".""")
        else:
            return existing_users

    @staticmethod
    def __for_workspace_create_group(trio: WorkspaceTrio):
        # Just in case it's not ready
        trio.workspace_api.wait_until_ready()

        name = trio.workspace_config.name

        print(f"""Creating {len(trio.workspace_config.groups)} groups for "{name}".""")

        existing_groups = trio.classroom.databricks.groups.list()

        if len(existing_groups) > 0:
            print(f"""Found {len(existing_groups)} groups for "{name}".""")

        for group, usernames in trio.workspace_config.groups.items():
            # Create the group

            if group not in existing_groups:
                trio.classroom.databricks.groups.create(group)

            for username in usernames:
                trio.classroom.databricks.groups.add_member(group, user_name=username)

    def __for_workspace_start_universal_workspace_setup(self, trio: WorkspaceTrio):
        from dbacademy.classrooms.monitor import Commands
        try:
            workspace_config = trio.workspace_config
            name = workspace_config.name

            # Just in case it's not ready
            trio.workspace_api.wait_until_ready()

            print(f"""Starting Universal-Workspace-Setup for "{name}" """)
            result = Commands.universal_setup(trio.workspace_api,
                                              node_type_id=workspace_config.default_node_type_id,
                                              spark_version=workspace_config.default_dbr,
                                              datasets=workspace_config.datasets,
                                              lab_id=self.account_config.event_config.event_id,
                                              description=self.account_config.event_config.description)

            assert result == "SUCCESS", f"""Expected final state of Universal-Workspace-Setup to be "SUCCESS", found "{result}"."""

            # TODO delete the Bootstrap job if successful, leaving the log-lived job.
            print(f"""Finished Universal-Workspace-Setup for "{name}" """)

        except Exception as e:
            raise Exception(f"""Failed to create the workspace "{trio.workspace_config.name}".""") from e

    def __for_workspace_create_metastore(self, trio: WorkspaceTrio):

        # Just in case it's not ready
        trio.workspace_api.wait_until_ready()

        # No-op if a metastore is already assigned to the workspace
        metastore_id = self.__get_or_create_metastore(trio)

        self.__enable_delta_sharing(trio, metastore_id)

        self.__update_uc_permissions(trio, metastore_id)

        self.__assign_metastore_to_workspace(trio, metastore_id)

    @staticmethod
    def __assign_metastore_to_workspace(trio: WorkspaceTrio, metastore_id: str):
        # Assign the metastore to the workspace
        workspace_id = trio.workspace_api.get("workspace_id")
        trio.workspace_api.api("PUT", f"2.1/unity-catalog/workspaces/{workspace_id}/metastore", {
            "metastore_id": metastore_id,
            "default_catalog_name": "main"
        })

    @staticmethod
    def __update_uc_permissions(trio: WorkspaceTrio, metastore_id: str):
        # Grant all users permission to create resources in the metastore
        trio.workspace_api.api("PATCH", f"2.1/unity-catalog/permissions/metastore/{metastore_id}", {
            "changes": [{
                "principal": "account users",
                "add": ["CREATE CATALOG", "CREATE EXTERNAL LOCATION", "CREATE SHARE", "CREATE RECIPIENT", "CREATE PROVIDER"]
            }]
        })

    def __enable_delta_sharing(self, trio: WorkspaceTrio, metastore_id: str):
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

    @staticmethod
    def __find_metastore(trio) -> Optional[str]:
        # According to the docs, there is no pagination for this call.
        response = trio.workspace_api.api("GET", "2.1/unity-catalog/metastores")
        metastores = response.get("metastores", list())
        for metastore in metastores:
            if metastore.get("name") == trio.workspace_config.name:
                return metastore.get("metastore_id")

        return None

    def __get_or_create_metastore(self, trio: WorkspaceTrio) -> str:
        try:
            # Check to see if a metastore is assigned
            metastore = trio.workspace_api.api("GET", f"2.1/unity-catalog/current-metastore-assignment")
            print(f"""Using previously assigned metastore for "{trio.workspace_config.name}".""")
            return metastore.get("metastore_id")

        except DatabricksApiException as e:
            if e.error_code != "METASTORE_DOES_NOT_EXIST":
                raise Exception(f"""Unexpected exception looking up metastore for workspace "{trio.workspace_config.name}".""") from e

        # No metastore is assigned, check to see if it exists.
        metastore_id = self.__find_metastore(trio)
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

    @staticmethod
    def __for_workspace_destroy_metastore(trio: WorkspaceTrio) -> None:
        name = trio.workspace_config.name

        # Unassign the metastore from the workspace
        print(f"""Deleting the metastore for workspace "{name}".""")

        response = trio.workspace_api.api("GET", f"2.1/unity-catalog/current-metastore-assignment")
        metastore_id = response["metastore_id"]
        workspace_id = response["workspace_id"]
        trio.workspace_api.api("DELETE", f"2.1/unity-catalog/workspaces/{workspace_id}/metastore", {
          "metastore_id": metastore_id
        })

        # Delete the metastore, but only if there are no other workspaces attached.
        trio.workspace_api.api("DELETE", f"2.1/unity-catalog/metastores/{metastore_id}", {
            "force": True
        })


