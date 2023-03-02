from typing import List, Optional, Callable

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

    def __init__(self, account_config: AccountConfig):
        from dbacademy.dougrest import AccountsApi

        assert account_config is not None, f"""The parameter "account_config" must be specified."""
        self.__account_config = account_config
        self.__workspaces: List[WorkspaceTrio] = list()

        self.__accounts_api = AccountsApi(account_id=self.account_config.account_id,
                                          user=self.account_config.username,
                                          password=self.account_config.password)

    @property
    def accounts_api(self):
        return self.__accounts_api

    @property
    def workspaces(self) -> List[WorkspaceTrio]:
        return self.__workspaces

    @property
    def account_config(self) -> AccountConfig:
        return self.__account_config

    def create_workspaces(self):
        from dbacademy.classrooms.classroom import Classroom

        print("\n")
        print("-"*100)
        print(f"""Creating {len(self.account_config.workspaces)} workspaces.""")

        for workspace_config in self.account_config.workspaces:
            name = workspace_config.name
            print(f"""Creating the workspace "{name}".""")

            workspace_api = self.accounts_api.workspaces.get_by_name(name, if_not_exists="ignore")
            if workspace_api is None:
                # It doesn't exist, so go ahead and create it.
                workspace_api = self.accounts_api.workspaces.create(workspace_name=name,
                                                                    deployment_name=name,
                                                                    region=self.account_config.region,
                                                                    credentials_name=self.account_config.workspace_config_template.credentials_name,
                                                                    storage_configuration_name=self.account_config.workspace_config_template.storage_configuration)

            classroom = Classroom(num_students=workspace_config.max_user_count,
                                  username_pattern=workspace_config.username_pattern,
                                  databricks_api=workspace_api)

            self.__workspaces.append(WorkspaceTrio(workspace_config, workspace_api, classroom))

        #############################################################
        # Workspaces were all created synchronously, blocking here until we confirm that all workspaces are created.
        print("-"*100)
        for workspace in self.workspaces:
            print(f"""Waiting for workspace "{workspace.workspace_config.name}" to finish provisioning...""")
            workspace.workspace_api.wait_until_ready()

        #############################################################
        # Create users
        self.for_each_workspace(self.__for_workspace_create_users)

        #############################################################
        # Create the meta stores for each workspace
        self.for_each_workspace(self.__for_workspace_create_metastore)

        #############################################################
        # Create workspace setup job
        self.for_each_workspace(self.__for_workspace_start_universal_workspace_setup)

        print(f"""Completed setup for {len(self.account_config.workspaces)} workspaces.""")

    def for_each_workspace(self, some_action: Callable[[WorkspaceTrio], None]) -> None:
        # from multiprocessing.pool import ThreadPool

        print("-"*100)

        # with ThreadPool(len(self.workspaces)) as pool:
        #     pool.map(self.some_action, self.workspaces)

        for trio in self.workspaces:
            some_action(trio)

    def delete_workspaces(self):
        print("\n")
        print("-"*100)

        for workspace_config in self.account_config.workspaces:
            workspace_api = self.accounts_api.workspaces.get_by_name(workspace_config.name, if_not_exists="ignore")

            if workspace_api is not None:
                trio = WorkspaceTrio(workspace_config, workspace_api, None)
                self.workspaces.append(trio)

        print(f"Destroying {len(self.workspaces)} workspaces.""")
        for trio in self.workspaces:
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
    def __for_workspace_create_users(trio: WorkspaceTrio):
        # Just in case it's not ready
        trio.workspace_api.wait_until_ready()

        name = trio.workspace_config.name
        max_user_count = trio.workspace_config.max_user_count

        print(f"""Creating {max_user_count} users for "{name}".""")
        trio.classroom.create_users()

    def __for_workspace_start_universal_workspace_setup(self, trio: WorkspaceTrio):
        from dbacademy.classrooms.monitor import Commands
        try:
            workspace_config = trio.workspace_config
            name = workspace_config.name

            # Just in case it's not ready
            trio.workspace_api.wait_until_ready()

            print(f"""Starting Universal-Workspace-Setup for "{name}" """)
            Commands.universal_setup(trio.workspace_api,
                                     node_type_id=workspace_config.default_node_type_id,
                                     spark_version=workspace_config.default_dbr,
                                     datasets=workspace_config.datasets,
                                     lab_id=self.account_config.event_config.event_id,
                                     description=self.account_config.event_config.description)

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
