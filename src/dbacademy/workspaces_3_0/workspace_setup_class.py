from typing import List

from dbacademy.rest.common import DatabricksApiException


class WorkspacePair:
    from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig
    from dbacademy.dougrest.accounts.workspaces import Workspace as WorkspaceAPI

    def __init__(self, workspace_config: WorkspaceConfig, workspace_api: WorkspaceAPI):
        self.__workspace_config = workspace_config
        self.__workspace_api = workspace_api

    @property
    def workspace_config(self) -> WorkspaceConfig:
        return self.__workspace_config

    @property
    def workspace_api(self) -> WorkspaceAPI:
        return self.__workspace_api


class WorkspaceSetup:
    from dbacademy.workspaces_3_0.account_config_class import AccountConfig

    def __init__(self, account_config: AccountConfig):
        from dbacademy.dougrest import AccountsApi

        assert account_config is not None, f"""The parameter "account_config" must be specified."""
        self.__account_config = account_config
        self.__workspaces: List[WorkspacePair] = list()

        self.__accounts_api = AccountsApi(account_id=self.account_config.account_id,
                                          user=self.account_config.username,
                                          password=self.account_config.password)

    @property
    def accounts_api(self):
        return self.__accounts_api

    @property
    def workspaces(self) -> List[WorkspacePair]:
        return self.__workspaces

    @property
    def account_config(self) -> AccountConfig:
        return self.__account_config

    def __setup_workspace(self, pair: WorkspacePair):
        from dbacademy.classrooms.classroom import Classroom
        from dbacademy.classrooms.monitor import Commands
        try:
            workspace_api = pair.workspace_api
            workspace_config = pair.workspace_config
            name = workspace_config.name

            classroom = Classroom(num_students=workspace_config.max_user_count,
                                  username_pattern=workspace_config.username_pattern,
                                  databricks_api=workspace_api)

            print(f"""Waiting for workspace "{name}" to finish provisioning..""")
            workspace_api.wait_until_ready()

            print(f"""Creating {workspace_config.max_user_count} users for "{name}".""")
            classroom.create_users()

            print(f"""Starting Universal-Workspace-Setup for "{name}" """)
            Commands.universal_setup(workspace_api,
                                     node_type_id=workspace_config.default_node_type_id,
                                     spark_version=workspace_config.default_dbr,
                                     datasets=workspace_config.datasets,
                                     lab_id=self.account_config.event_config.event_id,
                                     description=self.account_config.event_config.description)

            print(f"""Finished setup for "{name}" """)
        except Exception as e:
            raise Exception(f"""Failed to create the workspace "{pair.workspace_config.name}".""") from e

    def create_workspaces(self):
        # from multiprocessing.pool import ThreadPool

        for workspace_config in self.account_config.workspaces:
            name = workspace_config.name
            print(f"""Creating the workspace "{name}".""")

            workspace_api = self.accounts_api.workspaces.get_by_name(name, if_not_exists="ignore")
            if workspace_api is None:
                # It doesn't exist, so go ahead and create it.
                workspace_api = self.accounts_api.workspaces.create(workspace_name=name,
                                                                    deployment_name=name,
                                                                    region=self.account_config.region,
                                                                    credentials_name=self.account_config.storage_config.credentials_name,
                                                                    storage_configuration_name=self.account_config.storage_config.storage_configuration)

            self.__workspaces.append(WorkspacePair(workspace_config, workspace_api))

        for workspace in self.workspaces:
            self.__setup_workspace(workspace)

        # with ThreadPool(len(self.workspaces)) as pool:
        #     pool.map(self.__setup_workspace, self.workspaces)

    def delete_workspaces(self):
        for workspace_config in self.account_config.workspaces:
            name = workspace_config.name

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
