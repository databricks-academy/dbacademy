from typing import List, Union, Optional


class AccountConfig:
    from dbacademy.workspaces_3_0.uc_storage_config_class import UcStorageConfig
    from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig

    @staticmethod
    def from_env(*, account_id_env_name, account_password_env_name, account_username_env_name, region: str, uc_storage_config: UcStorageConfig, workspace_config_template: WorkspaceConfig, first_workspace_number: int, total_workspaces: int, specific_workspace_numbers: Optional[List[int]], ignored_workspaces: List[Union[int, str]] = None) -> "AccountConfig":
        import os
        print()

        account_id = os.environ.get(account_id_env_name)
        assert account_id is not None, f"""Failed to load the environment variable "{account_id_env_name}", please check your configuration and try again."""

        password = os.environ.get(account_password_env_name)
        assert password is not None, f"""Failed to load the environment variable "{account_password_env_name}", please check your configuration and try again."""

        username = os.environ.get(account_username_env_name)
        assert username is not None, f"""Failed to load the environment variable "{account_username_env_name}", please check your configuration and try again."""

        return AccountConfig(region=region,
                             account_id=account_id,
                             username=username,
                             password=password,
                             uc_storage_config=uc_storage_config,
                             workspace_config_template=workspace_config_template,
                             ignored_workspaces=ignored_workspaces,
                             first_workspace_number=first_workspace_number,
                             specific_workspace_numbers=specific_workspace_numbers,
                             total_workspaces=total_workspaces)

    def __init__(self, *, region: str, account_id: str, username: str, password: str, uc_storage_config: UcStorageConfig, workspace_config_template: WorkspaceConfig, ignored_workspaces: List[Union[int, str]] = None, first_workspace_number: int, total_workspaces: int, specific_workspace_numbers: List[int]) -> None:
        """
        Creates the configuration for account-level settings.
        """
        from dbacademy import common
        from dbacademy.workspaces_3_0.uc_storage_config_class import UcStorageConfig
        from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig

        self.__account_id = common.verify_type(str, min_length=1, account_id=account_id)
        self.__password = common.verify_type(str, min_length=1, password=password)
        self.__username = common.verify_type(str, min_length=1, username=username)

        print(f"Account ID: {account_id}")
        print(f"Password:   {password[0]}***{password[-1]}")
        print(f"Username:   {username}")

        self.__region = common.verify_type(str, min_length=1, region=region)
        self.__ignored_workspaces = common.verify_type(list, non_none=True, ignored_workspaces=ignored_workspaces)
        self.__uc_storage_config = common.verify_type(UcStorageConfig, non_none=True, uc_storage_config=uc_storage_config)
        self.__workspace_config_template = common.verify_type(WorkspaceConfig, non_none=True, workspace_config_template=workspace_config_template)

        self.__workspaces = list()

        if specific_workspace_numbers is None:
            assert total_workspaces == 0, f"""The parameter "total_workspaces" must be 0 if the parameter "total_workspaces" is non-None."""
            assert first_workspace_number == 0, f"""The parameter "first_workspace_number" must be 0 if the parameter "specific_workspace_numbers" is non-None."""
            specific_workspace_numbers = range(first_workspace_number, first_workspace_number+total_workspaces)

        for workspace_number in specific_workspace_numbers:
            self.create_workspace_config(template=workspace_config_template,
                                         workspace_number=workspace_number)

    def create_workspace_config(self, *, template: WorkspaceConfig, workspace_number: int) -> WorkspaceConfig:
        from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig

        workspace = WorkspaceConfig(workspace_number=workspace_number,
                                    max_participants=template.max_participants,
                                    course_definitions=template.course_definitions,
                                    cds_api_token=template.cds_api_token,
                                    datasets=template.datasets,
                                    default_node_type_id=template.default_node_type_id,
                                    default_dbr=template.default_dbr,
                                    credentials_name=template.credentials_name,
                                    storage_configuration=template.storage_configuration,
                                    username_pattern=template.username_pattern,
                                    entitlements=template.entitlements,
                                    workspace_group=template.workspace_group,
                                    workspace_name_pattern=template.workspace_name_pattern)

        if workspace.name in self.ignored_workspaces:
            print(f"Skipping workspace #{workspace.name}")
        elif workspace_number in self.ignored_workspaces:
            print(f"Skipping workspace #{workspace_number}")
        else:
            self.__workspaces.append(workspace)

        return workspace

    @property
    def ignored_workspaces(self) -> List[Union[int, str]]:
        return self.__ignored_workspaces

    @property
    def workspaces(self) -> List[WorkspaceConfig]:
        return self.__workspaces

    @property
    def region(self) -> str:
        return self.__region

    @property
    def account_id(self) -> str:
        return self.__account_id

    @property
    def username(self) -> str:
        return self.__username

    @property
    def password(self) -> str:
        return self.__password

    @property
    def uc_storage_config(self) -> UcStorageConfig:
        return self.__uc_storage_config

    @property
    def workspace_config_template(self) -> WorkspaceConfig:
        return self.__workspace_config_template
