__all__ = ["AccountConfig"]

from typing import List, Union, Optional
from dbacademy.common import validate


class AccountConfig:
    from dbacademy.jobs.pools.uc_storage_config_class import UcStorageConfig
    from dbacademy.jobs.pools.workspace_config_classe import WorkspaceConfig

    @staticmethod
    def from_env(*, account_id_env_name, account_password_env_name, account_username_env_name, region: str, uc_storage_config: UcStorageConfig, workspace_config_template: WorkspaceConfig, workspace_numbers: List[int], ignored_workspaces: List[Union[int, str]] = None) -> "AccountConfig":
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
                             workspace_numbers=workspace_numbers)

    def __init__(self, *,
                 region: str,
                 account_id: str,
                 username: str,
                 password: str,
                 uc_storage_config: UcStorageConfig,
                 workspace_config_template: WorkspaceConfig,
                 ignored_workspaces: Optional[List[str]] = None,
                 workspace_numbers: List[int]) -> None:
        """
        Creates the configuration for account-level settings.
        """
        from dbacademy.jobs.pools.uc_storage_config_class import UcStorageConfig
        from dbacademy.jobs.pools.workspace_config_classe import WorkspaceConfig

        self.__account_id = validate(account_id=account_id).required.str(min_length=1)
        self.__password = validate(password=password).required.str(min_length=1)
        self.__username = validate(username=username).required.str(min_length=1)

        print(f"Account ID:    {account_id}")
        print(f"Password:      {password[0]}***{password[-1]}")
        print(f"Username:      {username}")

        self.__region = validate(region=region).required.str(min_length=1)
        self.__ignored_workspaces = validate(ignored_workspaces=ignored_workspaces).optional.list(str, auto_create=True)
        self.__uc_storage_config = validate(uc_storage_config=uc_storage_config).required.as_type(UcStorageConfig)
        self.__workspace_config_template = validate(workspace_config_template=workspace_config_template).required.as_type(WorkspaceConfig)

        self.__workspaces = list()
        self.__workspace_numbers = validate(workspace_numbers=workspace_numbers).required.list(int)

        for workspace_number in self.workspace_numbers:
            self.create_workspace_config(template=workspace_config_template,
                                         workspace_number=workspace_number)

    def create_workspace_config(self, *, template: WorkspaceConfig, workspace_number: int) -> WorkspaceConfig:
        from dbacademy.jobs.pools.workspace_config_classe import WorkspaceConfig

        workspace = WorkspaceConfig(workspace_number=workspace_number,
                                    max_participants=template.max_participants,
                                    default_node_type_id=template.default_node_type_id,
                                    credentials_name=template.credentials_name,
                                    storage_configuration=template.storage_configuration,
                                    username_pattern=template.username_pattern,
                                    entitlements=template.entitlements,
                                    workspace_group=template.workspace_group,
                                    workspace_name_pattern=template.workspace_name_pattern)

        if workspace.name in self.ignored_workspaces:
            # print(f"Skipping:   {workspace.name}")
            pass
        elif workspace_number in self.ignored_workspaces:
            # print(f"Skipping:    #{workspace_number}")
            pass
        else:
            self.__workspaces.append(workspace)

        return workspace

    @property
    def workspace_numbers(self) -> List[int]:
        return self.__workspace_numbers

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
