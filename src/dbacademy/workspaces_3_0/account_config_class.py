from typing import List, Union


class AccountConfig:
    from dbacademy.workspaces_3_0.event_config_class import EventConfig
    from dbacademy.workspaces_3_0.uc_storage_config_class import UcStorageConfig
    from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig

    @staticmethod
    def from_env(account_name: str, region: str, event_config: EventConfig, uc_storage_config: UcStorageConfig, workspace_config_template: WorkspaceConfig, first_workspace_number: int, ignored_workspaces: List[Union[int, str]] = None) -> "AccountConfig":
        import os
        print()

        env = f"WORKSPACE_SETUP_{account_name}_ACCOUNT_ID"
        account_id = os.environ.get(env)
        assert account_id is not None, f"""Failed to load the environment variable "{env}", please check your configuration and try again."""
        print(f"Account ID ({env}): {account_id}")

        env = f"WORKSPACE_SETUP_{account_name}_PASSWORD"
        password = os.environ.get(env)
        assert password is not None, f"""Failed to load the environment variable "{env}", please check your configuration and try again."""
        print(f"Password   ({env}):   {password[0]}***{password[-1]}")

        env = f"WORKSPACE_SETUP_{account_name}_USERNAME"
        username = os.environ.get(env)
        assert username is not None, f"""Failed to load the environment variable "{env}", please check your configuration and try again."""
        print(f"Username   ({env}):   {username}")

        return AccountConfig(region=region,
                             account_id=account_id,
                             username=username,
                             password=password,
                             event_config=event_config,
                             uc_storage_config=uc_storage_config,
                             workspace_config_template=workspace_config_template,
                             ignored_workspaces=ignored_workspaces,
                             first_workspace_number=first_workspace_number)

    def __init__(self, *, region: str, account_id: str, username: str, password: str, event_config: EventConfig, uc_storage_config: UcStorageConfig, workspace_config_template: WorkspaceConfig, ignored_workspaces: List[Union[int, str]] = None, first_workspace_number: int) -> None:
        """
        Creates the configuration for account-level settings.
        """
        import math
        from dbacademy.workspaces_3_0 import ParameterValidator

        assert type(region) == str, f"""The parameter "region" must be a string value, found {type(region)}."""
        assert len(region) > 0, f"""The parameter "region" must be specified, found "{region}"."""

        assert type(account_id) == str, f"""The parameter "account_id" must be a string value, found {type(account_id)}."""
        assert len(account_id) > 0, f"""The parameter "account_id" must be specified, found "{account_id}"."""

        assert type(username) == str, f"""The parameter "username" must be a string value, found {type(username)}."""
        assert len(username) > 0, f"""The parameter "username" must be specified, found "{username}"."""

        ParameterValidator.validate_type(str, password=password)
        # assert type(password) == str, f"""The parameter "password" must be a string value, found {type(password)}."""
        assert len(password) > 0, f"""The parameter "password" must be specified, found "{password}"."""

        ignored_workspaces = ignored_workspaces or list()  # Default to an empty list if not provided.
        ParameterValidator.validate_type(list, ignored_workspaces=ignored_workspaces)

        ParameterValidator.validate_not_none(event_config=event_config)
        ParameterValidator.validate_not_none(storage_config=uc_storage_config)
        ParameterValidator.validate_not_none(workspace_config=workspace_config_template)

        self.__region = region

        self.__account_id = account_id
        self.__username = username
        self.__password = password

        self.__event_config = event_config
        self.__uc_storage_config = uc_storage_config
        self.__workspace_config_template = workspace_config_template
        self.__ignored_workspaces = ignored_workspaces

        self.__workspaces = list()

        actual_users = len(workspace_config_template.usernames)
        self.__max_users = actual_users - 2  # Accounts for user zero, max inclusive & class+analyst@databricks.com
        count = math.ceil(event_config.max_participants / actual_users)

        for workspace_number in range(first_workspace_number, first_workspace_number+count):
            self.create_workspace_config(event_config, workspace_config_template, workspace_number, self.max_users)

    def create_workspace_config(self, event_config: EventConfig, template: WorkspaceConfig, workspace_number: int, max_users: int) -> WorkspaceConfig:
        from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig

        workspace = WorkspaceConfig(max_users=max_users,
                                    course_definitions=template.course_definitions,
                                    cds_api_token=template.cds_api_token,
                                    datasets=template.datasets,
                                    default_node_type_id=template.default_node_type_id,
                                    default_dbr=template.default_dbr,
                                    credentials_name=template.credentials_name,
                                    storage_configuration=template.storage_configuration,
                                    username_pattern=template.username_pattern,
                                    entitlements=template.entitlements,
                                    groups=template.groups,
                                    workspace_name_pattern=template.workspace_name_pattern)

        workspace.init(event_config=event_config, workspace_number=workspace_number)

        if workspace.name in self.ignored_workspaces:
            print(f"Skipping workspace #{workspace.name}")
        elif workspace_number in self.ignored_workspaces:
            print(f"Skipping workspace #{workspace_number}")
        else:
            self.__workspaces.append(workspace)

        return workspace

    @property
    def max_users(self) -> int:
        return self.__max_users

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
    def event_config(self) -> EventConfig:
        return self.__event_config

    @property
    def uc_storage_config(self) -> UcStorageConfig:
        return self.__uc_storage_config

    @property
    def workspace_config_template(self) -> WorkspaceConfig:
        return self.__workspace_config_template
