from typing import List


class AccountConfig:
    from dbacademy.workspaces_3_0.event_config_class import EventConfig
    from dbacademy.workspaces_3_0.storage_config_class import StorageConfig
    from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig

    @staticmethod
    def from_env(region: str, event_config: EventConfig, storage_config: StorageConfig, workspace_config: WorkspaceConfig, ignored_workspaces: List[int] = None) -> "AccountConfig":
        import os

        env = "WORKSPACE_SETUP_ACCOUNT_ID"
        account_id = os.environ.get(env)
        assert account_id is not None, f"""Failed to load the environment variable "{env}", please check your configuration and try again."""

        env = "WORKSPACE_SETUP_PASSWORD"
        password = os.environ.get(env)
        assert password is not None, f"""Failed to load the environment variable "{env}", please check your configuration and try again."""

        env = "WORKSPACE_SETUP_USERNAME"
        username = os.environ.get(env)
        assert username is not None, f"""Failed to load the environment variable "{env}", please check your configuration and try again."""

        return AccountConfig(region=region,
                             account_id=account_id,
                             username=username,
                             password=password,
                             event_config=event_config,
                             storage_config=storage_config,
                             workspace_config=workspace_config,
                             ignored_workspaces=ignored_workspaces)

    def __init__(self, *, region: str, account_id: str, username: str, password: str, event_config: EventConfig, storage_config: StorageConfig, workspace_config: WorkspaceConfig, ignored_workspaces: List[int] = None) -> None:
        """
        Creates the configuration for account-level settings.
        """
        import math
        from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig
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
        # ParameterValidator.validate_min_length("ignored", 0, ignored_workspaces)

        ParameterValidator.validate_not_none(event_config=event_config)
        ParameterValidator.validate_not_none(storage_config=storage_config)
        ParameterValidator.validate_not_none(workspace_config=workspace_config)

        self.__region = region

        self.__account_id = account_id
        self.__username = username
        self.__password = password

        self.__event_config = event_config
        self.__storage_config = storage_config
        self.__workspace_config = workspace_config
        self.__ignored_workspaces = ignored_workspaces

        count = math.ceil(event_config.max_participants / workspace_config.max_user_count)
        self.__workspaces = list()
        for i in range(0, count):
            workspace = WorkspaceConfig(max_user_count=workspace_config.max_user_count,
                                        courses="example-course",
                                        datasets="example-course",
                                        default_node_type_id="i3.xlarge",
                                        default_dbr=workspace_config.default_dbr,
                                        dbc_urls=workspace_config.dbc_urls)

            workspace.init(event_config=event_config,
                           workspace_number=i+1)

            self.__workspaces.append(workspace)

    @property
    def ignored_workspaces(self) -> List[int]:
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
    def storage_config(self) -> StorageConfig:
        return self.__storage_config

    @property
    def workspace_config(self) -> WorkspaceConfig:
        return self.__workspace_config
