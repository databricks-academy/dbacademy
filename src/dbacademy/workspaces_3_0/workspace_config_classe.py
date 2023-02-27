from typing import List, Union

__all__ = ["WorkspaceConfig"]


class WorkspaceConfig:
    from dbacademy.workspaces_3_0.event_config_class import EventConfig

    def __init__(self, *, max_user_count: int, default_node_type_id: str, default_dbr: str, dbc_urls: Union[None, str, List[str]], courses: Union[None, str, List[str]], datasets: Union[None, str, List[str]], username_pattern: str = "class+{student_number:03d}@databricks.com") -> None:
        """
        Creates the configuration for workspace-level settings
        :param max_user_count: see the corresponding property
        :param default_dbr: see the corresponding property
        :param default_node_type_id: see the corresponding property
        :param courses: see the corresponding property
        :param datasets: see the corresponding property
        :param dbc_urls: see the corresponding property
        """

        assert type(max_user_count) == int, f"""The parameter "max_user_count" must be an integral value, found {type(max_user_count)}."""
        assert max_user_count > 0, f"""The parameter "max_user_count" must be greater than zero, found "{max_user_count}"."""

        assert type(default_node_type_id) == str, f"""The parameter "default_node_type_id" must be a string value, found {type(default_node_type_id)}."""
        assert len(default_node_type_id) > 3, f"""Invalid node type, found "{default_node_type_id}"."""

        assert type(default_dbr) == str, f"""The parameter "default_dbr" must be a string value, found {type(default_dbr)}."""
        assert len(default_dbr) > 3, f"""Invalid DBR format, found "{default_dbr}"."""

        courses = courses or list()  # Convert none to empty list
        courses = [courses] if type(courses) == str else courses  # Convert single string to list of strings
        assert type(courses) == list, f"""The parameter "courses" must be a string value, found {type(courses)}."""

        datasets = datasets or list()  # Convert none to empty list
        datasets = [datasets] if type(datasets) == str else datasets  # Convert single string to list of strings
        assert type(datasets) == list, f"""The parameter "datasets" must be a string value, found {type(datasets)}."""

        dbc_urls = dbc_urls or list()  # Convert none to empty list
        dbc_urls = [dbc_urls] if type(dbc_urls) == str else dbc_urls  # Convert single string to list of strings
        assert type(dbc_urls) == list, f"""The parameter "dbc_urls" must be a list of strings, found {type(dbc_urls)}."""

        assert type(username_pattern) == str, f"""The parameter "username_pattern" must be a string value, found {type(username_pattern)}."""
        assert len(username_pattern) > 3, f"""The parameter "username_pattern" must have a length > 0, found "{username_pattern}"."""

        self.__event_config = None
        self.__workspace_number = None
        self.__name = None

        for i, dbc_url in enumerate(dbc_urls):
            self.__validate_url(i, dbc_url)

        self.__courses = courses
        self.__datasets = datasets
        self.__default_node_type_id = default_node_type_id
        self.__default_dbr = default_dbr
        self.__max_user_count = max_user_count
        self.__dbc_urls = dbc_urls
        self.__username_pattern = username_pattern

        self.__users: List[str] = list()
        for i in range(0, max_user_count):
            self.__users.append(f"class+{i:03d}@databricks.com")

    def init(self, *, event_config: EventConfig, workspace_number: int):

        assert type(workspace_number) == int, f"""The parameter "workspace_number" must be an integral value, found {type(workspace_number)}."""
        assert workspace_number > 0, f"""The parameter "workspace_number" must be greater than zero, found "{workspace_number}"."""

        self.__event_config = event_config
        self.__workspace_number = workspace_number
        self.__name = f"classroom-{event_config.event_id}-{workspace_number:03d}"

    @property
    def name(self) -> str:
        return self.__name

    @property
    def username_pattern(self) -> str:
        return self.__username_pattern

    @property
    def workspace_number(self) -> int:
        return self.__workspace_number

    @property
    def event_config(self) -> EventConfig:
        return self.__event_config

    @classmethod
    def __validate_url(cls, i: int, dbc_url: str) -> None:
        assert type(dbc_url) == str, f"""Item {i} of the parameter "dbc_urls" must be a strings, found {type(dbc_url)}."""
        prefix = "https://labs.training.databricks.com/api/courses?"
        assert dbc_url.startswith(prefix), f"""Item {i} for the parameter "dbc_urls" must start with "{prefix}", found "{dbc_url}"."""

        pos = dbc_url.find("?")
        assert pos >= 0, f"""Item {i} for the parameter "dbc_urls" is missing its query parameters: course, version, artifact, token."""
        query = dbc_url[pos+1:]
        params = query.split("&")

        found_course = False
        found_version = False
        found_artifact = False
        found_token = False

        for param in params:
            if param.startswith("course="):
                found_course = True
            if param.startswith("version="):
                found_version = True
            if param.startswith("artifact="):
                found_artifact = True
            if param.startswith("token="):
                found_token = True

        assert found_course, f"""Item {i} for the parameter "dbc_url" is missing the "course" query parameter, found "{dbc_url}"."""
        assert found_version, f"""Item {i} for the parameter "dbc_url" is missing the "version" query parameter, found "{dbc_url}"."""
        assert found_artifact, f"""Item {i} for the parameter "dbc_url" is missing the "artifact" query parameter, found "{dbc_url}"."""
        assert found_token, f"""Item {i} for the parameter "dbc_url" is missing the "token" query parameter, found "{dbc_url}"."""

    @property
    def dbc_urls(self) -> List[str]:
        return self.__dbc_urls

    @property
    def users(self) -> List[str]:
        return self.__users

    @property
    def max_user_count(self) -> int:
        return self.__max_user_count

    @property
    def courses(self):
        return self.__courses

    @property
    def datasets(self):
        return self.__datasets

    @property
    def default_node_type_id(self):
        return self.__default_node_type_id

    @property
    def default_dbr(self) -> str:
        return self.__default_dbr
