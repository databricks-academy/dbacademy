from typing import List, Union, Dict

__all__ = ["WorkspaceConfig"]


class WorkspaceConfig:
    from dbacademy.workspaces_3_0.event_config_class import EventConfig

    def __init__(self, *,
                 max_users: int,
                 default_node_type_id: str,
                 default_dbr: str,
                 course_definitions: Union[None, str, List[str]],
                 datasets: Union[None, str, List[str]],
                 username_pattern: str,
                 entitlements: Dict[str, bool],
                 workspace_name_pattern: str,
                 credentials_name: str,
                 storage_configuration: str,
                 cds_api_token: str = None,
                 groups: Dict[str, List]) -> None:
        """
        Creates the configuration for workspace-level settings
        :param max_users: see the corresponding property
        :param default_dbr: see the corresponding property
        :param default_node_type_id: see the corresponding property
        :param course_definitions: see the corresponding property
        :param datasets: see the corresponding property
        """
        from dbacademy.dbhelper import WorkspaceHelper

        assert type(max_users) == int, f"""The parameter "max_users" must be an integral value, found {type(max_users)}."""
        assert max_users > 0, f"""The parameter "max_users" must be greater than zero, found "{max_users}"."""

        assert type(default_node_type_id) == str, f"""The parameter "default_node_type_id" must be a string value, found {type(default_node_type_id)}."""
        assert len(default_node_type_id) > 3, f"""Invalid node type, found "{default_node_type_id}"."""

        assert type(default_dbr) == str, f"""The parameter "default_dbr" must be a string value, found {type(default_dbr)}."""
        assert len(default_dbr) > 3, f"""Invalid DBR format, found "{default_dbr}"."""

        assert cds_api_token is None or type(cds_api_token) == str, f"""The parameter "cds_api_token" must be None or a string value, found {type(default_dbr)}."""
        # assert len(cds_api_token) > 3, f"""Invalid DBR format, found "{default_dbr}"."""

        course_definitions = course_definitions or list()  # Convert none to empty list
        assert type(course_definitions) == list or type(course_definitions) == str, f"""The parameter "course_definitions" must be a string value or a list of strings, found {type(course_definitions)}."""
        course_definitions = [course_definitions] if type(course_definitions) == str else course_definitions  # Convert single string to list of strings
        assert type(course_definitions) == list, f"""The parameter "course_definitions" must be a string value, found {type(course_definitions)}."""

        datasets = datasets or list()  # Convert none to empty list
        datasets = [datasets] if type(datasets) == str else datasets  # Convert single string to list of strings
        assert type(datasets) == list, f"""The parameter "datasets" must be a string value, found {type(datasets)}."""

        assert type(entitlements) == dict, f"""The parameter "entitlements" must be a dictionary value, found {type(entitlements)}."""
        # assert len(entitlements) > 3, f"""The parameter "entitlements" must have a length > 0, found "{username_pattern}"."""

        assert type(username_pattern) == str, f"""The parameter "username_pattern" must be a string value, found {type(username_pattern)}."""
        assert len(username_pattern) > 3, f"""The parameter "username_pattern" must have a length > 0, found "{username_pattern}"."""

        assert type(workspace_name_pattern) == str, f"""The parameter "workspace_name_pattern" must be a string value, found {type(workspace_name_pattern)}."""
        assert len(workspace_name_pattern) > 3, f"""The parameter "workspace_name_pattern" must have a length > 0, found "{workspace_name_pattern}"."""

        assert type(credentials_name) == str, f"""The parameter "credentials_name" must be a string value, found {type(credentials_name)}."""
        assert len(credentials_name) > 0, f"""The parameter "credentials_name" must be specified, found "{credentials_name}"."""

        assert type(storage_configuration) == str, f"""The parameter "storage_configuration" must be a string value, found {type(storage_configuration)}."""
        assert len(storage_configuration) > 0, f"""The parameter "storage_configuration" must be specified, found "{storage_configuration}"."""

        assert "{student_number}" in username_pattern, f"""Expected the parameter "username_pattern" to contain "{{student_number}}", found "{username_pattern}"."""
        assert "{workspace_number}" in workspace_name_pattern, f"""Expected the parameter "workspace_name_pattern" to contain "{{workspace_number}}", found "{workspace_name_pattern}"."""

        self.__credentials_name = credentials_name
        self.__storage_configuration = storage_configuration

        self.__event_config = None
        self.__workspace_number = None
        self.__name = None

        self.__course_definitions = course_definitions
        self.__cds_api_token = cds_api_token
        self.__datasets = datasets
        self.__default_node_type_id = default_node_type_id
        self.__default_dbr = default_dbr
        self.__username_pattern = username_pattern
        self.__workspace_name_pattern = workspace_name_pattern
        self.__entitlements = entitlements

        self.__cds_api_token = cds_api_token

        self.__dbc_urls = list()
        for course_def in self.course_definitions:
            url, course, version, artifact, token = WorkspaceHelper.parse_course_args(course_def=course_def)
            if token is not None:
                raise AssertionError(f"""The CDS API token should not be specified in the courseware definition, please use the "cds_api_token" parameter" instead.""")

            dbc_url = WorkspaceHelper.compose_courseware_url(url, course, version, artifact, self.cds_api_token)
            self.__dbc_urls.append(dbc_url)

        for i, dbc_url in enumerate(self.dbc_urls):
            self.__validate_url(i, dbc_url)

        self.__usernames: List[str] = list()
        for i in range(0, max_users+1):
            # Zero to max inclusive; the +1 accounts for user-zero as the instructor
            value = f"{i:03d}"
            self.__usernames.append(self.__username_pattern.format(student_number=value))

        # Create the group analyst and instructors
        self.__groups = dict()

        # Start by initializing groups as an empty list
        for group_name in groups:
            self.__groups[group_name] = []

        for group_name, usernames in groups.items():
            for username in usernames:
                if type(username) == int:
                    # We are identifying the user by the Nth user in usernames
                    username = self.__usernames[username]

                elif username not in self.__usernames:
                    # Specified a user that doesn't exist in the default set of users
                    self.__usernames.append(username)

                # Add each user to their group
                self.__groups.get(group_name).append(username)

    def init(self, *, event_config: EventConfig, workspace_number: int):
        from dbacademy.dbgems import stable_hash

        assert type(workspace_number) == int, f"""The parameter "workspace_number" must be an integral value, found {type(workspace_number)}."""
        assert workspace_number > 0, f"""The parameter "workspace_number" must be greater than zero, found "{workspace_number}"."""

        self.__event_config = event_config
        self.__workspace_number = workspace_number

        if "event_id" in self.workspace_name_pattern and "workspace_number" in self.workspace_name_pattern:
            event_id_str = f"{event_config.event_id:03d}"
            workspace_number_str = f"{workspace_number:03d}"
            name = self.workspace_name_pattern.format(event_id=event_id_str, workspace_number=workspace_number_str)
        elif "workspace_number" in self.workspace_name_pattern:
            workspace_number_str = f"{self.workspace_number:03d}"
            name = self.workspace_name_pattern.format(workspace_number=workspace_number_str)
        else:
            raise Exception(f"Invalid workspace_name_pattern, found {self.workspace_name_pattern}")

        hashcode = stable_hash("Databricks Lakehouse", event_config.event_id, workspace_number, length=5)
        self.__name = f"{name}-{hashcode}".lower()

    @property
    def name(self) -> str:
        return self.__name

    @property
    def entitlements(self) -> Dict[str, bool]:
        return self.__entitlements

    @property
    def username_pattern(self) -> str:
        return self.__username_pattern

    @property
    def workspace_name_pattern(self) -> str:
        return self.__workspace_name_pattern

    @property
    def workspace_number(self) -> int:
        return self.__workspace_number

    @property
    def event_config(self) -> EventConfig:
        return self.__event_config

    @property
    def dbc_urls(self) -> List[str]:
        return self.__dbc_urls

    @property
    def usernames(self) -> List[str]:
        return self.__usernames

    @property
    def groups(self) -> Dict[str, List[str]]:
        return self.__groups

    @property
    def cds_api_token(self):
        return self.__cds_api_token

    @property
    def course_definitions(self):
        return self.__course_definitions

    @property
    def datasets(self):
        return self.__datasets

    @property
    def default_node_type_id(self):
        return self.__default_node_type_id

    @property
    def default_dbr(self) -> str:
        return self.__default_dbr

    @property
    def credentials_name(self):
        """
        This is the name of the credentials for a workspaces' storage configuration (e.g. DBFS).
        :return: the credential's name
        """
        return self.__credentials_name

    @property
    def storage_configuration(self):
        """
        This is the name of the storage configuration for a workspace (e.g. DBFS)
        :return:
        """
        return self.__storage_configuration

    @classmethod
    def __validate_url(cls, i: int, dbc_url: str) -> None:
        assert type(dbc_url) == str, f"""Item {i} of the parameter "dbc_urls" must be a strings, found {type(dbc_url)}."""
        prefix = "https://labs.training.databricks.com/api/v1/courses/download.dbc?"
        assert dbc_url.startswith(prefix), f"""Item {i} for the parameter "dbc_urls" must start with "{prefix}", found "{dbc_url}"."""

        pos = dbc_url.find("?")
        assert pos >= 0, f"""Item {i} for the parameter "dbc_urls" is missing its query parameters: course, version, artifact, token."""
        query = dbc_url[pos+1:]
        params = query.split("&")

        found_course = False
        # found_version = False
        # found_artifact = False
        found_token = False

        for param in params:
            if param.startswith("course="):
                found_course = True
            # if param.startswith("version="):
            #     found_version = True
            # if param.startswith("artifact="):
            #     found_artifact = True
            if param.startswith("token="):
                found_token = True

        assert found_course, f"""Item {i} for the parameter "dbc_url" is missing the "course" query parameter, found "{dbc_url}"."""
        # assert found_version, f"""Item {i} for the parameter "dbc_url" is missing the "version" query parameter, found "{dbc_url}"."""
        # assert found_artifact, f"""Item {i} for the parameter "dbc_url" is missing the "artifact" query parameter, found "{dbc_url}"."""
        assert found_token, f"""Item {i} for the parameter "dbc_url" is missing the "token" query parameter, found "{dbc_url}"."""
