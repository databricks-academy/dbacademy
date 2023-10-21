from typing import List, Union, Dict, Optional

__all__ = ["WorkspaceConfig"]


class WorkspaceConfig:
    def __init__(self, *,
                 max_participants: int,
                 default_node_type_id: str,
                 default_dbr: str,
                 course_definitions: Union[None, str, List[str]],
                 datasets: Union[None, str, List[str]],
                 username_pattern: str,
                 entitlements: Optional[Dict[str, bool]],
                 workspace_name_pattern: str,
                 credentials_name: str,
                 storage_configuration: str,
                 workspace_number: int = None,
                 cds_api_token: str = None,
                 courseware_subdirectory: str = None,
                 workspace_group: Optional[Dict[str, List]]) -> None:
        """
        Creates the configuration for workspace-level settings
        :param max_participants: see the corresponding property
        :param default_dbr: see the corresponding property
        :param default_node_type_id: see the corresponding property
        :param course_definitions: see the corresponding property
        :param datasets: see the corresponding property
        """
        from dbacademy.dbhelper import WorkspaceHelper
        from dbacademy.common import validate

        self.__max_participants = validate.int_value(min_value=0, max_participants=max_participants)

        assert workspace_number is None or type(workspace_number) == int, f"""The parameter "workspace_number" must be None or an integral value, found {type(workspace_number)}."""
        assert workspace_number is None or workspace_number >= 0, f"""The parameter "workspace_number" must be None or greater than zero, found "{workspace_number}"."""

        assert type(default_node_type_id) == str, f"""The parameter "default_node_type_id" must be a string value, found {type(default_node_type_id)}."""
        assert len(default_node_type_id) > 3, f"""Invalid node type, found "{default_node_type_id}"."""

        assert type(default_dbr) == str, f"""The parameter "default_dbr" must be a string value, found {type(default_dbr)}."""
        assert len(default_dbr) > 3, f"""Invalid DBR format, found "{default_dbr}"."""

        assert cds_api_token is None or type(cds_api_token) == str, f"""The parameter "cds_api_token" must be None or a string value, found {type(default_dbr)}."""

        assert courseware_subdirectory is None or type(courseware_subdirectory) == str, f"""The parameter "courseware_subdirectory" must be None or a string value, found {type(courseware_subdirectory)}."""

        course_definitions = course_definitions or list()  # Convert none to empty list
        assert type(course_definitions) == list or type(course_definitions) == str, f"""The parameter "course_definitions" must be a string value or a list of strings, found {type(course_definitions)}."""
        course_definitions = [course_definitions] if type(course_definitions) == str else course_definitions  # Convert single string to list of strings
        assert type(course_definitions) == list, f"""The parameter "course_definitions" must be a string value, found {type(course_definitions)}."""

        datasets = datasets or list()  # Convert none to empty list
        datasets = [datasets] if type(datasets) == str else datasets  # Convert single string to list of strings
        assert type(datasets) == list, f"""The parameter "datasets" must be a string value, found {type(datasets)}."""

        # assert type(entitlements) == dict, f"""The parameter "entitlements" must be a dictionary value, found {type(entitlements)}."""
        # assert len(entitlements) > 3, f"""The parameter "entitlements" must have a length > 0, found "{username_pattern}"."""
        self.__entitlements = validate.dict_value(entitlements=entitlements, required=True)

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

        self.__course_definitions = course_definitions
        self.__courseware_subdirectory = courseware_subdirectory
        self.__cds_api_token = cds_api_token
        self.__default_node_type_id = default_node_type_id
        self.__default_dbr = default_dbr
        self.__username_pattern = username_pattern
        self.__workspace_name_pattern = workspace_name_pattern

        self.__cds_api_token = cds_api_token

        self.__dbc_urls = list()
        self.__datasets = datasets or list()

        for course_def in self.course_definitions:
            url, course, version, artifact, token = WorkspaceHelper.parse_course_args(course_def=course_def)
            if course not in self.__datasets:
                self.__datasets.append(course)
            if token is not None:
                raise AssertionError(f"""The CDS API token should not be specified in the courseware definition, please use the "cds_api_token" parameter" instead.""")

            dbc_url = WorkspaceHelper.compose_courseware_url(url, course, version, artifact, self.cds_api_token)
            self.__dbc_urls.append(dbc_url)

        for i, dbc_url in enumerate(self.dbc_urls):
            self.__validate_url(i, dbc_url)

        # Zero to max inclusive; the +1 accounts for user-zero as the instructor
        self.__usernames: List[str] = list()
        for i in range(0, self.max_participants+1):
            value = f"{i:03d}"
            self.__usernames.append(self.__username_pattern.format(student_number=value))

        # Create the group analyst and instructors
        self.__workspace_group = dict()
        workspace_group = validate.dict_value(workspace_group=workspace_group, required=True)

        # Start by initializing groups as an empty list
        for group_name in workspace_group:
            self.__workspace_group[group_name] = []

        for group_name, usernames in workspace_group.items():
            for username in usernames:
                if type(username) == int:
                    # We are identifying the user by the Nth user in usernames
                    username = self.__usernames[username]

                elif username not in self.__usernames:
                    # Specified a user that doesn't exist in the default set of users
                    self.__usernames.append(username)

                # Add each user to their group
                self.__workspace_group.get(group_name).append(username)

        if workspace_number is None:
            # This is a template instance
            self.__name = None
            self.__workspace_number = None
        else:
            # This is not our template, need to configure the workspace
            self.__workspace_number = workspace_number

            workspace_number_str = f"{self.workspace_number:03d}"
            name = self.workspace_name_pattern.format(workspace_number=workspace_number_str)

            # event_id = 0  # This use to be pre-defined, but was always zero. Hard coding zero for backwards compatibility for workspaces < 375
            hashcode = self.hashcode(workspace_number)  # stable_hash("Databricks Lakehouse", event_id, workspace_number, length=5)
            self.__name = f"{name}-{hashcode}".lower()

    @staticmethod
    def hashcode(workspace_number: int) -> str:
        from dbacademy.dbgems import stable_hash

        event_id = 0  # This use to be pre-defined, but was always zero. Hard coding zero for backwards compatibility for workspaces < 375
        return stable_hash("Databricks Lakehouse", event_id, workspace_number, length=5)

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
    def dbc_urls(self) -> List[str]:
        return self.__dbc_urls

    @property
    def max_participants(self) -> int:
        return self.__max_participants

    @property
    def usernames(self) -> List[str]:
        return self.__usernames

    @property
    def workspace_group(self) -> Dict[str, List[str]]:
        return self.__workspace_group

    @property
    def cds_api_token(self):
        return self.__cds_api_token

    @property
    def course_definitions(self):
        return self.__course_definitions

    @property
    def courseware_subdirectory(self):
        return self.__courseware_subdirectory

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
    def storage_configuration(self) -> str:
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
