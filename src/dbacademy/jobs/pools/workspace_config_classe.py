from typing import List, Dict, Optional

__all__ = ["WorkspaceConfig"]


class WorkspaceConfig:
    def __init__(self, *,
                 max_participants: int,
                 default_node_type_id: str,
                 username_pattern: str,
                 entitlements: Optional[Dict[str, bool]],
                 workspace_name_pattern: str,
                 credentials_name: str,
                 storage_configuration: str,
                 workspace_number: int = None,
                 workspace_group: Optional[Dict[str, List]]) -> None:

        from dbacademy.common import validate

        self.__max_participants = validate(max_participants=max_participants).optional.int(min_value=0)

        workspace_number = validate(workspace_number=workspace_number).optional.int(min_value=1000)
        default_node_type_id = validate(default_node_type_id=default_node_type_id).required.str(min_length=5)

        # TODO, Remove this feature as it should be handled by the Universal-Workspace-Setup
        self.__entitlements: Dict[str, bool] = validate(entitlements=entitlements).required.dict(str)

        username_pattern = validate(username_pattern=username_pattern).required.str(min_length=5)
        assert "{student_number}" in username_pattern, f"""Expected the parameter "username_pattern" to contain "{{student_number}}", found "{username_pattern}"."""

        workspace_name_pattern = validate(workspace_name_pattern=workspace_name_pattern).required.str(min_length=5)
        assert "{workspace_number}" in workspace_name_pattern, f"""Expected the parameter "workspace_name_pattern" to contain "{{workspace_number}}", found "{workspace_name_pattern}"."""

        self.__credentials_name = validate(credentials_name=credentials_name).required.str(min_length=5)
        self.__storage_configuration = validate(storage_configuration=storage_configuration).required.str(min_length=5)

        self.__default_node_type_id = default_node_type_id
        self.__username_pattern = username_pattern
        self.__workspace_name_pattern = workspace_name_pattern

        self.__dbc_urls = list()

        # Zero to max inclusive; the +1 accounts for user-zero as the instructor
        self.__usernames: List[str] = list()
        for i in range(0, self.max_participants+1):
            value = f"{i:03d}"
            self.__usernames.append(self.__username_pattern.format(student_number=value))

        # Create the group analyst and instructors
        self.__workspace_group = dict()
        workspace_group: Dict[str, List] = validate(workspace_group=workspace_group).optional.dict(str, auto_create=True)

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
    def default_node_type_id(self):
        return self.__default_node_type_id

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
