from dbacademy_gems import dbgems
from dbacademy_helper import DBAcademyHelper
from typing import Callable, List, TypeVar
T = TypeVar("T")

ALL_USERS = "All Users"
MISSING_USERS_ONLY = "Missing Users Only"
CURRENT_USER_ONLY = "Current User Only"


class WorkspaceHelper:

    def __init__(self, da: DBAcademyHelper):
        from dbacademy_helper.warehouses_helper import WarehousesHelper
        from dbacademy_helper.databases_helper import DatabasesHelper
        from dbacademy_helper.clusters_helper import ClustersHelper

        self.da = da
        self.client = da.client
        self.warehouses = WarehousesHelper(self, da)
        self.databases = DatabasesHelper(self, da)
        self.clusters = ClustersHelper(self, da)

        self._usernames = None
        self._existing_databases = None

        self.configure_for_options = ["", ALL_USERS, MISSING_USERS_ONLY, CURRENT_USER_ONLY]
        self.valid_configure_for_options = self.configure_for_options[1:]  # all but empty-string

    def add_entitlement_allow_instance_pool_create(self):
        group = self.client.scim.groups.get_by_name("users")
        self.client.scim.groups.add_entitlement(group.get("id"), "allow-instance-pool-create")

    def add_entitlement_workspace_access(self):
        group = self.client.scim.groups.get_by_name("users")
        self.client.scim.groups.add_entitlement(group.get("id"), "workspace-access")

    def add_entitlement_allow_cluster_create(self):
        group = self.client.scim.groups.get_by_name("users")
        self.client.scim.groups.add_entitlement(group.get("id"), "allow-cluster-create")

    def add_entitlement_databricks_sql_access(self):
        group = self.client.scim.groups.get_by_name("users")
        self.client.scim.groups.add_entitlement(group.get("id"), "databricks-sql-access")

    def do_for_all_users(self, f: Callable[[str], T]) -> List[T]:
        from multiprocessing.pool import ThreadPool

        # if self.usernames is None:
        #     raise ValueError("DBAcademyHelper.workspace.usernames must be defined before calling DBAcademyHelper.workspace.do_for_all_users(). See also DBAcademyHelper.workspace.load_all_usernames()")
        if len(self.usernames) == 0:
            return []

        with ThreadPool(len(self.usernames)) as pool:
            return pool.map(f, self.usernames)

    @property
    def org_id(self):
        try:
            return dbgems.get_tag("orgId", "unknown")
        except:
            # dbgems.get_tags() can throw exceptions in some secure contexts
            return "unknown"

    @property
    def workspace_name(self):
        try:
            workspace_name = dbgems.get_browser_host_name()
            return dbgems.get_notebooks_api_endpoint() if workspace_name is None else workspace_name
        except:
            # dbgems.get_tags() can throw exceptions in some secure contexts
            return dbgems.get_notebooks_api_endpoint()

    @property
    def configure_for(self):
        # Under test, we are always configured for the current user only
        configure_for = CURRENT_USER_ONLY if self.da.is_smoke_test() else dbgems.get_parameter("configure_for")
        assert configure_for in self.valid_configure_for_options, f"Who the workspace is being configured for must be specified, found \"{configure_for}\". Options include {self.valid_configure_for_options}"
        return configure_for

    @property
    def usernames(self):
        if self._usernames is None:
            users = self.client.scim().users().list()
            self._usernames = [r.get("userName") for r in users]
            self._usernames.sort()

        if self.configure_for == CURRENT_USER_ONLY:
            # Override for the current user only
            return [self.da.username]

        elif self.configure_for == MISSING_USERS_ONLY:
            # The presumption here is that if the user doesn't have their own
            # database, then they are also missing the rest of their config.
            missing_users = []
            for username in self._usernames:
                schema_name = self.da.to_schema_name(username)
                if schema_name not in self.existing_databases:
                    missing_users.append(username)

            missing_users.sort()
            return missing_users

        return self._usernames

    @property
    def existing_databases(self):
        if self._existing_databases is None:
            existing = dbgems.spark.sql("SHOW DATABASES").collect()
            self._existing_databases = {d[0] for d in existing}
        return self._existing_databases

    @property
    def event_name(self):
        import re

        event_name = "Smoke Test" if self.da.is_smoke_test() else dbgems.get_parameter("event_name")
        assert event_name is not None and len(event_name) >= 3, f"The parameter event_name must be specified with min-length of 3, found \"{event_name}\"."

        event_name = re.sub(r"[^a-zA-Z\d]", "_", event_name)
        while "__" in event_name: event_name = event_name.replace("__", "_")

        return event_name

    @property
    def student_count(self):
        students_count = dbgems.get_parameter("students_count", "0").strip()
        students_count = int(students_count) if students_count.isnumeric() else 0
        students_count = max(students_count, len(self.usernames))
        return students_count
