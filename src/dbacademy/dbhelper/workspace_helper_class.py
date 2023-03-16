from typing import Callable, List, TypeVar


class WorkspaceHelper:
    from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper
    from dbacademy.dbrest import DBAcademyRestClient

    T = TypeVar("T")

    WORKSPACE_SETUP_JOB_NAME = "DBAcademy Workspace-Setup"
    BOOTSTRAP_JOB_NAME = "DBAcademy Workspace-Setup (Bootstrap)"

    PARAM_LAB_ID = "lab_id"
    PARAM_DESCRIPTION = "description"
    PARAM_CONFIGURE_FOR = "configure_for"
    PARAM_NODE_TYPE_ID = "node_type_id"
    PARAM_SPARK_VERSION = "spark_version"
    PARAM_DATASETS = "datasets"
    PARAM_COURSES = "courses"
    PARAM_SOURCE = "source"
    PARAM_ORG_ID = "org_id"
    PARAM_WORKSPACE_NAME = "workspace_name"

    CONFIGURE_FOR_ALL_USERS = "All Users"
    CONFIGURE_FOR_MISSING_USERS_ONLY = "Missing Users Only"
    CONFIGURE_FOR_CURRENT_USER_ONLY = "Current User Only"

    CONFIGURE_FOR_OPTIONS = ["", CONFIGURE_FOR_ALL_USERS, CONFIGURE_FOR_MISSING_USERS_ONLY, CONFIGURE_FOR_CURRENT_USER_ONLY]
    CONFIGURE_FOR_VALID_OPTIONS = CONFIGURE_FOR_OPTIONS[1:]  # all but empty-string

    def __init__(self, da: DBAcademyHelper):
        from dbacademy.dbhelper.warehouses_helper_class import WarehousesHelper
        from dbacademy.dbhelper.databases_helper_class import DatabasesHelper
        from dbacademy.dbhelper.clusters_helper_class import ClustersHelper

        self.da = da
        self.client = da.client
        self.warehouses = WarehousesHelper(self, da)
        self.databases = DatabasesHelper(self, da)
        self.clusters = ClustersHelper(self, da)

        self._usernames = None
        self.__existing_databases = None
        self.__existing_catalogs = None

    @staticmethod
    def get_spark_version():
        from dbacademy import dbgems

        return dbgems.get_parameter(WorkspaceHelper.PARAM_SPARK_VERSION)

    @staticmethod
    def get_lab_id():
        from dbacademy import dbgems

        return dbgems.get_parameter(WorkspaceHelper.PARAM_LAB_ID)

    @staticmethod
    def get_workspace_name():
        from dbacademy import dbgems

        return dbgems.get_spark_config("spark.databricks.workspaceUrl", default=dbgems.get_notebooks_api_endpoint())

    @staticmethod
    def get_workspace_description():
        from dbacademy import dbgems

        return dbgems.get_parameter(WorkspaceHelper.PARAM_DESCRIPTION)

    @staticmethod
    def install_datasets(installed_datasets: str):
        from dbacademy.dbgems import dbutils
        from dbacademy.dbhelper.dataset_manager_class import DatasetManager

        if installed_datasets is not None and installed_datasets.strip() not in ("", "null", "None"):
            datasets = installed_datasets.split(",")
            print(f"Installing " + ", ".join(datasets) + " datasets...")
        else:
            print(f"Installing all datasets...")
            datasets = [
                "example-course",
                "apache-spark-programming-with-databricks",
                "data-analysis-with-databricks",
                "data-engineer-learning-path",
                "data-engineering-with-databricks",
                "deep-learning-with-databricks",
                "introduction-to-python-for-data-science-and-data-engineering",
                "ml-in-production",
                "scalable-machine-learning-with-apache-spark",
            ]

        for dataset in datasets:
            if ":" in dataset:
                dataset, data_source_version = dataset.split(":")
            else:
                data_source_version = None

            if not data_source_version:
                datasets_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{dataset}"
                data_source_version = sorted([f.name[:-1] for f in dbutils.fs.ls(datasets_uri)])[-1]

            datasets_path = f"dbfs:/mnt/dbacademy-datasets/{dataset}/{data_source_version}"
            data_source_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{dataset}/{data_source_version}"

            print(f"| {data_source_uri}")
            print(f"| {datasets_path}")

            remote_files = DatasetManager.list_r(data_source_uri)

            dataset_manager = DatasetManager(data_source_uri=data_source_uri,
                                             staging_source_uri=None,
                                             datasets_path=datasets_path,
                                             remote_files=remote_files)

            dataset_manager.install_dataset(install_min_time=None,
                                            install_max_time=None,
                                            reinstall_datasets=False)

            print("\n" + ("-" * 100) + "\n")

    @staticmethod
    def uninstall_courseware(client: DBAcademyRestClient, courses_arg: str, subdirectory: str, usernames: List[str] = None) -> None:

        course_defs = [c.strip() for c in courses_arg.split(",")]
        usernames = usernames or [u.get("userName") for u in client.scim.users.list()]

        for username in usernames:
            print(f"Uninstalling courses for {username}")

            for course_def in course_defs:
                url, course, version, artifact, token = WorkspaceHelper.__parse_course_args(course_def)

                if subdirectory is None:
                    install_dir = f"/Users/{username}/{course}"
                else:
                    install_dir = f"/Users/{username}/{subdirectory}/{course}"

                print(install_dir)
                client.workspace.delete_path(install_dir)

            print("-" * 80)

    @staticmethod
    def install_courseware(client: DBAcademyRestClient, courses_arg: str, subdirectory: str, usernames: List[str] = None) -> None:
        if courses_arg is None or courses_arg.strip() in ("", "null", "None"):
            print("No courses specified for installation.")
            return

        course_defs = [c.strip() for c in courses_arg.split(",")]
        usernames = usernames or [u.get("userName") for u in client.scim.users.list()]

        for username in usernames:

            print(f"Installing courses for {username}")

            for course_def in course_defs:
                print()

                url, course, version, artifact, token = WorkspaceHelper.__parse_course_args(course_def)

                download_url = f"{url}?course={course}"

                if version is not None:
                    download_url += f"&version={version}"

                if artifact is not None:
                    download_url += f"&artifact={artifact}"

                print(download_url)
                download_url += f"&token={token}"

                if subdirectory is None:
                    install_dir = f"/Users/{username}/{course}"
                else:
                    install_dir = f"/Users/{username}/{subdirectory}/{course}"

                print(f" - {install_dir}")

                files = client.workspace.ls(install_dir)
                count = 0 if files is None else len(files)
                if count > 0:
                    print(f" - Skipping, course already exists.")
                else:
                    client.workspace.import_dbc_files(install_dir, download_url)
                    print(f" - Installed.")

            print("-" * 80)

    @staticmethod
    def __parse_course_args(course_def: str) -> (str, str, str, str, str):
        from dbacademy.dbgems import dbutils

        token = dbutils.secrets.get("workspace-setup", "token")

        parts = course_def.split("?")
        if len(parts) == 1:
            url = f"https://labs.training.databricks.com/api/v1/courses/download.dbc"
            query = parts[0]
        elif len(parts) == 2:
            url = parts[0]
            query = parts[1]
        else:
            raise Exception(f"""Invalid course definition, found "{course_def}".""")

        params = {p.split("=")[0]: p.split("=")[1] for p in query.split("&")}
        course = params.get("course")
        version = params.get("version")
        artifact = params.get("artifact")
        token = token or params.get("token")

        return url, course, version, artifact, token

    @staticmethod
    def add_entitlement_allow_instance_pool_create(client: DBAcademyRestClient):
        group = client.scim.groups.get_by_name("users")
        client.scim.groups.add_entitlement(group.get("id"), "allow-instance-pool-create")

    @staticmethod
    def add_entitlement_workspace_access(client: DBAcademyRestClient):
        group = client.scim.groups.get_by_name("users")
        client.scim.groups.add_entitlement(group.get("id"), "workspace-access")

    @staticmethod
    def add_entitlement_allow_cluster_create(client: DBAcademyRestClient):
        group = client.scim.groups.get_by_name("users")
        client.scim.groups.add_entitlement(group.get("id"), "allow-cluster-create")

    @staticmethod
    def add_entitlement_databricks_sql_access(client: DBAcademyRestClient):
        group = client.scim.groups.get_by_name("users")
        client.scim.groups.add_entitlement(group.get("id"), "databricks-sql-access")

    @staticmethod
    def do_for_all_users(usernames: List[str], f: Callable[[str], T]) -> List[T]:
        from multiprocessing.pool import ThreadPool

        # if self.usernames is None:
        #     raise ValueError("DBAcademyHelper.workspace.usernames must be defined before calling DBAcademyHelper.workspace.do_for_all_users(). See also DBAcademyHelper.workspace.load_all_usernames()")
        if len(usernames) == 0:
            return []

        with ThreadPool(len(usernames)) as pool:
            return pool.map(f, usernames)

    @property
    def org_id(self):
        from dbacademy import dbgems

        try:
            return dbgems.get_tag("orgId", "unknown")
        except:
            # dbgems.get_tags() can throw exceptions in some secure contexts
            return "unknown"

    @property
    def workspace_name(self):
        from dbacademy import dbgems

        try:
            workspace_name = dbgems.get_browser_host_name()
            return dbgems.get_notebooks_api_endpoint() if workspace_name is None else workspace_name
        except:
            # dbgems.get_tags() can throw exceptions in some secure contexts
            return dbgems.get_notebooks_api_endpoint()

    def get_usernames(self, configure_for: str):
        assert configure_for in WorkspaceHelper.CONFIGURE_FOR_VALID_OPTIONS, f"Who the workspace is being configured for must be specified, found \"{configure_for}\". Options include {WorkspaceHelper.CONFIGURE_FOR_VALID_OPTIONS}"

        if self._usernames is None:
            users = self.client.scim().users().list()
            self._usernames = [r.get("userName") for r in users]
            self._usernames.sort()

        if configure_for == WorkspaceHelper.CONFIGURE_FOR_CURRENT_USER_ONLY:
            # Override for the current user only
            return [self.da.username]

        elif configure_for == WorkspaceHelper.CONFIGURE_FOR_MISSING_USERS_ONLY:
            # TODO - This isn't going to hold up long-term, maybe track per-user properties in this respect.
            # The presumption here is that if the user doesn't have their own
            # database, then they are also missing the rest of their config.
            missing_users = set()

            if self.da.lesson_config.requires_uc:
                for username in self._usernames:
                    prefix = self.da.to_catalog_name_prefix(username=username)
                    for catalog_name in self.existing_catalogs:
                        if catalog_name.startswith(prefix):
                            missing_users.add(username)
            else:
                for username in self._usernames:
                    prefix = self.da.to_schema_name_prefix(username=username, course_code=self.da.course_config.course_code)
                    for schema_name in self.existing_databases:
                        if schema_name.startswith(prefix):
                            missing_users.add(username)

            missing_users = list(missing_users)
            missing_users.sort()
            return missing_users

        return self._usernames

    @property
    def existing_databases(self):
        from dbacademy import dbgems

        if self.__existing_databases is None:
            existing = dbgems.spark.sql("SHOW DATABASES").collect()
            self.__existing_databases = {d[0] for d in existing}

        return self.__existing_databases

    def clear_existing_databases(self):
        self.__existing_databases = None

    @property
    def existing_catalogs(self):
        from dbacademy import dbgems

        if self.__existing_catalogs is None:
            existing = dbgems.spark.sql("SHOW CATALOGS").collect()
            self.__existing_catalogs = {d[0] for d in existing}

        return self.__existing_catalogs

    def clear_existing_catalogs(self):
        self.__existing_catalogs = None

    @property
    def lab_id(self):
        from dbacademy import dbgems
        from dbacademy import common
        from dbacademy.dbhelper import DBAcademyHelper

        lab_id = "Smoke Test" if DBAcademyHelper.is_smoke_test() else dbgems.get_parameter(WorkspaceHelper.PARAM_LAB_ID, None)
        return None if lab_id is None else common.clean_string(lab_id)

    @property
    def description(self):
        from dbacademy import dbgems
        from dbacademy import common
        from dbacademy.dbhelper import DBAcademyHelper

        description = "This is a smoke test" if DBAcademyHelper.is_smoke_test() else dbgems.get_parameter(WorkspaceHelper.PARAM_DESCRIPTION, None)
        return None if description is None else common.clean_string(description)
