__all__ = ["WorkspaceHelper"]

from typing import Callable, List, TypeVar, Optional, Union, Dict, Any
from dbacademy.dbhelper import dbh_constants
from dbacademy.dbhelper.lesson_config import LessonConfig
from dbacademy.clients.darest import DBAcademyRestClient

T = TypeVar("T")


class WorkspaceHelper:

    def __init__(self, db_academy_rest_client: DBAcademyRestClient):
        from dbacademy.common import validate

        self.__client = validate(db_academy_rest_client=db_academy_rest_client).required.as_type(DBAcademyRestClient)

        self._usernames = None
        self.__existing_databases = None
        self.__existing_catalogs = None

    @staticmethod
    def get_spark_version():
        from dbacademy import dbgems

        return dbgems.get_parameter(dbh_constants.WORKSPACE_HELPER.PARAM_DEFAULT_SPARK_VERSION)

    @staticmethod
    def get_lab_id():
        from dbacademy import dbgems

        return dbgems.get_parameter(dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_ID)

    @staticmethod
    def get_workspace_name():
        from dbacademy import dbgems

        return dbgems.get_spark_config("spark.databricks.workspaceUrl", default=dbgems.get_notebooks_api_endpoint())

    @staticmethod
    def get_workspace_description():
        from dbacademy import dbgems

        return dbgems.get_parameter(dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_DESCRIPTION)

    @staticmethod
    def install_datasets(datasets: Union[str, List[str]]):
        from dbacademy import dbgems
        from dbacademy.dbhelper.dataset_manager import DatasetManager
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        if datasets is not None:
            if type(datasets) == list:
                pass  # leave as is.
            elif type(datasets) == str and datasets.strip() not in ("", "null", "None"):
                datasets = datasets.split(",")
                print(f"Installing {len(datasets)} datasets...")
            else:
                raise Exception(f"""The parameter "datasets" is expected to be of type "str" (and comma seperated) or "list", found "{type(datasets)}".""")
        else:
            print(f"Installing all datasets...")
            datasets = [
                "example-course",
                "apache-spark-programming-with-databricks",
                "advanced-data-engineering-with-databricks",
                # "large-language-models",  # Not ILT courseware and is freakishly huge.
                "data-analysis-with-databricks",
                "data-engineer-learning-path",
                "data-engineering-with-databricks",
                "deep-learning-with-databricks",
                "introduction-to-python-for-data-science-and-data-engineering",
                "ml-in-production",
                "scalable-machine-learning-with-apache-spark",
            ]
            print(f"Installing all instructor-led ({len(datasets)}) datasets...")

        print("\n" + ("=" * 100) + "\n")

        for dataset in datasets:
            if ":" in dataset:
                data_source_name, dataset_version = dataset.split(":")
            else:
                data_source_name = dataset
                dataset_version = None

            inventory_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net"
            datasets_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{data_source_name}"

            inventory = [i.name.split("/")[0] for i in dbgems.dbutils.fs.ls(inventory_uri)]
            if data_source_name not in inventory:
                print(f"""| Dataset "{data_source_name}" not found.""")
                continue

            # We may have to install more than one version due to the
            # fact that we often teach the latest and latest-1 for a season.
            versions = list()

            if dataset_version is not None:
                # We were told which one to use.
                versions.append(dataset_version)
            else:
                try:
                    files = sorted(dbgems.dbutils.fs.ls(datasets_uri))
                except:
                    # dbutils.fs.ls can fail when the directory doesn't exist.
                    files = list()

                if len(files) > 1:
                    # There are at least two versions, so we need to install the last two just in case.
                    versions.append(files[-1].name[:-1])  # dropping the trailing /
                    versions.append(files[-2].name[:-1])
                elif len(files) > 0:
                    # There is only one version, just use the last one.
                    versions.append(files[-1].name[:-1])

            for data_source_version in versions:
                start = dbgems.clock_start()

                data_source_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{data_source_name}/{data_source_version}"
                # remote_files = DatasetManager.list_r(data_source_uri)

                install_path = f"{DBAcademyHelper.get_dbacademy_datasets_path()}/{data_source_name}/{data_source_version}"

                print(f"| {data_source_uri}")
                print("| listing remote files", end="...")

                print(dbgems.clock_stopped(start))

                dataset_manager = DatasetManager(_data_source_uri=data_source_uri,
                                                 _staging_source_uri=None,
                                                 _install_path=install_path,
                                                 _datasets_path=None,
                                                 _archives_path=None,
                                                 _install_min_time=None,
                                                 _install_max_time=None)

                dataset_manager.install_dataset(reinstall_datasets=False)

                if data_source_version == versions[-1] and len(versions) > 1:
                    print("\n" + ("-" * 100) + "\n")

    def __parse_args(self, courses_arg: str, usernames: List[str]) -> (List[str], Dict[str, Any]):

        if courses_arg is None or courses_arg.strip() in ("", "null", "None"):
            print("No courses specified.")
            return list(), None

        course_defs = [c.strip() for c in courses_arg.split(",")]
        usernames = usernames or [u.get("userName") for u in self.__client.scim.users.list()]

        return usernames, course_defs

    def uninstall_courseware(self, courses_arg: str, subdirectory: str, usernames: List[str] = None) -> None:

        usernames, course_defs = self.__parse_args(courses_arg, usernames)

        for username in usernames:
            print(f"Uninstalling courses for {username}")

            for course_def in course_defs:
                url, course, version, artifact, token = WorkspaceHelper.parse_course_args(course_def)

                if subdirectory is None:
                    install_dir = f"/Users/{username}/{course}"
                else:
                    install_dir = f"/Users/{username}/{subdirectory}/{course}"

                print(install_dir)
                self.__client.workspace.delete_path(install_dir)

            print("-" * 80)

    def install_courseware(self, courses_arg: str, subdirectory: str, usernames: List[str] = None) -> None:

        usernames, course_defs = self.__parse_args(courses_arg, usernames)

        for username in usernames:

            print(f"Installing courses for {username}")

            for course_def in course_defs:
                print()

                url, course, version, artifact, token = WorkspaceHelper.parse_course_args(course_def)
                download_url = WorkspaceHelper.compose_courseware_url(url, course, version, artifact, token)

                if subdirectory is None:
                    install_dir = f"/Users/{username}/{course}"
                else:
                    install_dir = f"/Users/{username}/{subdirectory}/{course}"

                print(f" - {install_dir}")

                files = self.__client.workspace.ls(install_dir)
                count = 0 if files is None else len(files)
                if count > 0:
                    print(f" - Skipping, course already exists.")
                    # for file in files or list():
                    #     path = file.get("path")
                    #     print(f" - {path}")
                else:
                    self.__client.workspace.import_dbc_files(install_dir, download_url)
                    print(f" - Installed.")

            print("-" * 80)

    @staticmethod
    def compose_courseware_url(url: str, course: str, version: Optional[str], artifact: Optional[str], token: str) -> str:
        download_url = f"{url}?course={course}"

        if version is not None:
            download_url += f"&version={version}"

        if artifact is not None:
            download_url += f"&artifact={artifact}"

        if token is None:
            raise AssertionError(f"The CDS API token must be specified.")
        download_url += f"&token={token}"

        return download_url

    @staticmethod
    def parse_course_args(course_def: str) -> (str, str, str, str, str):

        try:
            from dbacademy.dbgems import dbutils
            token = dbutils.secrets.get("workspace-setup", "token")
        except:
            token = None

        parts = course_def.split("?")
        if len(parts) == 1:
            url = f"https://labs.training.databricks.com/api/v1/courses/download.dbc"
            query = parts[0]
        elif len(parts) == 2:
            url = parts[0]
            query = parts[1]
        else:
            raise Exception(f"""Invalid course definition, found "{course_def}".""")

        try:
            params = {p.split("=")[0]: p.split("=")[1] for p in query.split("&")}
            course = params.get("course")
            version = params.get("version")
            artifact = params.get("artifact")
            token = token or params.get("token")

            return url, course, version, artifact, token
        except Exception as e:
            raise Exception(f"""Unexpected exception parsing query parameters "{query}".""") from e

    def add_entitlement_allow_instance_pool_create(self):
        group = self.__client.scim.groups.get_by_name("users")
        self.__client.scim.groups.add_entitlement(group.get("id"), "allow-instance-pool-create")

    def add_entitlement_workspace_access(self):
        group = self.__client.scim.groups.get_by_name("users")
        self.__client.scim.groups.add_entitlement(group.get("id"), "workspace-access")

    def add_entitlement_allow_cluster_create(self):
        group = self.__client.scim.groups.get_by_name("users")
        self.__client.scim.groups.add_entitlement(group.get("id"), "allow-cluster-create")

    def add_entitlement_databricks_sql_access(self):
        group = self.__client.scim.groups.get_by_name("users")
        self.__client.scim.groups.add_entitlement(group.get("id"), "databricks-sql-access")

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

    def get_usernames(self, lesson_config: LessonConfig):
        from dbacademy.common import validate
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        lesson_config = validate(lesson_config=lesson_config).required.as_type(LessonConfig)

        if self._usernames is None:
            users = self.__client.scim().users().list()
            self._usernames = [r.get("userName") for r in users]
            self._usernames.sort()

        # TODO - This isn't going to hold up long-term, maybe track per-user properties in this respect.
        # The presumption here is that if the user doesn't have their own
        # database, then they are also missing the rest of their config.
        missing_users = set()

        if lesson_config.requires_uc:
            for username in self._usernames:
                prefix = DBAcademyHelper.to_catalog_name_prefix(username=username)
                for catalog_name in self.existing_catalogs:
                    if catalog_name.startswith(prefix):
                        break
                else:
                    missing_users.add(username)
        else:
            for username in self._usernames:
                prefix = DBAcademyHelper.to_schema_name_prefix(username=username, course_code=lesson_config.course_config.course_code)

                for schema_name in self.existing_databases:
                    if schema_name.startswith(prefix):
                        break
                else:
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
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        lab_id = "Smoke Test" if DBAcademyHelper.is_smoke_test() else dbgems.get_parameter(dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_ID, None)
        return None if lab_id is None else common.clean_string(lab_id)

    @property
    def description(self):
        from dbacademy import dbgems
        from dbacademy import common
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        description = "This is a smoke test" if DBAcademyHelper.is_smoke_test() else dbgems.get_parameter(dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_DESCRIPTION, None)
        return None if description is None else common.clean_string(description)
