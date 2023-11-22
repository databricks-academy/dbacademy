# __all__ = ["DatabasesHelper"]
__all__ = []

from typing import Callable, List, Dict
from dbacademy.dbhelper import dbh_constants
from dbacademy.common import validate
from dbacademy.clients.darest import DBAcademyRestClient
from dbacademy.dbhelper.lesson_config import LessonConfig
from dbacademy.dbhelper.supporting.workspace_helper import WorkspaceHelper


class DatabasesHelper:

    def __init__(self, db_academy_rest_client: DBAcademyRestClient, workspace_helper: WorkspaceHelper):
        self.__client = validate(db_academy_rest_client=db_academy_rest_client).required.as_type(DBAcademyRestClient)
        self.__workspace_helper = validate(workspace_helper=workspace_helper).required.as_type(WorkspaceHelper)

    def drop_databases(self, lesson_config: LessonConfig) -> None:

        lesson_config = validate(lesson_config=lesson_config).required.as_type(LessonConfig)

        usernames = self.__workspace_helper.get_usernames(lesson_config=lesson_config)
        groups = self.__to_group_of(usernames=usernames, max_group_size=50)

        for i, group in enumerate(groups):
            print(f"| Processing group {i+1} of {len(groups)} ({len(group)} users)")
            self.__workspace_helper.do_for_all_users(group, lambda username: self.__drop_databases_for(index=group.index(username),
                                                                                                       count=len(group),
                                                                                                       username=username,
                                                                                                       course_code=lesson_config.course_config.course_code))

            print("-" * 80)
            print()

        # Clear the list of databases (and derived users) to force a refresh
        self.__workspace_helper._usernames = None
        self.__workspace_helper.clear_existing_databases()

    def __drop_databases_for(self, *,
                             index: int,
                             count: int,
                             username: str,
                             course_code: str) -> None:

        from dbacademy import dbgems
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        dropped = False
        prefix = DBAcademyHelper.to_schema_name_prefix(username=username, course_code=course_code)

        for schema_name in self.__workspace_helper.existing_databases:
            if schema_name.startswith(prefix):
                print(f"| ({index+1}/{count}) Dropping the database \"{schema_name}\" for {username}")
                try:
                    dbgems.spark.sql(f"DROP DATABASE {schema_name} CASCADE;")
                    dropped = True
                except:
                    pass  # I don't care if it didn't exist.
        if not dropped:
            print(f"| ({index+1}/{count}) Database not dropped for {username}")

    def drop_catalogs(self, lesson_config: LessonConfig) -> None:

        lesson_config = validate(lesson_config=lesson_config).required.as_type(LessonConfig)

        self.__workspace_helper.do_for_all_users(self.__workspace_helper.get_usernames(lesson_config=lesson_config),
                                                 lambda username: self.__drop_catalogs_for(username=username,
                                                                                           lesson_config=lesson_config))

        # Clear the list of catalogs (and derived users) to force a refresh
        self.__workspace_helper._usernames = None
        self.__workspace_helper.clear_existing_databases()
        self.__workspace_helper.clear_existing_catalogs()

    def __drop_catalogs_for(self, username: str, lesson_config: LessonConfig) -> None:

        username = validate(username=username).required.str()
        lesson_config = validate(lesson_config=lesson_config).required.as_type(LessonConfig)

        from dbacademy import dbgems
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        dropped = False
        prefix = DBAcademyHelper.to_schema_name_prefix(username=username,
                                                       course_code=lesson_config.course_config.course_code)

        for catalog_name in self.__workspace_helper.existing_catalogs:
            if catalog_name.startswith(prefix):
                print(f"Dropping the catalog \"{catalog_name}\" for {username}")
                dropped = True
                dbgems.spark.sql(f"DROP CATALOG {catalog_name} CASCADE;")

        if not dropped:
            print(f"Catalog not drop for {username}")

    @staticmethod
    def __to_group_of(*,
                      usernames: List[str],
                      max_group_size: int) -> List[List[str]]:

        groups = list()
        curr_list = list()

        for username in usernames:
            if len(curr_list) == max_group_size:
                # create a new set of max_group_size
                groups.append(curr_list)
                curr_list = list()

            curr_list.append(username)

        groups.append(curr_list)
        return groups

    def create_databases(self, *,
                         drop_existing: bool,
                         lesson_config: LessonConfig,
                         post_create: Callable[[str, str], None] = None) -> None:

        drop_existing = validate(drop_existing=drop_existing).required.bool()
        lesson_config = validate(lesson_config=lesson_config).required.as_type(LessonConfig)
        post_create = validate(post_create=post_create).as_type(Callable)

        print(f"| Creating user-specific databases.")

        usernames = self.__workspace_helper.get_usernames(lesson_config=lesson_config)

        groups = self.__to_group_of(usernames=usernames, max_group_size=50)

        # Refactored to process only 50 at a time.
        print(f"| Processing {len(usernames)} users as {len(groups)} groups.")

        for i, group in enumerate(groups):
            print(f"| Processing group {i+1} of {len(groups)} ({len(group)} users)")
            self.__workspace_helper.do_for_all_users(group, lambda user: self.__create_database_for(username=user,
                                                                                                    drop_existing=drop_existing,
                                                                                                    lesson_config=lesson_config,
                                                                                                    post_create=post_create))
            print("-" * 80)
            print()
        # Clear the list of databases (and derived users) to force a refresh
        self.__workspace_helper._usernames = None

    def __create_database_for(self, *, 
                              username: str, 
                              drop_existing: bool, 
                              lesson_config: LessonConfig,
                              post_create: Callable[[str, str], None] = None) -> None:
        
        from dbacademy import dbgems
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        username = validate(username=username).required.str()
        drop_existing = validate(drop_existing=drop_existing).required.bool()
        lesson_config = validate(lesson_config=lesson_config).required.as_type(LessonConfig)
        post_create = validate(post_create=post_create).as_type(Callable)

        db_name = DBAcademyHelper.to_schema_name_prefix(username=username,
                                                        course_code=lesson_config.course_config.course_code)
        db_path = f"{DBAcademyHelper.get_dbacademy_users_path()}/{username}/{lesson_config.course_config.course_name}/database.db"

        if db_name in self.__workspace_helper.existing_databases:
            # The database already exists.

            if drop_existing:
                dbgems.spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE;")
            else:
                return print(f"| Skipping existing schema \"{db_name}\" for {username}")

        dbgems.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_path}';")

        msg = f"|\n| Created schema \"{db_name}\" for \"{username}\", dropped existing: {drop_existing}"

        if post_create:
            # Call the post-create init function if defined
            response = post_create(username, db_name)
            if response is not None:
                msg += "\n"
                msg += str(response)

        return print(msg)

    def create_catalog(self, *, 
                       drop_existing: bool,
                       lesson_config: LessonConfig,
                       post_create: Callable[[str, str], None] = None) -> None:

        drop_existing = validate(drop_existing=drop_existing).required.bool()
        lesson_config = validate(lesson_config=lesson_config).required.as_type(LessonConfig)
        post_create = validate(post_create=post_create).as_type(Callable)
        
        usernames = self.__workspace_helper.get_usernames(lesson_config=lesson_config)
        
        self.__workspace_helper.do_for_all_users(usernames, lambda username: self.__create_catalog_for(username=username,
                                                                                                       drop_existing=drop_existing,
                                                                                                       lesson_config=lesson_config,
                                                                                                       post_create=post_create))
        # Clear the list of catalogs (and derived users) to force a refresh
        self.__workspace_helper._usernames = None
        self.__workspace_helper.clear_existing_databases()
        self.__workspace_helper.clear_existing_catalogs()

    def __create_catalog_for(self, *,
                             username: str, 
                             drop_existing: bool, 
                             lesson_config: LessonConfig,
                             post_create: Callable[[str, str], None] = None) -> None:
        
        from dbacademy import dbgems
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        username = validate(username=username).required.str()
        drop_existing = validate(drop_existing=drop_existing).required.bool()
        lesson_config = validate(lesson_config=lesson_config).required.as_type(LessonConfig)
        post_create = validate(post_create=post_create).as_type(Callable)

        cat_name = DBAcademyHelper.to_schema_name_prefix(username=username,
                                                         course_code=lesson_config.course_config.course_code)
        # db_path = f"{DBAcademyHelper.get_dbacademy_users_path()}/{username}/{self.__da.course_config.course_name}/database.db"

        if cat_name in self.__workspace_helper.existing_catalogs:
            # The catalog already exists.

            if drop_existing:
                dbgems.spark.sql(f"DROP CATALOG IF EXISTS {cat_name} CASCADE;")
            else:
                return print(f"Skipping existing catalog \"{cat_name}\" for {username}")

        dbgems.sql(f"CREATE CATALOG IF NOT EXISTS {cat_name};")

        msg = f"Created schema \"{cat_name}\" for \"{username}\", dropped existing: {drop_existing}"

        if post_create:
            # Call the post-create init function if defined
            response = post_create(username, cat_name)
            if response is not None:
                msg += "\n"
                msg += str(response)

        return print(msg)

    def configure_permissions(self, notebook_name: str, spark_version: str):
        from dbacademy import dbgems
        from dbacademy.common import Cloud

        job_name = f"""DBAcademy {notebook_name.split("/")[-1]}"""
        print(f"Starting job \"{job_name}\" to update catalog and schema specific permissions")

        self.__client.jobs().delete_by_name(job_name, success_only=False)

        notebook_path = f"{dbgems.get_notebook_dir()}/{notebook_name}"

        params = {
            "name": job_name,
            "tags": {
                # "dbacademy.source": common.clean_string("Smoke-Test" if DBAcademyHelper.is_smoke_test() else WorkspaceHelper.get_lab_id())
            },
            "email_notifications": {},
            "timeout_seconds": 7200,
            "max_concurrent_runs": 1,
            "format": "MULTI_TASK",
            "tasks": [
                {
                    "task_key": "Configure-Permissions",
                    "description": "Configure all users' permissions for user-specific databases.",
                    "libraries": [],
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": {}
                    },
                    "new_cluster": {
                        "num_workers": 0,
                        "cluster_name": "",
                        "spark_conf": {
                            dbh_constants.DBACADEMY_HELPER.SPARK_CONF_PROTECTED_EXECUTION: True,
                            "spark.master": "local[*]",
                            "spark.databricks.acl.dfAclsEnabled": "true",
                            "spark.databricks.repl.allowedLanguages": "sql,python",
                            "spark.databricks.cluster.profile": "serverless",
                        },
                        "runtime_engine": "STANDARD",
                    },
                },
            ],
        }
        new_cluster = params.get("tasks")[0].get("new_cluster")
        new_cluster["spark_version"] = spark_version

        if self.__client.clusters().get_current_instance_pool_id() is not None:
            new_cluster["instance_pool_id"] = self.__client.clusters().get_current_instance_pool_id()
        else:
            new_cluster["node_type_id"] = self.__client.clusters().get_current_node_type_id()
            if Cloud.current_cloud().is_aws:
                aws_attributes: Dict[str, str] = {"availability": "ON_DEMAND"}
                new_cluster["aws_attributes"] = aws_attributes

        create_response = self.__client.jobs().create(params)
        job_id = create_response.get("job_id")

        run_response = self.__client.jobs().run_now(job_id)
        run_id = run_response.get("run_id")

        print(f"| See {dbgems.get_workspace_url()}#job/{job_id}/run/{run_id}")

        final_response = self.__client.runs().wait_for(run_id)

        final_state = final_response.get("state").get("result_state")
        assert final_state == "SUCCESS", f"Expected the final state to be SUCCESS, found {final_state}"

        print()
        print(f"Completed \"{job_name}\" ({job_id}) successfully.")

        dbgems.display_html(f"""
        <html style="margin:0"><body style="margin:0"><div style="margin:0">
            See <a href="/#job/{job_id}/run/{run_id}" target="_blank">{job_name} ({job_id}/{run_id})</a>
        </div></body></html>
        """)

        return job_id
