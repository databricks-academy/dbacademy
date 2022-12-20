from typing import Callable, TypeVar

from dbacademy import dbgems


class DatabasesHelper:
    from dbacademy.dbrest import DBAcademyRestClient
    from .dbacademy_helper_class import DBAcademyHelper
    from .workspace_helper_class import WorkspaceHelper

    T = TypeVar("T")

    def __init__(self, workspace: WorkspaceHelper, da: DBAcademyHelper):
        self.da = da
        self.client = da.client
        self.workspace = workspace

    def drop_databases(self, configure_for: str):
        self.workspace.do_for_all_users(self.workspace.get_usernames(configure_for), lambda username: self.__drop_databases_for(username=username))

        # Clear the list of databases (and derived users) to force a refresh
        self.workspace._usernames = None
        self.workspace._existing_databases = None

    def __drop_databases_for(self, username: str):
        dropped = False
        prefix = self.da.to_schema_name_prefix(username=username,
                                               course_code=self.da.course_config.course_code)

        for schema_name in self.workspace.existing_databases:
            if schema_name.startswith(prefix):
                print(f"Dropping the database \"{schema_name}\" for {username}")
                dropped = True
                dbgems.spark.sql(f"DROP DATABASE {schema_name} CASCADE;")

        if not dropped:
            print(f"Database not drop for {username}")

    def create_databases(self, configure_for: str, drop_existing: bool, post_create: Callable[[str, str], None] = None):
        usernames = self.workspace.get_usernames(configure_for)
        self.workspace.do_for_all_users(usernames, lambda username: self.__create_database_for(username=username,
                                                                                               drop_existing=drop_existing,
                                                                                               post_create=post_create))
        # Clear the list of databases (and derived users) to force a refresh
        self.workspace._usernames = None
        self.workspace._existing_databases = None

    def __create_database_for(self, username: str, drop_existing: bool, post_create: Callable[[str, str], None] = None):
        from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper

        db_name = self.da.to_schema_name_prefix(username=username,
                                                course_code=self.da.course_config.course_code)
        db_path = f"{DBAcademyHelper.get_dbacademy_users_path()}/{username}/{self.da.course_config.course_name}/database.db"

        if db_name in self.da.workspace.existing_databases:
            # The database already exists.

            if drop_existing:
                dbgems.spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE;")
            else:
                return print(f"Skipping existing schema \"{db_name}\" for {username}")

        dbgems.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_path}';")

        msg = f"Created schema \"{db_name}\" for \"{username}\", dropped existing: {drop_existing}"

        if post_create:
            # Call the post-create init function if defined
            response = post_create(username, db_name)
            if response is not None:
                msg += "\n"
                msg += str(response)

        return print(msg)

    @staticmethod
    def configure_permissions(client: DBAcademyRestClient, notebook_name: str, spark_version: str):
        from dbacademy.common import Cloud
        from dbacademy.dbhelper import DBAcademyHelper, WorkspaceHelper

        job_name = f"""DBAcademy {notebook_name.split("/")[-1]}"""
        print(f"Starting job \"{job_name}\" to update catalog and schema specific permissions")

        client.jobs().delete_by_name(job_name, success_only=False)

        notebook_path = f"{dbgems.get_notebook_dir()}/{notebook_name}"

        params = {
            "name": job_name,
            "tags": {
                "dbacademy.source": dbgems.clean_string("Smoke-Test" if DBAcademyHelper.is_smoke_test() else WorkspaceHelper.get_lab_id())
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
                            DBAcademyHelper.SPARK_CONF_PROTECTED_EXECUTION: True,
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
        cluster_params = params.get("tasks")[0].get("new_cluster")
        cluster_params["spark_version"] = spark_version

        if client.clusters().get_current_instance_pool_id() is not None:
            cluster_params["instance_pool_id"] = client.clusters().get_current_instance_pool_id()
        else:
            cluster_params["node_type_id"] = client.clusters().get_current_node_type_id()
            if Cloud.current_cloud().is_aws:
                # noinspection PyTypeChecker
                cluster_params["aws_attributes"] = {"availability": "ON_DEMAND"}

        create_response = client.jobs().create(params)
        job_id = create_response.get("job_id")

        run_response = client.jobs().run_now(job_id)
        run_id = run_response.get("run_id")

        print(f"| See {dbgems.get_workspace_url()}#job/{job_id}/run/{run_id}")

        final_response = client.runs().wait_for(run_id)

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
