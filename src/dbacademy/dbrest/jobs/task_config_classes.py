from typing import Dict, Any, List


class TaskConfig:

    def __init__(self, *,
                 job_params: Dict[str, Any],
                 task_params: Dict[str, Any],
                 task_key: str,
                 description: str = None,
                 max_retries: int = 0,
                 min_retry_interval_millis: int = 0,
                 retry_on_timeout: bool = False,
                 timeout_seconds: int = None,
                 depends_on: List[str] = None):

        task_config: TaskConfig = self
        self.defined = []

        self.job_params = job_params
        self.params = task_params

        self.params["task_key"] = task_key
        self.params["max_retries"] = max_retries
        self.params["min_retry_interval_millis"] = min_retry_interval_millis
        self.params["retry_on_timeout"] = retry_on_timeout
        self.params["depends_on"] = depends_on or list()

        if description is not None:
            self.params["description"] = description

        if timeout_seconds is not None:
            self.params["timeout_seconds"] = timeout_seconds

        class Cluster:
            from dbacademy.dbrest.clusters import JobClusterConfig

            def __init__(self):
                self.name = "cluster"

            def on_demand(self, existing_cluster_id: str) -> TaskConfig:
                assert self.name not in task_config.defined, "The cluster has already been defined."
                task_config.defined.append(self.name)
                task_config.params["existing_cluster_id"] = existing_cluster_id
                return task_config

            def job(self, job_cluster_key: str) -> TaskConfig:
                assert self.name not in task_config.defined, "The cluster has already been defined."
                task_config.defined.append(self.name)
                task_config.params["job_cluster_key"] = job_cluster_key
                return task_config

            def new(self, cluster_config: JobClusterConfig) -> TaskConfig:
                from dbacademy.common import validate_type
                from dbacademy.dbrest.clusters import JobClusterConfig

                assert self.name not in task_config.defined, "The cluster has already been defined."
                validate_type(cluster_config, "cluster_config", JobClusterConfig)

                task_config.defined.append(self.name)
                task_config.params["new_cluster"] = cluster_config.params
                return task_config

        self.cluster = Cluster()

        class Task:
            def __init__(self):
                self.name = "task"

            def notebook(self, notebook_path: str, source: str, base_parameters: Dict[str, str] = None) -> TaskConfig:
                assert self.name not in task_config.defined, "The task has already been defined."
                task_config.defined.append(self.name)

                sources = ["WORKSPACE", "GIT"]
                assert source.upper() in sources, f"The source parameter must be one of {sources}, found \"{source}\""

                if source == "GIT":
                    assert task_config.job_params["git_source"] is not None, f"The git source must be specified before defining a git notebook task"

                task_config.params["notebook_task"] = {
                    "notebook_path": notebook_path,
                    "source": source,
                    "base_parameters": base_parameters or dict()
                }

                return task_config

            def jar(self) -> TaskConfig:  # , main_class_name: str, parameters: List[str]) -> TaskConfig:
                assert self.name not in task_config.defined, "The task has already been defined."
                task_config.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_config

            def python(self) -> TaskConfig:  # , python_file: str, parameters: List[str]) -> TaskConfig:
                assert self.name not in task_config.defined, "The task has already been defined."
                task_config.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_config

            def submit(self) -> TaskConfig:  # , parameters: List[str]) -> TaskConfig:
                assert self.name not in task_config.defined, "The task has already been defined."
                task_config.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_config

            def pipeline(self) -> TaskConfig:  # , pipeline_id: str, full_refresh: bool = False) -> TaskConfig:
                assert self.name not in task_config.defined, "The task has already been defined."
                task_config.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_config

            def wheel(self) -> TaskConfig:  # , package_name: str, entry_point: str, parameters: List[str], named_parameters: List[str]) -> TaskConfig:
                assert self.name not in task_config.defined, "The task has already been defined."
                task_config.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_config

            def sql(self) -> TaskConfig:  # , query_id: str, dashboard_id: str, alert_id: str, parameters: List[str], warehouse_id: str) -> TaskConfig:
                assert self.name not in task_config.defined, "The task has already been defined."
                task_config.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_config

            def dbt(self) -> TaskConfig:  # , project_directory: str, commands: List[str], schema: str, warehouse_id: str, catalog: str, profiles_directory: str) -> TaskConfig:
                assert self.name not in task_config.defined, "The task has already been defined."
                task_config.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_config

        self.task = Task()

        class Library:
            def __init__(self):
                self.name = "library"

            def jar(self):
                assert self.name not in task_config.defined, "The library has already been defined."
                task_config.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_config

            def egg(self):
                assert self.name not in task_config.defined, "The library has already been defined."
                task_config.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_config

            def wheel(self):
                assert self.name not in task_config.defined, "The library has already been defined."
                task_config.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_config

            def pypi(self):
                assert self.name not in task_config.defined, "The library has already been defined."
                task_config.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_config

            def maven(self):
                assert self.name not in task_config.defined, "The library has already been defined."
                task_config.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_config

            def cran(self):
                assert self.name not in task_config.defined, "The library has already been defined."
                task_config.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_config

            def from_dict(self, libraries):
                assert self.name not in task_config.defined, "The library has already been defined."
                task_config.defined.append(self.name)
                task_config.libraries = libraries
                return task_config

        self.library: Library = Library()

    def add_email_notifications(self, *, on_start: List[str], on_success: List[str], on_failure: List[str], no_alert_for_skipped_runs: bool = False) -> "TaskConfig":
        self.params["email_notifications"] = {
            "on_start": on_start or list(),
            "on_success": on_success or list(),
            "on_failure": on_failure or list(),
            "no_alert_for_skipped_runs": no_alert_for_skipped_runs
        },
        return self

    def add_webhook_notifications(self, *, on_start: List[str], on_success: List[str], on_failure: List[str]) -> "TaskConfig":
        self.params["email_notifications"] = {
            "on_start": on_start or list(),
            "on_success": on_success or list(),
            "on_failure": on_failure or list(),
        },
        return self
