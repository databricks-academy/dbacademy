__all__ = ["TaskConfig", "NotebookSource"]
# Code Review: JDP on 11-26-2023

import inspect
from enum import Enum
from typing import Dict, Any, List, Optional, Union
from dbacademy.common import validate
from dbacademy.clients.dbrest.clusters_api.cluster_config import LibraryFactory, JobClusterConfig


class NotebookSource(Enum):
    GIT = "GIT"
    WORKSPACE = "WORKSPACE"


class TaskConfig:
    # No defaults, those are done by the calling factory method in JobConfig

    def __init__(self, *,
                 task_key: str,
                 description: Optional[str],
                 max_retries: int,
                 min_retry_interval_millis: int,
                 retry_on_timeout: bool,
                 timeout_seconds: Optional[int],
                 job_params: Optional[Dict[str, Any]],
                 depends_on: Optional[List[str]]):

        self.__params: Dict[str, Any] = dict()
        self.__task_configured: bool = False

        self.__libraries = LibraryFactory(None)
        self.__params["libraries"] = self.__libraries.definitions

        self.__job_params: Dict[str, Any] = validate(job_params=job_params).optional.dict(str, auto_create=True)

        self.params["task_key"] = validate(task_key=task_key).required.str()
        self.params["max_retries"] = validate(max_retries=max_retries).required.int()
        self.params["min_retry_interval_millis"] = validate(min_retry_interval_millis=min_retry_interval_millis).required.int()
        self.params["retry_on_timeout"] = validate(retry_on_timeout=retry_on_timeout).required.bool()
        self.params["depends_on"] = validate(depends_on=depends_on).optional.list(str, auto_create=True)

        if description is not None:
            self.params["description"] = validate(description=description).required.str()

        if timeout_seconds is not None:
            self.params["timeout_seconds"] = validate(timeout_seconds=timeout_seconds).required.int()

    def assert_task_not_configured(self):
        assert self.__task_configured is False, f"""The task "{self.task_key}" has already been defined."""
        self.__task_configured = True

    @property
    def params(self) -> Dict[str, Any]:
        return self.__params

    @property
    def task_key(self) -> str:
        return self.params.get("task_key")

    @property
    def libraries(self) -> LibraryFactory:
        return self.__libraries

    def as_notebook(self, *, notebook_path: str, source: Union[str, NotebookSource], base_parameters: Optional[Dict[str, str]] = None) -> None:
        self.assert_task_not_configured()

        source = validate(source=source).required.enum(NotebookSource, auto_convert=True)

        if source == NotebookSource.GIT:
            assert self.__job_params.get("git_source") is not None, f"The git source must be specified before defining a git notebook task"

        self.params["notebook_task"] = {
            "notebook_path": validate(notebook_path=notebook_path).required.str(),
            "source": source.value,
            "base_parameters": validate(base_parameters=base_parameters).optional.dict(str, str, auto_create=True)
        }

    def as_jar(self) -> None:  # , main_class_name: str, parameters: List[str]) -> AbstractTaskConfig:
        self.assert_task_not_configured()
        raise NotImplementedError(f"""{self.__class__.__name__}.{inspect.stack()[0].function}(..) is not implemented""")

    def as_python(self) -> None:  # , python_file: str, parameters: List[str]) -> AbstractTaskConfig:
        self.assert_task_not_configured()
        raise NotImplementedError(f"""{self.__class__.__name__}.{inspect.stack()[0].function}(..) is not implemented""")

    def as_submit(self) -> None:  # , parameters: List[str]) -> AbstractTaskConfig:
        self.assert_task_not_configured()
        raise NotImplementedError(f"""{self.__class__.__name__}.{inspect.stack()[0].function}(..) is not implemented""")

    def as_pipeline(self) -> None:  # , pipeline_id: str, full_refresh: bool = False) -> AbstractTaskConfig:
        self.assert_task_not_configured()
        raise NotImplementedError(f"""{self.__class__.__name__}.{inspect.stack()[0].function}(..) is not implemented""")

    def as_wheel(self) -> None:  # , package_name: str, entry_point: str, parameters: List[str], named_parameters: List[str]) -> AbstractTaskConfig:
        self.assert_task_not_configured()
        raise NotImplementedError(f"""{self.__class__.__name__}.{inspect.stack()[0].function}(..) is not implemented""")

    def as_sql(self) -> None:  # , query_id: str, dashboard_id: str, alert_id: str, parameters: List[str], warehouse_id: str) -> AbstractTaskConfig:
        self.assert_task_not_configured()
        raise NotImplementedError(f"""{self.__class__.__name__}.{inspect.stack()[0].function}(..) is not implemented""")

    def as_dbt(self) -> None:  # , project_directory: str, commands: List[str], schema: str, warehouse_id: str, catalog: str, profiles_directory: str) -> AbstractTaskConfig:
        self.assert_task_not_configured()
        raise NotImplementedError(f"""{self.__class__.__name__}.{inspect.stack()[0].function}(..) is not implemented""")

        # noinspection PyUnreachableCode
        return self

    def __cluster_reset(self):
        if "existing_cluster_id" in self.params:
            del self.params["existing_cluster_id"]

        if "job_cluster_key" in self.params:
            del self.params["job_cluster_key"]

        if "new_cluster" in self.params:
            del self.params["new_cluster"]

    def cluster_on_demand(self, existing_cluster_id: str) -> None:
        self.__cluster_reset()
        self.params["existing_cluster_id"] = existing_cluster_id

    def cluster_job(self, job_cluster_key: str) -> None:
        self.__cluster_reset()
        self.params["job_cluster_key"] = job_cluster_key

    def cluster_new(self, cluster_config: JobClusterConfig) -> None:
        self.__cluster_reset()
        cluster_config = validate(cluster_config=cluster_config).optional.as_type(JobClusterConfig)
        self.params["new_cluster"] = cluster_config.params
