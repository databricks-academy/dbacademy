from typing import Dict, Any, List
from dbacademy import common
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer
from dbacademy.dbrest.clusters import ClusterConfig


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

        task_builder = self
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
            def __init__(self):
                self.name = "cluster"

            def on_demand(self, existing_cluster_id: str) -> TaskConfig:
                assert self.name not in task_builder.defined, "The cluster has already been defined."
                task_builder.defined.append(self.name)
                task_builder.params["existing_cluster_id"] = existing_cluster_id
                return task_builder

            def job(self, job_cluster_key: str) -> TaskConfig:
                assert self.name not in task_builder.defined, "The cluster has already been defined."
                task_builder.defined.append(self.name)
                task_builder.params["job_cluster_key"] = job_cluster_key
                return task_builder

            def new(self, cluster_config: ClusterConfig) -> TaskConfig:
                assert self.name not in task_builder.defined, "The cluster has already been defined."
                task_builder.defined.append(self.name)
                task_builder.params["new_cluster"] = cluster_config.params
                return task_builder

        self.cluster = Cluster()

        class Task:
            def __init__(self):
                self.name = "task"

            def notebook(self, notebook_path: str, source: str, base_parameters: Dict[str, str] = None) -> TaskConfig:
                assert self.name not in task_builder.defined, "The task has already been defined."
                task_builder.defined.append(self.name)

                sources = ["WORKSPACE", "GIT"]
                assert source.upper() in sources, f"The source parameter must be one of {sources}, found \"{source}\""

                if source == "GIT":
                    assert task_builder.job_params["git_source"] is not None, f"The git source must be specified before defining a git notebook task"

                task_builder.params["notebook_task"] = {
                    "notebook_path": notebook_path,
                    "source": source,
                    "base_parameters": base_parameters or dict()
                }

                return task_builder

            def jar(self) -> TaskConfig:  # , main_class_name: str, parameters: List[str]) -> TaskConfig:
                assert self.name not in task_builder.defined, "The task has already been defined."
                task_builder.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_builder

            def python(self) -> TaskConfig:  # , python_file: str, parameters: List[str]) -> TaskConfig:
                assert self.name not in task_builder.defined, "The task has already been defined."
                task_builder.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_builder

            def submit(self) -> TaskConfig:  # , parameters: List[str]) -> TaskConfig:
                assert self.name not in task_builder.defined, "The task has already been defined."
                task_builder.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_builder

            def pipeline(self) -> TaskConfig:  # , pipeline_id: str, full_refresh: bool = False) -> TaskConfig:
                assert self.name not in task_builder.defined, "The task has already been defined."
                task_builder.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_builder

            def wheel(self) -> TaskConfig:  # , package_name: str, entry_point: str, parameters: List[str], named_parameters: List[str]) -> TaskConfig:
                assert self.name not in task_builder.defined, "The task has already been defined."
                task_builder.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_builder

            def sql(self) -> TaskConfig:  # , query_id: str, dashboard_id: str, alert_id: str, parameters: List[str], warehouse_id: str) -> TaskConfig:
                assert self.name not in task_builder.defined, "The task has already been defined."
                task_builder.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_builder

            def dbt(self) -> TaskConfig:  # , project_directory: str, commands: List[str], schema: str, warehouse_id: str, catalog: str, profiles_directory: str) -> TaskConfig:
                assert self.name not in task_builder.defined, "The task has already been defined."
                task_builder.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_builder

        self.task = Task()

        class Library:
            def __init__(self):
                self.name = "library"

            def jar(self):
                assert self.name not in task_builder.defined, "The library has already been defined."
                task_builder.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_builder

            def egg(self):
                assert self.name not in task_builder.defined, "The library has already been defined."
                task_builder.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_builder

            def wheel(self):
                assert self.name not in task_builder.defined, "The library has already been defined."
                task_builder.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_builder

            def pypi(self):
                assert self.name not in task_builder.defined, "The library has already been defined."
                task_builder.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_builder

            def maven(self):
                assert self.name not in task_builder.defined, "The library has already been defined."
                task_builder.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_builder

            def cran(self):
                assert self.name not in task_builder.defined, "The library has already been defined."
                task_builder.defined.append(self.name)
                assert 1/0, "Not yet implemented"
                return task_builder

        self.library = Library()

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


class JobConfig:
    def __init__(self, *,
                 job_name: str,
                 timeout_seconds: int = 300,
                 max_concurrent_runs: int = 1,
                 tags: Dict[str, str] = None):

        self.params = {
            "name": job_name,
            "tags": tags or dict(),
            "timeout_seconds": timeout_seconds,
            "max_concurrent_runs": max_concurrent_runs,
            "format": "MULTI_TASK",
            "tasks": [],
        }

    def git_branch(self, *, provider: str, url: str, branch: str):
        self.params["git_source"] = {
            "git_provider": provider,
            "git_url": url,
            "git_branch": branch
        }

    def git_tag(self, *, provider: str, url: str, tag: str):
        self.params["git_source"] = {
            "git_provider": provider,
            "git_url": url,
            "git_tag": tag
        }

    def git_commit(self, *, provider: str, url: str, commit: str):
        self.params["git_source"] = {
            "git_provider": provider,
            "git_url": url,
            "git_commit": commit
        }

    def add_task(self, *, task_key: str, description: str = None, max_retries: int = 0, min_retry_interval_millis: int = 0, retry_on_timeout: bool = False, timeout_seconds: int = None, depends_on: List[str] = None) -> TaskConfig:
        depends_on = depends_on or list()

        if "tasks" not in self.params:
            self.params["tasks"] = []

        task = dict()
        self.params["tasks"].append(task)

        return TaskConfig(job_params=self.params,
                          task_params=task,
                          task_key=task_key,
                          description=description,
                          max_retries=max_retries,
                          min_retry_interval_millis=min_retry_interval_millis,
                          retry_on_timeout=retry_on_timeout,
                          timeout_seconds=timeout_seconds,
                          depends_on=depends_on)


class JobsClient(ApiContainer):

    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class
        self.base_uri = f"{self.client.endpoint}/api/2.1/jobs"

    @common.deprecated("Use JobsClient.create_from_config() instead")
    def create(self, params) -> Dict[str, Any]:
        if "notebook_task" in params:
            print("*"*80)
            print("* DEPRECATION WARNING")
            print("* You are using the Jobs 2.0 version of create as noted by the existence of the notebook_task parameter.")
            print("* Please upgrade to the 2.1 version.")
            print("*"*80)
            return self.create_2_0(params)
        else:
            return self.create_2_1(params)

    @common.deprecated("Use JobsClient.create_from_config() instead")
    def create_2_0(self, params) -> Dict[str, Any]:
        return self.client.api("POST", f"{self.client.endpoint}/api/2.0/jobs/create", params)

    @common.deprecated("Use JobsClient.create_from_config() instead")
    def create_2_1(self, params) -> Dict[str, Any]:
        return self.client.api("POST", f"{self.client.endpoint}/api/2.1/jobs/create", params)

    def create_from_config(self, config: JobConfig) -> str:
        return self.create_from_dict(config.params)

    def create_from_dict(self, params: Dict[str, Any]) -> str:
        response = self.client.api("POST", f"{self.client.endpoint}/api/2.1/jobs/create", params)
        return response.get("job_id")

    def run_now(self, job_id: str, notebook_params: dict = None):
        payload = {
            "job_id": job_id
        }
        if notebook_params is not None:
            payload["notebook_params"] = notebook_params

        return self.client.api("POST", f"{self.client.endpoint}/api/2.0/jobs/run-now", payload)

    @common.deprecated("Use JobsClient.get_by_id() instead")
    def get(self, job_id):
        return self.get_by_id(job_id)

    def get_by_id(self, job_id):
        return self.client.api("GET", f"{self.client.endpoint}/api/2.0/jobs/get?job_id={job_id}")

    def get_by_name(self, name: str):
        offset = 0  # Start with zero
        limit = 25  # Default maximum

        def search(jobs_list):
            job_ids = [j.get("job_id") for j in jobs_list if name == j.get("settings").get("name")]
            return (False, None) if len(job_ids) == 0 else (True, self.get_by_id(job_ids[0]))

        target_url = f"{self.client.endpoint}/api/2.1/jobs/list?limit={limit}"
        response = self.client.api("GET", target_url)
        jobs = response.get("jobs", list())

        found, job = search(jobs)
        if found:
            return job

        while response.get("has_more", False):
            offset += limit
            response = self.client.api("GET", f"{target_url}&offset={offset}")
            jobs = response.get("jobs", list())

            found, job = search(jobs)
            if found:
                return job

        return None

    def list_n(self, offset: int = 0, limit: int = 25, expand_tasks: bool = False):
        limit = min(25, limit)
        offset = max(0, offset)

        target_url = f"{self.client.endpoint}/api/2.1/jobs/list?offset={offset}&limit={limit}&expand_tasks={expand_tasks}"
        response = self.client.api("GET", target_url)
        return response.get("jobs", list())

    def list(self, expand_tasks: bool = False):
        offset = 0  # Start with zero
        limit = 25  # Default maximum

        target_url = f"{self.client.endpoint}/api/2.1/jobs/list?limit={limit}&expand_tasks={expand_tasks}"
        response = self.client.api("GET", target_url)
        all_jobs = response.get("jobs", list())

        while response.get("has_more", False):
            offset += limit
            response = self.client.api("GET", f"{target_url}&offset={offset}")
            all_jobs.extend(response.get("jobs", list()))

        return all_jobs

    @common.deprecated("Use JobsClient.delete_by_id() instead")
    def delete_by_job_id(self, job_id):
        self.delete_by_id(job_id)

    def delete_by_id(self, job_id):
        self.client.api("POST", f"{self.client.endpoint}/api/2.0/jobs/delete", job_id=job_id)

    def delete_by_name(self, job_names, success_only: bool):
        if type(job_names) == dict:
            job_names = list(job_names.keys())
        elif type(job_names) == list:
            job_names = job_names
        elif type(job_names) == str:
            job_names = [job_names]
        else:
            raise TypeError(f"Unsupported type: {type(job_names)}")

        # Get a list of all jobs
        jobs = self.list()

        s = "s" if len(jobs) != 1 else ""
        self.client.vprint(f"Found {len(jobs)} job{s}.")

        assert type(success_only) == bool, f"Expected \"success_only\" to be of type \"bool\", found \"{success_only}\"."
        self.client.vprint(f"...deleting successful jobs only: {success_only}")

        deleted = 0

        for job_name in job_names:
            for job in jobs:
                if job_name == job.get("settings").get("name"):
                    job_id = job.get("job_id")

                    runs = self.client.runs().list_by_job_id(job_id)
                    s = "s" if len(runs) != 1 else ""
                    self.client.vprint(f"Found {len(runs)} run{s} for job {job_id}")
                    delete_job = True

                    for run in runs:
                        state = run.get("state")
                        result_state = state.get("result_state", None)
                        life_cycle_state = state.get("life_cycle_state", None)

                        if success_only and life_cycle_state != "TERMINATED":
                            delete_job = False
                            print(f""" - The job "{job_name}" was not "TERMINATED" but "{life_cycle_state}", this job must be deleted manually""")
                        if success_only and result_state != "SUCCESS":
                            delete_job = False
                            print(f""" - The job "{job_name}" was not "SUCCESS" but "{result_state}", this job must be deleted manually""")

                    if delete_job:
                        self.client.vprint(f"...Deleting job #{job_id}, \"{job_name}\"")
                        for run in runs:
                            run_id = run.get("run_id")
                            self.client.vprint(f"""   - Deleting run #{run_id}""")
                            self.client.runs().delete(run_id)

                        self.delete_by_id(job_id)
                        deleted += 1

        self.client.vprint(f"...deleted {deleted} jobs")
