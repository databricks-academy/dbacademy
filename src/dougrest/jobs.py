from typing import Union, Dict, List, Any, Optional

from dbacademy.rest.common import DatabricksApiException, ApiContainer
from dbacademy.dougrest.runs import Runs


class Jobs(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks
        self.runs = Runs(databricks)

    def _id(self, job, *, if_not_exists="error"):
        if isinstance(job, dict):
            if "job_id" in job:
                job = job["job_id"]
            elif "name" in job:
                job = job["name"]
            else:
                raise ValueError(f"Job dict must have job_id or name: {job!r}")
        if isinstance(job, str):
            existing_jobs = self.get_all(job)
            if len(existing_jobs) == 0:
                if if_not_exists == "error":
                    raise DatabricksApiException(f"No jobs with name={job!r} found.", 404)
                elif if_not_exists == "ignore":
                    return None
                else:
                    raise ValueError(
                        f"if_not_exists argument must be either 'ignore' or 'error'.  Found if_not_exists={if_not_exists!r}.")
            elif len(existing_jobs) == 1:
                job = existing_jobs[0]["job_id"]
            else:
                raise DatabricksApiException(f"Ambiguous command.  Multiple jobs with name={job!r} found.", 400)
        if isinstance(job, int):
            return job
        raise ValueError(
            f"DatabricksApi.jobs.delete(job): job must be id:int, name:str, job:dict.  Found: {job!r}.")

    def get(self, job, *, return_list=False, if_not_exists="error"):
        if isinstance(job, dict):
            if "job_id" in job:
                job = job["job_id"]
            elif "name" in job:
                job = job["name"]
            else:
                raise ValueError(f"Job dict must have job_id or name: {job!r}")
        if isinstance(job, str):
            existing_jobs = self.get_all(job)
            if len(existing_jobs) == 0:
                if if_not_exists == "error":
                    raise DatabricksApiException(f"No jobs with name={job!r} found.", 404)
                elif if_not_exists == "ignore":
                    return existing_jobs if return_list else None
                else:
                    raise ValueError(
                        f"if_not_exists argument must be either 'ignore' or 'error'.  Found if_not_exists={if_not_exists!r}.")
            elif return_list:
                return existing_jobs
            elif len(existing_jobs) == 1:
                return existing_jobs[0]
            else:
                raise DatabricksApiException(f"Ambiguous command.  Multiple jobs with name={job!r} found.", 400)
        if isinstance(job, int):
            job = self.databricks.api("GET", "2.1/jobs/get", data={"job_id": job})
            return [job] if return_list else job
        raise ValueError(
            f"DatabricksApi.jobs.delete(job): job must be id:int, name:str, job:dict.  Found: {job!r}.")

    def list(self):
        offset=0
        while True:
            response = self.databricks.api_simple("GET", "2.1/jobs/list", limit=25, offset=offset)
            jobs = response.get('jobs', [])
            yield from jobs
            offset += len(jobs)
            if not response["has_more"]:
                return

    def list_by_name(self):
        results = {}
        for job in self.list():
            results.setdefault(job["settings"]["name"], []).append(job)
        return results

    def list_names(self):
        return self.list_by_name().keys()

    def get_all(self, name):
        return self.list_by_name().get(name, [])

    def exists(self, job):
        job = self.get(job, return_list=True, if_not_exists="ignore")
        return bool(job)

    def update(self, job):
        return self.databricks.api("POST", "2.1/jobs/update", job)

    def delete(self, job, *, if_not_exists="error"):
        if isinstance(job, str):
            existing_jobs = self.get_all(job)
            if not existing_jobs and if_not_exists == "error":
                raise DatabricksApiException(f"No jobs with name={job!r} found.", 404)
            for j in existing_jobs:
                self.delete(j)
            return len(existing_jobs)
        if isinstance(job, dict):
            job = job["job_id"]
        if not isinstance(job, int):
            raise ValueError(
                f"DatabricksApi.jobs.delete(job): job must be id:int, name:str, job:dict.  Found: {job!s}.")
        if if_not_exists == "error":
            self.databricks.api("POST", "2.1/jobs/delete", data={"job_id": job})
            return 1
        elif if_not_exists == "ignore":
            try:
                self.databricks.api("POST", "2.1/jobs/delete", data={"job_id": job})
                return 1
            except DatabricksApiException as e:
                if e.http_code == 404 and e.error_code == 'RESOURCE_DOES_NOT_EXIST':
                    return 0
                elif e.http_code == 400 and e.error_code == 'INVALID_PARAMETER_VALUE' and "does not exist" in e.message:
                    return 0
                else:
                    raise e
        else:
            raise ValueError(
                f"if_not_exists argument must be either 'ignore' or 'error'.  Found if_not_exists={if_not_exists!r}.")

    def create_single_task_job(self, name, *, notebook_path=None, timeout_seconds=None, max_concurrent_runs=1,
                               new_cluster=None, existing_cluster_id=None, if_exists="proceed", **args):
        task = {"task_key": name}
        if notebook_path:
            task["notebook_task"] = {"notebook_path": notebook_path}
        if new_cluster:
            task["new_cluster"] = new_cluster
        elif existing_cluster_id:
            task["existing_cluster_id"] = existing_cluster_id
        else:
            task["new_cluster"] = {
                "num_workers": 0,
                "spark_version": self.databricks.default_spark_version,
                "spark_conf": {"spark.master": "local[*]"},
                "node_type_id": self.databricks.default_machine_type,
            }
        return self.create_multi_task_job(name, [task],
                                          max_concurrent_runs=max_concurrent_runs,
                                          new_cluster=new_cluster,
                                          if_exists=if_exists, **args)

    def create_multi_task_job(self, name, tasks, *, max_concurrent_runs=1, new_cluster=None, if_exists="proceed",
                              **args):
        spec = {
            "name": name,
            "max_concurrent_runs": 1,
            "format": "MULTI_TASK",
            "tasks": tasks,
        }
        spec.update(**args)
        if if_exists == "overwrite":
            self.delete(name, if_not_exists="ignore")
            return self.databricks.api("POST", "2.1/jobs/create", data=spec)["job_id"]
        elif not self.exists(name) or if_exists == "proceed":
            return self.databricks.api("POST", "2.1/jobs/create", data=spec)["job_id"]
        elif if_exists == "ignore":
            return None
        elif if_exists == "error":
            raise DatabricksApiException(f"Job with name={name!r} already exists", 409)
        else:
            raise ValueError(
                f"if_exists argument must be one of 'ignore', 'proceed', or 'error'.  Found if_exists={if_exists!r}.")

    def run(self,
            job: Union[int, str, dict],
            idempotency_token: str = None,
            notebook_params: Dict[str, str] = None,
            jar_params: List[str] = None,
            python_params: List[str] = None,
            spark_submit_params: List[str] = None,
            python_named_params: Dict[str, Any] = None,
            if_not_exists: str = "error"
            ) -> Optional[dict]:
        job = self._id(job)
        spec = {"job_id": job}
        vars = locals()
        for param in ("idempotency_token", "notebook_params", "jar_params", "python_params", "spark_submit_params",
                      "python_named_params"):
            if vars[param]:
                spec[param] = vars[param]
        if if_not_exists == "error":
            return self.databricks.api("POST", "2.1/jobs/run-now", data=spec)
        elif if_not_exists == "ignore":
            try:
                return self.databricks.api("POST", "2.1/jobs/run-now", data=spec)
            except DatabricksApiException as e:
                if e.http_code == 404 and e.error_code == 'RESOURCE_DOES_NOT_EXIST':
                    return None
                elif e.http_code == 400 and e.error_code == 'INVALID_PARAMETER_VALUE' and "does not exist" in e.message:
                    return None
                else:
                    raise e
        else:
            raise ValueError(
                f"if_not_exists argument must be one of 'ignore', 'error', or 'create'.  Found if_not_exists={if_not_exists!r}.")
