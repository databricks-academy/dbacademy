from typing import Union, Optional, Literal, List

from dbacademy.rest.common import DatabricksApiException, ApiContainer


class Runs(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks

    @staticmethod
    def _id(run: Union[int, dict]) -> int:
        if isinstance(run, dict):
            if "run_id" in run:
                run = run["run_id"]
            else:
                raise ValueError(f"Run definition missing run_id: {run!r}")
        if not isinstance(run, int):
            raise ValueError(
                f"DatabricksApi.jobs.runs._id(run): run must be run_id:int or run:dict.  Found: {run!r}.")
        return run

    def get(self, run: Union[int, dict], *, if_not_exists: str = "error") -> dict:
        run = self._id(run)
        return self.databricks.api("GET", "2.1/jobs/runs/get", data={"run_id": run})

    def get_output(self, run: Union[int, dict], *, if_not_exists: str = "error") -> dict:
        # Detect if it's a multi-task job with only 1 task.
        if isinstance(run, dict) and len(run.get("tasks", ())) == 1:
            run = run["tasks"][0]["run_id"]
        else:
            run = self._id(run)
        try:
            return self.databricks.api("GET", "2.1/jobs/runs/get-output", data={"run_id": run})
        except DatabricksApiException as e:
            if e.http_code == 400 and "Retrieving the output of runs with multiple tasks is not supported" in e.message:
                tasks = self.databricks.jobs.runs.get(run).get("tasks", ())
                if len(tasks) == 1:
                    return self.get_output(tasks[0])
            raise e

    def list(self,
             active_only: bool = False,
             completed_only: bool = False,
             job_id: int = None,
             offset: int = 0,
             limit: int = 25,
             run_type: Optional[
                 Union[Literal["JOB_RUN"], Literal["WORKFLOW_RUN"], Literal["SUBMIT_RUN"]]] = None,
             expand_tasks: bool = False,
             start_time_from: int = None,
             start_time_to: int = None,
             ) -> list:
        params = ("active_only", "completed_only", "job_id", "offset", "limit", "run_type", "expand_tasks",
                  "start_time_from", "start_time_to")
        values = locals()
        spec = {k: values[k] for k in params if values[k] is not None}
        return self.databricks.api("GET", "2.1/jobs/runs/list", data=spec).get("runs", [])

    def delete(self, run: Union[int, dict], *, if_not_exists: str = "error") -> dict:
        run = self._id(run)
        if if_not_exists == "ignore":
            return self.databricks.api("POST", "2.1/jobs/runs/delete", data={"run_id": run})
        else:
            try:
                return self.databricks.api("POST", "2.1/jobs/runs/delete", data={"run_id": run})
            except DatabricksApiException as e:
                raise e

    def cancel(self, run: Union[int, dict], *, if_not_exists: str = "error") -> dict:
        run = self._id(run)
        if if_not_exists == "ignore":
            return self.databricks.api("POST", "2.1/jobs/runs/cancel", data={"run_id": run})
        else:
            try:
                return self.databricks.api("POST", "2.1/jobs/runs/cancel", data={"run_id": run})
            except DatabricksApiException as e:
                raise e

    def delete_all(self, runs: List[Union[int, dict]] = None) -> list:
        if runs is None:
            if_not_exists = "ignore"
            runs = self.list()
        if not runs:
            return []
        from multiprocessing.pool import ThreadPool
        with ThreadPool(min(len(runs), 500)) as pool:
            pool.map(lambda run: self.delete(run, if_not_exists="ignore"), runs)

    def cancel_all(self, runs: List[Union[int, dict]] = None) -> list:
        if runs is None:
            if_not_exists = "ignore"
            runs = self.list()
        if not runs:
            return []
        from multiprocessing.pool import ThreadPool
        with ThreadPool(min(len(runs), 500)) as pool:
            pool.map(lambda run: self.cancel(run, if_not_exists="ignore"), runs)
