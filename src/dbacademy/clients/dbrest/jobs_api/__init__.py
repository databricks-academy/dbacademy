__all__ = ["JobsApi"]
# Code Review: JDP on 11-26-2023

from typing import Dict, Any, Optional, Union, List
from dbacademy.common import validate
from dbacademy.clients.rest.common import ApiClient, ApiContainer
from dbacademy.clients.dbrest.jobs_api.job_config import JobConfig


class JobsApi(ApiContainer):

    def __init__(self, client: ApiClient):
        self.__client = validate(client=client).required.as_type(ApiClient)
        self.base_uri = f"{self.__client.endpoint}/api/2.1/jobs"

    def create_from_config(self, config: JobConfig) -> str:
        config = validate(config=config).required.as_type(JobConfig)

        return self.create_from_dict(config.params)

    def create_from_dict(self, params: Dict[str, Any]) -> str:
        params = validate(params=params).required.dict(str)

        import json
        print("="*80)
        print(json.dumps(params, indent=4))
        print("="*80)

        response = self.__client.api("POST", f"{self.base_uri}/create", params)
        return response.get("job_id")

    def run_now(self, job_id: Union[int, str], notebook_params: Optional[Dict[str, Any]] = None):
        payload = {
            "job_id": validate(job_id=job_id).required.as_type(int, str)
        }
        if notebook_params is not None:
            payload["notebook_params"] = validate(notebook_params=notebook_params).required.dict(str)

        return self.__client.api("POST", f"{self.__client.endpoint}/api/2.0/jobs/run-now", payload)

    def get_by_id(self, job_id: Union[int, str]):
        validate(job_id=job_id).required.as_type(int, str)
        return self.__client.api("GET", f"{self.__client.endpoint}/api/2.0/jobs/get?job_id={job_id}")

    def get_by_name(self, job_name: str) -> Optional[Dict[str, Any]]:
        jobs = self.list(job_name=validate(job_name=job_name).required.str())
        if len(jobs) == 0:
            return None
        else:
            assert len(jobs) == 1, f"""Expected only one job by the name "{job_name}", found {len(jobs)}."""

        return jobs[0]

    def __list(self, *, limit: int, expand_tasks: bool, job_name: Optional[str], page_token: Optional[str]) -> Dict[str, Any]:
        limit = min(100, validate(limit=limit).required.int())
        expand_tasks = validate(expand_tasks=expand_tasks).required.bool()

        url = f"{self.base_uri}/list?limit={limit}&expand_tasks={expand_tasks}"

        if job_name is not None:
            url += f"&name={validate(job_name=job_name).required.str()}"

        if page_token is not None:
            url += f"&page_token={validate(page_token=page_token).required.str()}"

        return self.__client.api("GET", url)

    def list(self, *, limit: int = 100, expand_tasks: bool = False, job_name: Optional[str] = None) -> List[Dict[str, Any]]:
        response = self.__list(limit=limit, expand_tasks=expand_tasks, job_name=job_name, page_token=None)
        all_jobs = response.get("jobs", list())

        while response.get("has_more", False):
            page_token = response.get('next_page_token')
            response = self.__list(limit=limit, expand_tasks=expand_tasks, job_name=job_name, page_token=page_token)
            all_jobs.extend(response.get("jobs", list()))

        return all_jobs

    def delete_by_id(self, job_id: Union[str, int], *, skip_if_not_successful: bool = False, delete_by_name: bool = False) -> None:
        from dbacademy.clients import dbrest

        job_id = validate(job_id=job_id).required.as_type(int, str)
        job = self.get_by_id(job_id)

        m_prefix = " - " if delete_by_name else ""
        o_prefix = "   " if delete_by_name else ""

        if job is None:
            self.__client.vprint(f"""{m_prefix}Deleting job #{job_id}.""")
            self.__client.vprint(f"""{o_prefix} - Job not found.""")
            return

        delete_job = True
        job_name = job.get("settings", dict()).get("name")
        self.__client.vprint(f"""{m_prefix}Deleting job #{job_id}, "{job_name}".""")

        da_client = dbrest.from_client(self.__client)
        runs = da_client.runs.list_by_job_id(job_id)
        self.__client.vprint(f"""{o_prefix} - Found {len(runs)} job runs.""")
        self.__client.vprint(f"""{o_prefix} - deleting successful jobs only: {skip_if_not_successful}""")

        for run in runs:
            delete_run = True
            run_id = run.get("run_id")
            result_state = run.get("state").get("result_state")
            life_cycle_state = run.get("state").get("life_cycle_state")

            if life_cycle_state != "TERMINATED":
                delete_run = False
                self.__client.vprint(f"""{o_prefix} - Run #{run_id} of {job_name}'s life cycle state was not "TERMINATED" but "{life_cycle_state}", this job must be deleted manually as it's still running.""")

            if validate(skip_if_not_successful=skip_if_not_successful).required.bool() and result_state != "SUCCESS":
                delete_run = False
                self.__client.vprint(f"""{o_prefix} - Run #{run_id} of {job_name}'s result state was not "SUCCESS" but "{result_state}", this job must be deleted manually.""")

            if delete_run:
                da_client.runs.delete_by_id(run_id)

            # Don't delete the job if we are not deleting the run.
            delete_job = delete_job and delete_run

        if delete_job:
            self.__client.api("POST", f"{self.__client.endpoint}/api/2.0/jobs/delete", job_id=job_id)
            self.__client.vprint(f"{o_prefix} - Deleted.")
        else:
            self.__client.vprint(f"{o_prefix} - Not deleted.")

    def delete_by_name(self, job_names: Union[list, dict, str], *, skip_if_not_successful: bool) -> None:
        # Verify that job_names is one of these three types
        validate(job_names=job_names).required.as_type(list, dict, str)

        if isinstance(job_names, dict):
            job_names: List[str] = list(job_names.keys())
        elif isinstance(job_names, list):
            job_names: List[str] = job_names
        elif isinstance(job_names, str):
            job_names: List[str] = [job_names]
        else:
            import inspect
            raise NotImplementedError(f"{self.__class__.__name__}.{inspect.stack()[0].function}(..) is not implemented for job_names of type {type(job_names)}")

        # Get a list of all jobs
        all_jobs = list()

        for job_name in validate(job_names=job_names).required.list(str):
            all_jobs.extend(self.list(job_name=job_name))

        s = "s" if len(all_jobs) != 1 else ""
        self.__client.vprint(f"Found {len(all_jobs)} job{s}.")

        for job in all_jobs:
            job_id = job.get("job_id")
            self.delete_by_id(job_id,
                              skip_if_not_successful=skip_if_not_successful,
                              delete_by_name=True)

    @classmethod
    def __pause_status_for(cls, schedule: Dict[str, Any], paused: bool):
        return schedule.get("pause_status") if validate(paused=paused).optional.bool() is None else ("PAUSED" if paused else "UNPAUSED")

    def update_schedule(self, *,
                        job_id: Union[int, str],
                        paused: Optional[bool],
                        quartz_cron_expression: Optional[str],
                        timezone_id: Optional[str]) -> Dict[str, Any]:

        job = self.get_by_id(validate(job_id=job_id).required.as_type(int, str))
        settings = job.get("settings")
        schedule = settings.get("schedule")

        payload = {
            "job_id": job_id,
            "new_settings": {
                "schedule": {
                    "timezone_id": timezone_id or schedule.get("timezone_id"),
                    "quartz_cron_expression": quartz_cron_expression or schedule.get("quartz_cron_expression"),
                    "pause_status": self.__pause_status_for(schedule, paused),
                }
            }
        }
        self.__client.api("POST", f"{self.base_uri}/update", payload)
        return self.get_by_id(job_id)

    def update_continuous(self, *,
                          job_id: Union[int, str],
                          paused: Optional[bool]) -> Dict[str, Any]:

        job = self.get_by_id(validate(job_id=job_id).required.as_type(int, str))
        settings = job.get("settings")
        continuous = settings.get("continuous")

        payload = {
            "job_id": job_id,
            "new_settings": {
                "continuous": {
                    "pause_status": self.__pause_status_for(continuous, paused)
                }
            }
        }
        self.__client.api("POST", f"{self.base_uri}/update", payload)
        return self.get_by_id(job_id)

    def update_trigger(self, *,
                       job_id: Union[int, str],
                       paused: Optional[bool],
                       url: Optional[str],
                       min_time_between_triggers_seconds: Optional[int],
                       wait_after_last_change_seconds: Optional[int]) -> Dict[str, Any]:

        job = self.get_by_id(validate(job_id=job_id).required.as_type(int, str))
        settings = job.get("settings")
        trigger = settings.get("trigger")
        file_arrival = trigger.get("file_arrival")

        url = validate(url=url or file_arrival.get("url")).required.str()
        min_time_between_triggers_seconds = validate(min_time_between_triggers_seconds=min_time_between_triggers_seconds or file_arrival.get("min_time_between_triggers_seconds")).required.str()
        wait_after_last_change_seconds = validate(wait_after_last_change_seconds=wait_after_last_change_seconds or file_arrival.get("wait_after_last_change_seconds")).required.str()

        payload = {
            "job_id": job_id,
            "new_settings": {
                "trigger": {
                    "pause_status": self.__pause_status_for(trigger, paused),
                    "file_arrival": {
                        "url": url,
                        "min_time_between_triggers_seconds": min_time_between_triggers_seconds,
                        "wait_after_last_change_seconds": wait_after_last_change_seconds,
                    }
                }
            }
        }
        self.__client.api("POST", f"{self.base_uri}/update", payload)
        return self.get_by_id(job_id)
