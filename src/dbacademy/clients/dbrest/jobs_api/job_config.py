__all__ = ["JobConfig"]
# Code Review: JDP on 11-26-2023

from typing import Dict, List, Optional, Any
from dbacademy.common import validate
from dbacademy.clients.dbrest.jobs_api.task_config import TaskConfig


class JobConfig:

    def __init__(self, *,
                 job_name: str,
                 timeout_seconds: int = 300,
                 max_concurrent_runs: int = 1,
                 tags: Optional[Dict[str, str]] = None):

        self.__tasks: List[Dict[str, Any]] = list()

        self.params = {
            "name": validate(job_name=job_name).required.str(),
            "tags": validate(tags=tags).optional.dict(str, str, auto_create=True),
            "timeout_seconds": validate(timeout_seconds=timeout_seconds).required.int(),
            "max_concurrent_runs": validate(max_concurrent_runs=max_concurrent_runs).required.int(),
            "format": "MULTI_TASK",
            "tasks": self.__tasks,
        }

    def git_branch(self, *, provider: str, url: str, branch: str):
        self.params["git_source"] = {
            "git_provider": validate(provider=provider).required.str(),
            "git_url": validate(url=url).required.str(),
            "git_branch": validate(branch=branch).required.str()
        }

    def git_tag(self, *, provider: str, url: str, tag: str):
        self.params["git_source"] = {
            "git_provider": validate(provider=provider).required.str(),
            "git_url": validate(url=url).required.str(),
            "git_tag": validate(tag=tag).required.str()
        }

    def git_commit(self, *, provider: str, url: str, commit: str):
        self.params["git_source"] = {
            "git_provider": validate(provider=provider).required.str(),
            "git_url": validate(url=url).required.str(),
            "git_commit": validate(commit=commit).required.str()
        }

    def add_task(self, *,
                 task_key: str,
                 description: Optional[str] = None,
                 max_retries: int = 0,
                 min_retry_interval_millis: int = 0,
                 retry_on_timeout: bool = False,
                 timeout_seconds: Optional[int] = None,
                 depends_on: Optional[List[str]] = None) -> TaskConfig:

        # These are validated in the TaskConfig constructor
        task_config = TaskConfig(job_params=self.params,
                                 task_key=task_key,
                                 description=description,
                                 max_retries=max_retries,
                                 min_retry_interval_millis=min_retry_interval_millis,
                                 retry_on_timeout=retry_on_timeout,
                                 timeout_seconds=timeout_seconds,
                                 depends_on=depends_on)

        self.params["tasks"].append(task_config.params)
        return task_config

    def add_email_notifications(self, *,
                                on_start: Optional[List[str]],
                                on_success: Optional[List[str]],
                                on_failure: Optional[List[str]],
                                on_duration_warning_threshold_exceeded: Optional[List[str]],
                                no_alert_for_skipped_runs: bool = False) -> None:

        self.params["email_notifications"] = {
            "on_start": validate(on_start=on_start).optional.list(str, auto_create=True),
            "on_success": validate(on_success=on_success).optional.list(str, auto_create=True),
            "on_failure": validate(on_failure=on_failure).optional.list(str, auto_create=True),
            "on_duration_warning_threshold_exceeded": validate(on_duration_warning_threshold_exceeded=on_duration_warning_threshold_exceeded).optional.list(str, auto_create=True),
            "no_alert_for_skipped_runs": validate(no_alert_for_skipped_runs=no_alert_for_skipped_runs).required.bool(),
        }

    def add_webhook_notifications(self, *,
                                  on_start: Optional[List[str]],
                                  on_success: Optional[List[str]],
                                  on_failure: Optional[List[str]],
                                  on_duration_warning_threshold_exceeded: Optional[List[str]]) -> None:

        self.params["webhook_notifications"] = {
            "on_start": validate(on_start=on_start).optional.list(str, auto_create=True),
            "on_success": validate(on_success=on_success).optional.list(str, auto_create=True),
            "on_failure": validate(on_failure=on_failure).optional.list(str, auto_create=True),
            "on_duration_warning_threshold_exceeded": validate(on_duration_warning_threshold_exceeded=on_duration_warning_threshold_exceeded).optional.list(str, auto_create=True),
        }
