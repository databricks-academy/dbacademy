__all__ = ["JobConfig"]

from typing import Dict, List


class JobConfig:
    from dbacademy.clients.darest.jobs_api.task_config import TaskConfig

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
        from dbacademy.clients.darest.jobs_api.task_config import TaskConfig

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
