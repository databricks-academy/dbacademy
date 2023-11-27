__all__ = ["Result", "Watchdog"]

import os
from typing import List, Dict, Any, Optional, Literal, Union
from dbacademy.clients.dbrest import DBAcademyRestClient

# noinspection PyPep8Naming
DISABLED = False

_found_users = {
    "jacob.parr@databricks.com": False
}


def _err(_value: str) -> str:
    return "" if _value is None else f"[{_value}]"


def _eval(failures: List[str], _failed: bool, _label: str, _passed: bool) -> (bool, str):
    if _passed:
        return _failed, None
    else:
        failures.append(_label)
        return True, _label


class Result:

    RESULT_TYPE = Literal["INFO", "WARNING", "ERROR"]

    def __init__(self, *, _result_type: RESULT_TYPE, _workspace_name: str, _workspace_endpoint: str, _message: str, _scope: str = None, _failures: List[str] = None):
        self.result_type = _result_type
        self.message = _message.strip()
        self.workspace_name = _workspace_name
        self.workspace_endpoint = _workspace_endpoint

        self.failures = list()
        for failure in list() if _failures is None else _failures:
            self.failures.append(failure if _scope is None else f"{_scope}-{failure}")

    def __str__(self) -> str:
        string = f"{self.result_type} {self.workspace_endpoint}\n"
        string += f"{self.message}\n"

        if self.result_type == "ERROR":
            string += "-"*80
            string += "\n"

        return string


class Watchdog:
    def __init__(self):
        from dbacademy.common import Cloud
        from dbacademy.clients.dbrest import accounts_client

        self.__results: List[Result] = list()

        self.__workspace_client: Optional[DBAcademyRestClient] = None
        self.__workspace: Optional[Dict[str, Any]] = None

        self.__username = os.environ.get("WORKSPACE_SETUP_PROSVC_USERNAME")
        self.__password = os.environ.get("WORKSPACE_SETUP_PROSVC_PASSWORD")

        self.accounts_client = accounts_client.from_args(cloud=Cloud.AWS,
                                                         account_id=os.environ.get("WORKSPACE_SETUP_PROSVC_ACCOUNT_ID"),
                                                         username=self.username,
                                                         password=self.password)

    @property
    def username(self) -> str:
        return self.__username

    @property
    def password(self) -> str:
        return self.__password

    @property
    def workspace_client(self) -> DBAcademyRestClient:
        return self.__workspace_client

    @property
    def workspace(self) -> Optional[Dict[str, Any]]:
        return self.__workspace

    @property
    def workspace_name(self) -> str:
        return self.workspace.get("workspace_name")

    @property
    def workspace_domain(self) -> str:
        if self.workspace_name == "survey-dashboards":
            return "training-surveys"
        elif self.workspace_name == "trainers":
            return "training"
        else:
            return f"training-{self.workspace_name}"

    @property
    def workspace_endpoint(self) -> str:
        return f"https://{self.workspace_domain}.cloud.databricks.com"

    def __analyse_serving_endpoints(self):
        modern_endpoints = self.workspace_client.serving_endpoints.list()
        mlflow_endpoints = self.workspace_client.ml.mlflow_endpoints.list()
        if len(modern_endpoints) > 0 or len(mlflow_endpoints) > 0:
            self.__log_error(f"Serving Endpoints: {len(modern_endpoints)} ({len(mlflow_endpoints)})", "ML-SERVING-RUNNING")

    def ___analyse_workflows(self, _pause: bool):
        from datetime import datetime

        max_hours = 4
        jobs = self.workspace_client.jobs.list(expand_tasks=True)

        for job in jobs:
            job_id = job.get("job_id")
            creator = job.get("creator_user_name")

            created_time_ep = job.get("created_time") / 1000
            created_time = datetime.fromtimestamp(created_time_ep)
            created_duration = (datetime.now() - created_time)
            hours = (created_duration.days * 24) + (created_duration.seconds / 60 / 60)

            settings = job.get("settings", dict())
            name = settings.get("name")
            job_format = settings.get("format")
            run_as = settings.get("run_as")

            trigger = settings.get("trigger")
            trigger_paused = None if trigger is None else trigger.get("pause_status")

            continuous = settings.get("continuous")
            continuous_paused = None if continuous is None else continuous.get("pause_status")

            schedule = settings.get("schedule")
            schedule_paused = None if schedule is None else schedule.get("pause_status")

            if name in ["DBAcademy Workspace-Setup"]:
                continue
            elif self.workspace_name == "trainers" and name in ["DBAcademy Workspace-Setup"]:
                continue
            elif self.workspace_name == "survey-dashboards" and name in ["daily_refresh_of_DLT"]:
                continue

            failed = False
            failures = list()

            if hours > max_hours:
                # All jobs should be canceled within N hours.
                failed, schedule_failure = _eval(failures, failed, "SCHEDULED", schedule_paused is None or schedule_paused == "PAUSED")
                failed, continuous_failure = _eval(failures, failed, "CONTINUOUS", continuous_paused is None or continuous_paused == "PAUSED")
                failed, trigger_failure = _eval(failures, failed, "TRIGGER", trigger_paused is None or trigger_paused == "PAUSED")
            else:
                schedule_failure = None
                continuous_failure = None
                trigger_failure = None

            message = F"\nScheduled Job > {max_hours} hours"
            message += f"""\n  | Name:       {name}"""
            message += f"""\n  | Creator:    {creator}"""
            message += f"""\n  | Created:    {created_time} ({hours:.3} hours)"""
            message += f"""\n  | Format:     {job_format}"""
            message += f"""\n  | Run As:     {run_as}"""
            message += f"""\n  | Trigger:    {trigger_paused} {_err(trigger_failure)}"""
            message += f"""\n  |             {trigger}"""
            message += f"""\n  | Continuous: {continuous_paused} {_err(continuous_failure)}"""
            message += f"""\n  |             {continuous}"""
            message += f"""\n  | Schedule:   {schedule_paused} {_err(schedule_failure)}"""
            message += f"""\n  |             {schedule}"""

            if schedule_failure is not None and _pause:
                message += f"\n  | Paused schedule."
                self.workspace_client.jobs.update_schedule(job_id=job_id,
                                                           paused=True,
                                                           quartz_cron_expression=None,
                                                           timezone_id=None)
            if continuous_failure is not None and _pause:
                message += f"\n  | Paused continuous."
                self.workspace_client.jobs.update_continuous(job_id=job_id, paused=True)

            if trigger_paused is not None and _pause:
                message += f"\n  | Paused trigger."
                self.workspace_client.jobs.update_trigger(job_id=job_id,
                                                          paused=True,
                                                          url=None,
                                                          min_time_between_triggers_seconds=None,
                                                          wait_after_last_change_seconds=None)

            if failed:
                self.__log_error(message, _scope="JOBS", _failures=failures)

    def __analyse_clusters(self, _terminate: bool):
        from datetime import datetime
        from dbacademy.dbhelper import dbh_constants

        clusters = [c for c in self.workspace_client.clusters.list() if c.get("state") not in ["TERMINATED"]]
        if len(clusters) > 0:
            for cluster in clusters:
                cluster_name = cluster.get("cluster_name")
                state = cluster.get("state")
                creator_username = cluster.get("creator_user_name")
                single_username = cluster.get("single_user_name")
                node_type_id = cluster.get("node_type_id")
                autotermination_minutes = cluster.get("autotermination_minutes")
                num_workers = cluster.get("num_workers")
                cluster_source = cluster.get("cluster_source")
                policy_id = cluster.get("policy_id")
                policy = None if policy_id is None else self.workspace_client.cluster_policies.get_by_id(policy_id)
                policy_name = None if policy is None else policy.get("name")

                restarted_time_ep = cluster.get("last_restarted_time") / 1000
                restarted_time = datetime.fromtimestamp(restarted_time_ep)
                restarted_duration = (datetime.now() - restarted_time)
                hours = (restarted_duration.days * 24) + (restarted_duration.seconds / 60 / 60)

                failed = False
                failures = list()

                failed, hours_failure = _eval(failures, failed, "LONG-RUNNING", hours < 9)
                failed, num_workers_failure = _eval(failures, failed, "NON-ZERO-WORKERS", num_workers == 0)
                failed, policy_failure = _eval(failures, failed, "POLICY-VIOLATION", policy_name in dbh_constants.CLUSTERS_HELPER.POLICIES)
                failed, node_type_failure = _eval(failures, failed, "NODE-TYPE", node_type_id == "i3.xlarge")
                failed, auto_term_failure = _eval(failures, failed, "AUTO-TERMINATION", autotermination_minutes <= 120)

                message = ""
                message += f"""\n{state[0]}{state[1:].lower()} Cluster"""
                message += f"""\n  | Cluster:   {cluster_name}"""
                message += f"""\n  | Started:   {restarted_duration} ({hours:.3} hours) {_err(hours_failure)}"""
                message += f"""\n  | Creator:   {creator_username}"""
                message += f"""\n  | Username:  {single_username}"""
                message += f"""\n  | Node Type: {node_type_id} {_err(node_type_failure)}"""
                message += f"""\n  | Auto Term: {autotermination_minutes} minutes {_err(auto_term_failure)}"""
                message += f"""\n  | Workers:   {num_workers} {_err(num_workers_failure)}"""
                message += f"""\n  | Source:    {cluster_source}"""
                message += f"""\n  | Policy:    {policy_name} {_err(policy_failure)}"""

                if _terminate:
                    message += f"\n  | Terminating cluster {cluster_name}"
                    # client.clusters.terminate_by_id(cluster_id)
                else:
                    message += f"\n  | CLUSTER TERMINATION ABORTED"

                if failed:
                    self.__log_error(message, _scope="CLUSTERS", _failures=failures)
                elif hours < 2:
                    self.__log_info(f"""\n{state[0]}{state[1:].lower()} Cluster: "{cluster_name}" ({hours:.3} hours)""")
                elif hours < 9:
                    self.__log_warning(message)
                else:
                    failures.append("UNKNOWN")
                    self.__log_error(message, _scope="CLUSTERS", _failures=failures)

    def __analyse_users(self, _add_missing_users: bool):
        import copy

        found_users = copy.deepcopy(_found_users)
        users = self.workspace_client.scim.users.list()

        for user in users:
            username = user.get("userName")

            if username in found_users.keys():
                found_users[username] = True

            elif not username.endswith("@databricks.com") and self.workspace_name not in ["trainers"]:
                self.__log_error(f"Unauthorized user: {username}", _failures=["USERS-NOT-DB"])

        for username, found in found_users.items():
            if not found and _add_missing_users:
                self.__log_info(f"Added user {username}")
                user = self.workspace_client.scim.users.create(username)
                user_id = user.get("id")

                admins = self.workspace_client.scim.groups.get_by_name("admins")
                admin_id = admins.get("id")
                self.workspace_client.scim.groups.add_member(admin_id, user_id)

    def __analyse_workspace(self, *,
                            _workspace: Dict[str, Any],
                            _analyse_users: bool,
                            _analyse_serving_endpoints: bool,
                            _analyse_workflows: bool,
                            _pause_workflow: bool,
                            _analyse_clusters: bool,
                            _terminate_clusters: bool):

        from dbacademy.clients import dbrest
        self.__workspace = _workspace
        self.__workspace_client = dbrest.from_args(endpoint=self.workspace_endpoint, username=self.__username, password=self.__password)

        workspace_name = _workspace.get("workspace_name")
        print(f"* Processing workspace {workspace_name}")

        if _analyse_users:
            self.__analyse_users(_add_missing_users=True)

        if _analyse_serving_endpoints:
            self.__analyse_serving_endpoints()

        if _analyse_workflows:
            self.___analyse_workflows(_pause=_pause_workflow)

        if _analyse_clusters:
            self.__analyse_clusters(_terminate=_terminate_clusters)

    def __log_error(self, _message: str, _failures: Union[str, List[str]], _scope: str = None) -> None:
        if type(_failures) is str:
            _failures = [_failures]

        result = Result(_result_type="ERROR",
                        _workspace_name=self.workspace_name,
                        _workspace_endpoint=self.workspace_endpoint,
                        _message=_message,
                        _scope=_scope,
                        _failures=_failures)

        self.__results.append(result)
        print(result)

    def __log_warning(self, _message: str) -> None:
        result = Result(_result_type="WARNING",
                        _workspace_name=self.workspace_name,
                        _workspace_endpoint=self.workspace_endpoint,
                        _message=_message)

        self.__results.append(result)
        print(result)

    def __log_info(self, message: str) -> None:
        result = Result(_result_type="INFO",
                        _workspace_name=self.workspace_name,
                        _workspace_endpoint=self.workspace_endpoint,
                        _message=message)

        self.__results.append(result)
        print(result)

    def analyse(self) -> None:
        print()

        count = 0
        workspaces = self.accounts_client.workspaces.list()
        workspace_filter = None  # ["classroom-868-83vgw"]

        for workspace in workspaces:
            count += 1
            workspace_name = workspace.get("workspace_name")
            if workspace_filter and workspace_name not in workspace_filter:
                continue

            self.__analyse_workspace(_workspace=workspace,
                                     _analyse_users=True,
                                     _analyse_serving_endpoints=True,
                                     _analyse_workflows=True,
                                     _pause_workflow=True,
                                     _analyse_clusters=True,
                                     _terminate_clusters=True)
        print(f"Processed {count} workspaces")


Watchdog().analyse()
