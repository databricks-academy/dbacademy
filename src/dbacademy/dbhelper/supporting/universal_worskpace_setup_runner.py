__all__ = ["UniversalWorkspaceSetupRunner"]

from typing import Dict, Any, Optional
from dbacademy.clients.dbrest import DBAcademyRestClient
from dbacademy.dbhelper.course_config import CourseConfig

UWS_CONFIG_PATH = "dbfs:/mnt/dbacademy/uws.json"


class UniversalWorkspaceSetupRunner:

    def __init__(self, *, course_config: CourseConfig, token: str = None, endpoint: str = None, workspace_name: str = None):
        from dbacademy import dbgems
        from dbacademy.common import Cloud
        from dbacademy.clients import dbrest
        from dbacademy.common import validate

        self.__event_id = 0
        self.__event_description = f"Workspace {workspace_name}"
        self.__default_spark_version = course_config.supported_dbrs[0]

        self.__course_config = validate(course_config=course_config).required.as_type(CourseConfig)

        self.__workspace_name = validate(workspace_name=workspace_name or dbgems.sc.getConf().get("spark.databricks.workspaceUrl", defaultValue="Unknown")).required.str()

        self.__client = dbrest.from_token(token=validate(token=token or dbgems.get_notebooks_api_token()).required.str(),
                                          endpoint=validate(endpoint=endpoint or dbgems.get_notebooks_api_endpoint()).required.str())

        if Cloud.current_cloud().is_aws:
            self.__pools_node_type_id = "i3.xlarge"
        elif Cloud.current_cloud().is_msa:
            self.__pools_node_type_id = "Standard_D4ds_v4"
        elif Cloud.current_cloud().is_gcp:
            self.__pools_node_type_id = "n2-highmem-4"
        else:
            raise ValueError(f"The cloud {Cloud.current_cloud()} is not supported.")

    @property
    def course_config(self) -> CourseConfig:
        return self.__course_config

    @property
    def event_id(self) -> int:
        return self.__event_id

    @property
    def workspace_name(self) -> str:
        return self.__workspace_name

    @property
    def client(self) -> DBAcademyRestClient:
        return self.__client

    @property
    def event_description(self) -> str:
        return self.__event_description

    @property
    def default_spark_version(self) -> str:
        return self.__default_spark_version

    @property
    def pools_node_type_id(self) -> str:
        return self.__pools_node_type_id

    @classmethod
    def uws_read(cls) -> Optional[Dict[str, Any]]:
        import os, json

        uws_config_path = UWS_CONFIG_PATH.replace("dbfs:/", "/dbfs/")
        if not os.path.exists(uws_config_path):
            return None

        with open(uws_config_path, "r") as f:
            config_json = f.read()
            return json.loads(config_json)

    @classmethod
    def uws_status(cls, status: str) -> None:
        cls.uws_append_message(f"Status >> {status}")
        uws = cls.uws_read()
        uws["status"] = status
        cls.uws_write(uws)

    @classmethod
    def uws_write(cls, uws: Dict[str, Any]) -> None:
        import json

        uws_config_path = UWS_CONFIG_PATH.replace("dbfs:/", "/dbfs/")

        with open(uws_config_path, "w") as f:
            f.write(json.dumps(uws, indent=4))

    @classmethod
    def uws_reset(cls) -> None:

        cls.uws_write({"status": "UNKNOWN", "log": []})
        cls.uws_status("STARTED")

    @classmethod
    def uws_append_message(cls, message: str) -> None:
        print(f"| {message}")

        uws = cls.uws_read() or dict()
        uws["log"] = uws.get("log", list())
        i = len(uws["log"])
        uws.get("log").append(f"{i+1:02d} - {message}")

        cls.uws_write(uws)

    def run(self):
        import time
        from dbacademy.dbhelper import dbh_constants

        start = time.time()
        print("Running the Universal Workspace Setup job...")

        self.uws_reset()

        self.delete_job()

        job_id = self.create_job()

        run = self.client.jobs.run_now(job_id)
        run_id = run.get("run_id")
        self.uws_append_message(f"Started Run {run_id}")

        self.uws_append_message(f"Blocking On Run {run_id}")
        response = self.wait_for_job_run(job_id, run_id)
        # self.append_log_message(f"Run {run_id}: Completed")

        # The job has completed, now we need to evaluate the final state.
        state = response.get("state", dict())
        state_message = state.get("state_message", "Unknown")
        if state is None:
            raise AssertionError("The job's state object is missing.")

        life_cycle_state = state.get("life_cycle_state")
        if life_cycle_state == "SKIPPED":
            self.uws_append_message(f"Run {run_id}: Skipped")
            return

        elif life_cycle_state != "TERMINATED":
            self.uws_append_message(f"Run {run_id}: Failed, {life_cycle_state}")
            raise Exception(f"""Expected the final life cycle state of {dbh_constants.WORKSPACE_HELPER.UNIVERSAL_WORKSPACE_SETUP} to be "TERMINATED", found "{life_cycle_state}" for "{self.workspace_name}" | {state_message}""")

        else:
            result_state = state.get("result_state")
            if result_state != "SUCCESS":
                self.uws_append_message(f"Run {run_id}: Failed, {life_cycle_state}-{result_state}")
                raise Exception(f"""Expected the final state of {dbh_constants.WORKSPACE_HELPER.UNIVERSAL_WORKSPACE_SETUP} to be "SUCCESS", found "{result_state}" for "{self.workspace_name}" | {state_message}""")
            else:
                self.uws_append_message(f"Run {run_id}: Success")
                self.uws_status("COMPLETED")

        duration = int((time.time() - start) / 60)
        self.uws_append_message(f"Duration: {duration} minutes")
        print(f"""Finished {dbh_constants.WORKSPACE_HELPER.UNIVERSAL_WORKSPACE_SETUP} (job #{job_id}, run #{run_id}) for "{self.workspace_name}" ({duration} minutes).""")

    def delete_job(self) -> None:
        from dbacademy.dbhelper import dbh_constants
        job = self.client.jobs.get_by_name(dbh_constants.WORKSPACE_HELPER.WORKSPACE_SETUP_JOB_NAME)

        if job is None:
            self.uws_append_message("Delete Job: Skipped")
        else:
            job_id = job.get("job_id")
            self.client.jobs.delete_by_id(job_id)
            self.uws_append_message(f"Deleted Job {job_id}")

    def create_job(self) -> str:
        import requests, json

        config_text = requests.get("https://raw.githubusercontent.com/databricks-academy/workspace-setup/main/universal-workspace-setup-job-config.json").text
        config_text = config_text.replace("{{event_id}}", f"{self.event_id:03d}")
        config_text = config_text.replace("{{event_description}}", self.event_description)
        config_text = config_text.replace("{{deployment_context}}", f"Learning Platforms Workspace {self.event_id:03d}")

        config_text = config_text.replace("{{pools_node_type_id}}", self.pools_node_type_id)
        config_text = config_text.replace("{{default_spark_version}}", self.default_spark_version)

        config_text = config_text.replace("aws:node_type_id", "node_type_id")
        config_text = config_text.replace("aws:aws_attributes", "aws_attributes")

        config_text = config_text.replace("{{courses}}", str(None))
        config_text = config_text.replace("{{datasets}}", f"{self.course_config.course_name}:{self.course_config.data_source_version}")

        config = json.loads(config_text)

        job_id = self.client.jobs.create_from_dict(config)
        self.uws_append_message(f"Created Job {job_id}")

        return job_id

    def wait_for_job_run(self, job_id: str, run_id: str):
        import time
        print(f"| Waiting for job {job_id}, run {run_id} to finish.")

        wait = 15
        new_run = self.client.runs.get(run_id)

        life_cycle_state = new_run.get("state", dict()).get("life_cycle_state")

        if life_cycle_state == "SKIPPED":
            # For some reason, the job was aborted and then restarted.
            # Rather than simply reporting skipped, we want to get the
            # current run_id and resume monitoring from there.
            runs = self.client.runs.list_by_job_id(job_id)
            for past_runs in runs:
                life_cycle_state = past_runs.get("state", dict()).get("life_cycle_state")
                if life_cycle_state == "RUNNING":
                    return self.wait_for_job_run(job_id, past_runs.get("run_id"))

            return new_run

        elif life_cycle_state != "TERMINATED" and life_cycle_state != "INTERNAL_ERROR":
            if life_cycle_state == "PENDING" or life_cycle_state == "RUNNING":
                time.sleep(wait)
            else:
                time.sleep(5)

            return self.wait_for_job_run(job_id, run_id)

        return new_run
