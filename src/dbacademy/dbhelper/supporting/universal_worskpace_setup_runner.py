__all__ = ["UniversalWorkspaceSetupRunner"]

from dbacademy.dbhelper.course_config import CourseConfig


class UniversalWorkspaceSetupRunner:

    def __init__(self, *, course_config: CourseConfig, token: str = None, endpoint: str = None, workspace_name: str = None):
        from dbacademy import dbgems
        from dbacademy.common import Cloud
        from dbacademy.clients import databricks
        from dbacademy.common import validate

        course_config = validate.any_value(course_config=course_config, parameter_type=CourseConfig, required=True)

        token = token or dbgems.get_notebooks_api_token()
        token = validate.str_value(token=token, required=True)

        endpoint = endpoint or dbgems.get_notebooks_api_endpoint()
        endpoint = validate.str_value(endpoint=endpoint, required=True)

        workspace_name = workspace_name or dbgems.sc.getConf().get("spark.databricks.workspaceUrl", defaultValue="Unknown")
        workspace_name = validate.str_value(workspace_name=workspace_name, required=True)

        self.event_id = 0
        self.course_name = course_config.course_name
        self.data_source_version = course_config.data_source_version
        self.workspace_name = workspace_name
        self.client = databricks.from_token(token=token, endpoint=endpoint)
        self.event_description = f"Workspace {workspace_name}"

        self.default_spark_version = course_config.supported_dbrs[0]

        if Cloud.current_cloud().is_aws:
            self.pools_node_type_id = "i3.xlarge"
        elif Cloud.current_cloud().is_msa:
            self.pools_node_type_id = "Standard_D4ds_v4"
        elif Cloud.current_cloud().is_gcp:
            self.pools_node_type_id = "n2-highmem-4"
        else:
            raise ValueError(f"The cloud {Cloud.current_cloud()} is not supported.")

    def run(self):
        import time
        from dbacademy.dbhelper import dbh_constants

        start = time.time()
        print("Running the Universal Workspace Setup job...")

        self.client.jobs.delete_by_name(dbh_constants.WORKSPACE_HELPER.WORKSPACE_SETUP_JOB_NAME, success_only=False)

        job_id = self.create_job()
        print(f"| Created job {job_id}")

        run = self.client.jobs.run_now(job_id)
        run_id = run.get("run_id")
        print(f"| Started run {run_id}")

        print(f"| Started run {run_id}")
        response = self.wait_for_job_run(job_id, run_id)

        # The job has completed, now we need to evaluate the final state.
        state = response.get("state", dict())
        state_message = state.get("state_message", "Unknown")
        if state is None:
            raise AssertionError("The job's state object is missing.")

        life_cycle_state = state.get("life_cycle_state")
        if life_cycle_state == "SKIPPED":
            print(
                f"""Skipped {dbh_constants.WORKSPACE_HELPER.UNIVERSAL_WORKSPACE_SETUP} (job #{job_id}, run #{run_id}) for "{self.workspace_name}" | {state_message}.""")
            return

        elif life_cycle_state != "TERMINATED":
            raise Exception(
                f"""Expected the final life cycle state of {dbh_constants.WORKSPACE_HELPER.UNIVERSAL_WORKSPACE_SETUP} to be "TERMINATED", found "{life_cycle_state}" for "{self.workspace_name}" | {state_message}""")

        else:
            result_state = state.get("result_state")
            if result_state != "SUCCESS":
                raise Exception(
                    f"""Expected the final state of {dbh_constants.WORKSPACE_HELPER.UNIVERSAL_WORKSPACE_SETUP} to be "SUCCESS", found "{result_state}" for "{self.workspace_name}" | {state_message}""")

        duration = int((time.time() - start) / 60)
        print(f"""Finished {dbh_constants.WORKSPACE_HELPER.UNIVERSAL_WORKSPACE_SETUP} (job #{job_id}, run #{run_id}) for "{self.workspace_name}" ({duration} minutes).""")

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
        config_text = config_text.replace("{{datasets}}", f"{self.course_name}:{self.data_source_version}")

        config = json.loads(config_text)

        job_id = self.client.jobs.create_from_dict(config)
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
