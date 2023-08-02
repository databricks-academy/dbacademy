import os
from dbacademy.common import Cloud
from dbacademy.dbgems import stable_hash
from dbacademy.dbhelper import WorkspaceHelper
from dbacademy.dbrest import DBAcademyRestClient


def to_workspace_name(_event_id: int) -> str:
    """
    The name generated here is done in accordance with a
    standard used in CloudLabs, AirTable and Backup workspaces.
    """

    name = f"classroom-{_event_id:03d}"
    salt = "Databricks Lakehouse"   # A string we use to generate the hash with
    hashcode = stable_hash(salt, 0, _event_id, length=5)
    return f"{name}-{hashcode}".lower()


def create_job(_cloud: Cloud, _event_id: int, _workspace_name: str, _context: str) -> str:
    """
    Creates the job in the target's Databricks workspace using Databricks' REST API. Aside from parameters used
    by the various API calls, some of these parameters also results in custom tracks for tracking and reporting purposes.
    :param _cloud: Identifies the cloud of the target's Databricks workspace; one of AWS, MSA or GCP.
    :param _event_id: Optional 3+ digit code for tracking, ideally an integer.
    :param _workspace_name: E2 accounts have a name associated with each workspace which in turn is part of the
    workspaces' URL. In the case of the "training" E2 account, and the workspace name "classroom-700-rw69a",
    we wil get the URL https://training-classroom-700-rw69a.cloud.databricks.com
    :param _context: The assets' context; e.g. AirTable, Backup, CloudLabs' tenant, Vocareum's tenant, etc.
    :return: The job's ID.
    """
    import requests, json, os

    token = os.environ.get("CDS_DOWNLOAD_TOKEN")
    parameters = {
        "event_id": _event_id,                               # Optional 3+ digit code for tracking, ideally an integer
        "event_description": _workspace_name,                # Customizable, optional, just for general reference.
        "deployment_context": _context,                      # The assets' context; e.g. AirTable, Backup, CloudLabs' tenant, Vocareum's tenant, etc
        "pools_node_type_id": "i3.xlarge",                   # The node type of the pool and thus the student's clusters
        "default_spark_version": "11.3.x-cpu-ml-scala2.12",  # The default runtime presented to students or when clusters are created
        # Installs zero or more courses, see notebook for format examples
        # "courses": None,                                   # Translates to no courses
        # "datasets": None,                                  # Translates to all datasets
        "courses": f"course=example-course&version=v1.3.2&token={token}",
        "datasets": f"example-course:v01",
    }

    config_text = requests.get("https://raw.githubusercontent.com/databricks-academy/workspace-setup/main/universal-workspace-setup-job-config.json").text
    for key, value in parameters.items():
        config_text = config_text.replace("{{"+key+"}}", str(value))

    # The job's JSON file includes config for AWS, MSA and GCP. The following section makes
    # the required changes (e.g. dropping the prefixes) to conform the cloud-specific requirements.

    if _cloud == Cloud.AWS:
        # Enable AWS parameters
        config_text = config_text.replace("aws:node_type_id", "node_type_id")
        config_text = config_text.replace("aws:aws_attributes", "aws_attributes")

    elif _cloud == Cloud.MSA:
        # Enable MSA parameters
        config_text = config_text.replace("msa:node_type_id", "node_type_id")
        config_text = config_text.replace("msa:aws_attributes", "msa_attributes")

    elif _cloud == Cloud.GCP:
        # Enable GCP parameters
        config_text = config_text.replace("gcp:node_type_id", "node_type_id")
        config_text = config_text.replace("gcp:aws_attributes", "gcp_attributes")

    config = json.loads(config_text)

    return client.jobs.create_from_dict(config)


event_id = 700                                                            # Workspace 700 is Jacob's test workspace
workspace_name = to_workspace_name(event_id)                              # This convention comes from other in-house deployments

env_code = "PROSVC"                                                       # Convention used by Labs Platform
username = os.environ.get(f"WORKSPACE_SETUP_{env_code}_USERNAME")         # The username employed by the REST client
password = os.environ.get(f"WORKSPACE_SETUP_{env_code}_PASSWORD")         # The password employed by the REST client
endpoint = f"https://training-{workspace_name}.cloud.databricks.com"      # Construct the endpoint's URL

client = DBAcademyRestClient(endpoint=endpoint, user=username, password=password)
assert len(client.workspace.ls("/")) > 0, f"Expected at least one file."  # Testing connection

job_name = WorkspaceHelper.WORKSPACE_SETUP_JOB_NAME                       # The job's name allowing for configuration by convention
client.jobs.delete_by_name(job_name, False)                               # Hard coded into the corresponding JSON file.)
job_id = create_job(Cloud.AWS, event_id, workspace_name, "Demo")          # Create the job using the DBRest client
run_id = client.jobs.run_now(job_id).get("run_id")                        # Run the job using the DBRest client
response = client.runs.wait_for(run_id)                                   # Blocks until the job has completed

result_state = response.get("state").get("result_state")                  # Validate that results state.
assert result_state == "SUCCESS", f"""Expected the result_state to be "SUCCESS", found "{result_state}"."""

life_cycle_state = response.get("state").get("life_cycle_state")          # Validate the life cycle's state.
assert life_cycle_state == "TERMINATED", f"""Expected the life_cycle_state to be "TERMINATED", found "{life_cycle_state}"."""
