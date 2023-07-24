import os
from dbacademy.dbrest import DBAcademyRestClient


def to_workspace_name(_lab_id: int) -> str:
    """
    The name generated here is done in accordance with a
    started used in CloudLabs, AirTable and Backup workspaces.
    """
    from dbacademy.dbgems import stable_hash

    name = f"classroom-{_lab_id:03d}"
    salt = "Databricks Lakehouse"   # A string we use to generate the hash with
    hashcode = stable_hash(salt, 0, _lab_id, length=5)
    return f"{name}-{hashcode}".lower()


def create_job(_lab_id: int, _workspace_name: str, _context: str) -> str:
    import requests, json, os

    config_text = requests.get("https://raw.githubusercontent.com/databricks-academy/workspace-setup/main/universal-workspace-setup-job-config.json").text
    config_text = config_text.replace("{{event_id}}", str(_lab_id))
    config_text = config_text.replace("{{event_description}}", _workspace_name)
    config_text = config_text.replace("{{deployment_context}}", _context)

    config_text = config_text.replace("{{pools_node_type_id}}", "i3.xlarge")
    config_text = config_text.replace("{{default_spark_version}}", "11.3.x-cpu-ml-scala2.12")

    config_text = config_text.replace("aws:node_type_id", "node_type_id")
    config_text = config_text.replace("aws:aws_attributes", "aws_attributes")

    install_all = True
    if install_all:
        config_text = config_text.replace("{{courses}}", f"None")
        config_text = config_text.replace("{{datasets}}", f"None")
    else:
        token = os.environ.get("CDS_DOWNLOAD_TOKEN")
        config_text = config_text.replace("{{courses}}", f"course=example-course&version=v1.3.2&token={token}")
        config_text = config_text.replace("{{datasets}}", f"example-course:v01")

    config = json.loads(config_text)

    return client.jobs.create_from_dict(config)


def run_job(_job_id: str):
    run = client.jobs.run_now(_job_id)
    return run.get("run_id")


# The event_id and workspace_name convention come from other deployments
workspace_number = 700  # Workspace 700 is Jacob's test workspace
workspace_name = to_workspace_name(workspace_number)

env_code = "PROSVC"  # Convention used by Labs Platform
username = os.environ.get(f"WORKSPACE_SETUP_{env_code}_USERNAME")
password = os.environ.get(f"WORKSPACE_SETUP_{env_code}_PASSWORD")
endpoint = f"https://training-{workspace_name}.cloud.databricks.com"

client = DBAcademyRestClient(endpoint=endpoint, user=username, password=password)
assert len(client.workspace.ls("/")) > 0, f"Expected at least one file."  # Testing connection

client.jobs.delete_by_name("DBAcademy Workspace-Setup", False)  # Hard coded into the corresponding JSON file.)
job_id = create_job(workspace_number, workspace_name, "Demo")  # Context descriptor; AirTable, Backup, CloudLabs, Vocareum
run_id = run_job(job_id)
response = client.runs.wait_for(run_id)

result_state = response.get("state").get("result_state")
assert result_state == "SUCCESS", f"""Expected the result_state to be "SUCCESS", found "{result_state}"."""

life_cycle_state = response.get("state").get("life_cycle_state")
assert life_cycle_state == "TERMINATED", f"""Expected the life_cycle_state to be "TERMINATED", found "{life_cycle_state}"."""
