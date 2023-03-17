import json, os
from datetime import datetime
from dbacademy.workspaces_3_0.account_config_class import AccountConfig
from dbacademy.workspaces_3_0.event_config_class import EventConfig
from dbacademy.workspaces_3_0.uc_storage_config_class import UcStorageConfig
from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig
from dbacademy.workspaces_3_0.workspace_setup_class import WorkspaceSetup
from dbacademy.dbrest import DBAcademyRestClient

print("-"*100)
print(f"Starting script at {datetime.now()}")

# endpoint = os.environ.get("WORKSPACE_UNIT_TESTS_API_ENDPOINT")
# token = os.environ.get("WORKSPACE_UNIT_TESTS_API_TOKEN")

endpoint = os.environ.get("WORKSPACE_CURR_DEV_API_ENDPOINT")
token = os.environ.get("WORKSPACE_CURR_DEV_API_TOKEN")

client = DBAcademyRestClient(token=token, endpoint=endpoint)

# results = client.sql.statements.execute(warehouse_id="65f30dc5b33acd5c",
#                                         catalog="jacob_parr",
#                                         schema="default",
#                                         statement="SHOW DATABASES")
#
# state = results.get("status", dict()).get("state")
# assert state == "SUCCEEDED", f"""Expected state to be "SUCCEEDED", found "{state}"."""
# print(json.dumps(results, indent=4))

# existing = client.workspace.ls(path="/Users/jacob.parr@databricks.com/ml-in-production") is not None
# print("Existing:", existing)
#
# if not existing:
#     token = "cl-b0y61SxLcJwzTCbG"
#     target_path = "/Users/jacob.parr@databricks.com/dbacademy/welcome"
#     source_url = f"https://labs.training.databricks.com/api/v1/courses/download.dbc?course=welcome&token={token}"
#     client.workspace.import_dbc_files(target_path=target_path, source_url=source_url, local_file_path="/Temp/file.dbc")


# users = client.scim.users.list()
# for user in users:
#     username = user.get("userName")
#     if username.startswith("class+"):
#         print(username)
