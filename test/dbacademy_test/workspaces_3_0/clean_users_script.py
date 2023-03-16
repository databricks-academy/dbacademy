import os
from datetime import datetime
from dbacademy.workspaces_3_0.account_config_class import AccountConfig
from dbacademy.workspaces_3_0.event_config_class import EventConfig
from dbacademy.workspaces_3_0.uc_storage_config_class import UcStorageConfig
from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig
from dbacademy.workspaces_3_0.workspace_setup_class import WorkspaceSetup
from dbacademy.dbrest import DBAcademyRestClient

print("-"*100)
print(f"Starting script at {datetime.now()}")

# api_endpoint = os.environ.get("DBACADEMY_UNIT_TESTS_API_ENDPOINT")
# api_token = os.environ.get("DBACADEMY_UNIT_TESTS_API_TOKEN")

endpoint = os.environ.get("DBACADEMY_CURR_DEV_API_ENDPOINT")
token = os.environ.get("DBACADEMY_CURR_DEV_API_TOKEN")

client = DBAcademyRestClient(token=token, endpoint=endpoint)
users = client.scim.users.list()
for user in users:
    username = user.get("userName")
    if username.startswith("class+"):
        print(username)
