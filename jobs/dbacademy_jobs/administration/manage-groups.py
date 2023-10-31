import os
from typing import Dict, Any
from dbacademy import common
from dbacademy.clients import databricks
from dbacademy.clients.databricks import accounts

accounts = accounts.from_args_aws(account_id=os.environ.get("WORKSPACE_SETUP_CURR_ACCOUNT_ID"),
                                  username=os.environ.get("WORKSPACE_SETUP_CURR_USERNAME"),
                                  password=os.environ.get("WORKSPACE_SETUP_CURR_PASSWORD"))

config_file = r"c:\users\JacobParr\.databrickscfg"


def add_to_admins(group_name: str):
    group = client.scim.groups.get_by_name(group_name)
    assert group is not None, f"""The group "{group_name}" does not exist."""

    admins = client.scim.groups.get_by_name("admins")
    admins_id = admins.get("id")

    client.scim.groups.add_member(admins_id, group.get("id"))


def add_to_participants(_user: Dict[str, Any]):
    user_id = _user.get("id")

    group_name = "Class Participants"
    group = client.scim.groups.get_by_name(group_name)
    assert group is not None, f"""The group "{group_name}" does not exist."""

    group_id = group.get("id")
    client.scim.groups.add_member(group_id, user_id)
    pass


environment = "dev-aws"
configs = common.load_databricks_cfg(config_file)
token = configs.get(environment).get("token")
endpoint = configs.get(environment).get("host")

client = databricks.from_token(token=token, endpoint=endpoint)

# add_to_admins("Curriculum Team")
# add_to_admins("Engineering/Support")
# add_to_admins("Learning Platforms Tech")
# add_to_admins("Curriculum Team - Tudip")

for user in client.scim.users.list():
    username = user.get("userName")
    if username.startswith("class+") and username not in ["class+curriculum@databricks.com"]:
        print(f"Processing {username}")
        add_to_participants(user)
    else:
        print(f"Skipping {username}")

pass
