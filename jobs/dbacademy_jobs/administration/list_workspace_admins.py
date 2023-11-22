import os
from dbacademy import common
from dbacademy.clients import darest
from dbacademy.clients.darest import accounts

accounts = accounts.from_args_aws(account_id=os.environ.get("WORKSPACE_SETUP_CURR_ACCOUNT_ID"),
                                  username=os.environ.get("WORKSPACE_SETUP_CURR_USERNAME"),
                                  password=os.environ.get("WORKSPACE_SETUP_CURR_PASSWORD"))

environment = "dev-aws"
configs = common.load_databricks_cfg(r"c:\users\JacobParr\.databrickscfg")
token = configs.get(environment).get("token")
endpoint = configs.get(environment).get("host")
client = darest.from_token(token=token, endpoint=endpoint)

admin_ids = set()

for group_name in ["Curriculum Team", "Curriculum Team - Tudip", "Engineering/Support", "Learning Platforms Tech", "Class Participants"]:
    group = client.scim.groups.get_by_name(group_name)
    for member in group.get("members"):
        admin_ids.add(member.get("value"))

assert group is not None, f"""The group "{group_name}" does not exist."""
admins = client.scim.groups.get_by_name("admins")
print("-"*80)

for member in admins.get("members"):
    user_id = member.get("value")

    if member.get("$ref").startswith("Groups"):
        pass
    elif member.get("$ref").startswith("ServicePrincipals"):
        pass
    elif user_id not in admin_ids:
        user = accounts.scim.users.get_by_id(user_id)
        username: str = user.get("userName")
        if "+" in username:
            a = username.find("+")
            b = username.find("@")
            left = username[:a]
            right = username[b:]
            new_username = left+right
            print(new_username)
            new_user = accounts.scim.users.get_by_username(new_username)
            if new_user.get("id") not in admin_ids:
                print(username, new_user)
            # else:
            #     print(f"Skipping", username, new_username)
        else:
            print(username)

print("-"*80)
