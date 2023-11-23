import os
from dbacademy.common import Cloud
from dbacademy.clients.darest import accounts_client


accounts = accounts_client.from_args(cloud=Cloud.AWS,
                                     account_id=os.environ.get("WORKSPACE_SETUP_CURR_ACCOUNT_ID"),
                                     username=os.environ.get("WORKSPACE_SETUP_CURR_USERNAME"),
                                     password=os.environ.get("WORKSPACE_SETUP_CURR_PASSWORD"))

# for username in ["class+0@databricks.com"]:
#     user = accounts.scim.users.get_by_username(username)
#     if user is not None:
#         print(f"Deleting {username}.")
#         accounts.scim.users.delete_by_id(user.get("id"))
