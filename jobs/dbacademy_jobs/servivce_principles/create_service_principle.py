from dbacademy.dbrest import DBAcademyRestClient
from dbacademy import common

config_file = r"c:\users\JacobParr\.databrickscfg"

# TODO file bug
environments = ["dev-aws", "dev-msa"]  # , "dev-gcp"
for environment in environments:
    configs = common.load_databricks_cfg(config_file)
    token = configs.get(environment).get("token")
    endpoint = configs.get(environment).get("host")

    client = DBAcademyRestClient(token=token, endpoint=endpoint)

    admins = client.scim.groups.get_by_name("admins")
    admins_id = admins.get("id")

    sp_name = "Smoke Tester (Admin)"
    sp = client.scim.service_principals.get_by_name(sp_name)
    if sp is None:
        client.scim.service_principals.create(display_name=sp_name,
                                              group_ids=[admins_id],
                                              entitlements=[])
        # self, display_name: str, group_ids: List = None, entitlements: List = None

    sp = client.scim.service_principals.get_by_name(sp_name)
    sp_id = sp.get("id")
    application_id = sp.get("applicationId")

    print("-"*80)
    print(f"Endpoint: {client.endpoint}")
    print(sp.get("displayName"))

    tokens = client.token_management.list()
    sp_token = None
    for token in tokens:
        owner_id = token.get("owner_id")
        created_by = token.get("created_by_username")
        comment = token.get("comment")
        if application_id == created_by:
            print("Found token for SP.")
            sp_token = token
            # client.token_management.delete_by_id(token.get("token_id"))
            # print(json.dumps(token, indent=4))

    if sp_token is None:
        client.token_management.create_on_behalf_of_service_principal(application_id=application_id, comment="Test Automation", lifetime_seconds=None)
    pass
