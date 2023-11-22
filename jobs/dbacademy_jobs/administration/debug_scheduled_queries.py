import json
from dbacademy import common
from dbacademy.clients import darest

environment = "workspace-000"
configs = common.load_databricks_cfg(r"c:\users\JacobParr\.databrickscfg")
token = configs.get(environment).get("token")
endpoint = configs.get(environment).get("host")
client = darest.from_token(token=token, endpoint=endpoint)

# statement = client.sql.statements.get_statement("01ee722c-154e-168b-b09a-91ae6b3fcd4f")
statement = client.sql.statements.get_statement("01ee7519-e00f-1e77-a3e8-9629c57f2d3f")
print(json.dumps(statement, indent=4))
