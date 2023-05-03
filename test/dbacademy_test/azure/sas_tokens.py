from datetime import datetime

from azure.storage.blob import BlobServiceClient, ResourceTypes, AccountSasPermissions
from azure.identity import InteractiveBrowserCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import generate_account_sas


AZURE_TENANT_ID = "REDACTED"
AZURE_SUBSCRIPTION_ID = "REDACTED"

credentials = InteractiveBrowserCredential(tenant_id=AZURE_TENANT_ID)
resource_client = ResourceManagementClient(credentials, AZURE_SUBSCRIPTION_ID)
storage_client = StorageManagementClient(credentials, AZURE_SUBSCRIPTION_ID)

region_map = {}

for rg in resource_client.resource_groups.list():
    if "databricks-rg" in rg.name:
        continue
    for item in resource_client.resources.list_by_resource_group(rg.name):
        if "dbtraining" in item.name:
            continue
        if "dbtrain" not in item.name:
            continue
        if item.type != 'Microsoft.Storage/storageAccounts':
            continue
        storage_keys = storage_client.storage_accounts.list_keys(rg.name, item.name)
        storage_keys = {v.key_name: v.value for v in storage_keys.keys}
        key1 = storage_keys["key1"]
        account_name = item.name
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credentials)
        sas_token = generate_account_sas(
            account_name=account_name,
            account_key=key1,
            resource_types=ResourceTypes(service=True, container=True, object=True),
            permission=AccountSasPermissions(read=True, list=True),
            expiry=datetime.fromisoformat("2030-02-01T00:00:00")
        )
        region_map[item.location] = (account_name, sas_token)

print("def MSA_REGION_MAP() = {")
print("  Map(")

for region in ("australiacentral", "australiacentral2"):
    account_name, sas_token = region_map["australiasoutheast"]
    print(f'''    "{region + '"':<20} -> ("{account_name}", "?{sas_token}"),''')

for region in region_map:
    account_name, sas_token = region_map[region]
    print(f'''    "{region + '"':<20} -> ("{account_name}", "?{sas_token}"),''')

for region in ("_default",):
    account_name, sas_token = region_map["westus2"]
    print(f'''    "{region + '"':<20} -> ("{account_name}", "?{sas_token}")''')

print("  )")
print("}")
