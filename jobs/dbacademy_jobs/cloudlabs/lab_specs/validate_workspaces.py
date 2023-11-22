from typing import Dict
from dbacademy_jobs.cloudlabs.lab_specs.lab_specs import load_lab_specs, LabSpec
from dbacademy.clients import darest

lab_specs: Dict[str, LabSpec] = load_lab_specs("C:/Users/JacobParr/.cloudlabs/lt_user_acceptance.json")

for name, lab_spec in lab_specs.items():
    if lab_spec.enabled:
        print("-" * 80)
        print(name)
    else:
        print(f"** SKIPPING {name}")
        continue

    client = darest.from_args(
        token=lab_spec.token,
        endpoint=lab_spec.url,
        username=lab_spec.username,
        password=lab_spec.password,
    )
    try:
        users = [x.get("userName") for x in client.scim.users.list() if x.get("userName") != lab_spec.service_principle]
        count = len(users)
        # files = [x.get("path") for x in client.workspace.ls(f"/Users/{lab_spec.service_principle}")]

    except Exception as e:
        count = -1
        print(f"      {e}")

    print(f"      Found {count} user")
    print()
