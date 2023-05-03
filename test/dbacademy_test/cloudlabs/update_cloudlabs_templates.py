from typing import Set

from dbacademy.cloudlabs import CloudlabsApi, Tenant


def curl_login() -> CloudlabsApi:
    import os
    if "CLOUDLABS_URL" in os.environ:
        return os.environ["CLOUDLABS_CURL"]
    for path in (".curl_login", "../.curl_login", '~/.curl_login'):
        path = os.path.expanduser(path)
        if not os.path.exists(path):
            continue
        with open(path) as f:
            return f.read()
    raise ValueError("Environment variable CLOUDLABS_CURL must be set, or the file .curl_login must exist")


def update_template_descriptions(tenant: Tenant, template_ids: Set[int]):
    updated_ids = set()
    templates = tenant.templates.list_all()
    for template in templates:
        template_id = template["Id"]
        if template_id not in template_ids:
            continue
        updated_ids.add(template_id)
        if template["CloudPlatformId"] == 1:
            # Azure
            description = """<p>Please complete the registration to access your lab environment.</p>"""
            launch_description = """
                <p>Welcome to Databricks Academy:</p>
                <ul>
                    <li>Your lab URL, username &amp; password are in the <span style="font-weight: bold">Environment Details</span> tab.</li>
                    <li>Please open the Databricks URL in an Incognito/Anonymous browser window.</li>
                    <li>Click the <span style="font-weight: bold">Sign in with Azure AD</span> button and then enter the provided username and password.</li>
                    </ul>
            """
            launch_description = "".join(line.strip() for line in launch_description.split("\n"))
        elif template["CloudPlatformId"] == 2:
            # AWS
            description = """<p>Please complete the registration to access your lab environment.</p>"""
            launch_description = """
                <p>Welcome to Databricks Academy:</p>
                <ul>
                    <li>Your lab URL, username &amp; password are in the <span style="font-weight: bold">Environment Details</span> tab.</li>
                    <li>Please open the Databricks URL in an Incognito/Anonymous browser window.</li>
                    <li>Click the <span style="font-weight: bold">Single Sign On</span> button and then enter the provided username and password.</li>
                    </ul>
            """
            launch_description="".join(line.strip() for line in launch_description.split("\n"))
        else:
            raise ValueError(f'Unexpected {template["CloudPlatformId"]}')
        tenant.templates.patch(template_id, {
            "Description": description,
            "LabLaunchPageDescription": launch_description,
        })
    return updated_ids


def main():
    cloudlabs = CloudlabsApi.curl_auth(curl_login())
    tenant = cloudlabs.tenants["Databricks – User Success"]
    template_ids = {5925, 5926, 5927, 5928, 5929, 5930, 5931, 5932, 5933, 5934, 5935, 5936, 5937, 5941, 5943,
                    6105, 6106, 6107, 6192, 6193, 6194, 6195, 6276, 6277, 6278, 6279, 6280, 6281}
    updated_ids = update_template_descriptions(tenant, template_ids)
    if updated_ids != template_ids:
        print(template_ids - updated_ids)


if __name__ == "__main__":
    main()
