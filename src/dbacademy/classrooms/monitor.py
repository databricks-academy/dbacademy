from typing import Dict, Any, List, Callable, cast, Optional

from dbacademy.dougrest import DatabricksApi
from dbacademy.dougrest.accounts.workspaces import Workspace
from dbacademy.rest.common import DatabricksApiException

__all__ = ["Commands", "scan_workspaces", "find_workspace"]


class Commands(object):
    @staticmethod
    def get_region(ws: Workspace):
        """In order to use this function, you must have pre-installed dnspython."""
        from urllib.parse import urlparse
        from dns.resolver import resolve
        import re
        hostname = urlparse(ws.url).netloc
        control_plane = resolve(hostname, 'CNAME')[0].to_text()
        region = re.search("^[^-.]*", control_plane)[0]
        return region

    @staticmethod
    def users_count_instructors(ws: Workspace):
        """Returns a count of the number of odl_instructor_* users."""
        users = ws.users.list_usernames()
        return len([u for u in users if "odl_instructor" in u])

    @staticmethod
    def courseware_verify(courseware_spec: Dict, fix: bool = False, only_students: bool = False):
        def do_courseware_verify(ws: Workspace):
            """Compares each user's home folder to the courseware_spec defined above."""
            users = ws.users.list_usernames()
            results = []
            correct_file_count = -1
            for user in users:
                if only_students and "odl_user" not in user:
                    continue  # Skip instructors
                folder_name = courseware_spec['folder']
                if "dbc" in courseware_spec:
                    workspace_path = f"/Users/{user}/{folder_name}"
                    try:
                        file_count = len(list(ws.workspace.walk(workspace_path)))
                        if correct_file_count < 0:
                            correct_file_count = file_count
                            continue
                        elif file_count == correct_file_count:
                            continue
                        elif file_count > correct_file_count:
                            results.append(user + ", extra files found")
                            continue
                        elif file_count < correct_file_count:
                            results.append(user + ", half imported")
                            if fix:
                                ws.workspace.delete(workspace_path, recursive=True)
                            else:
                                continue
                    except DatabricksApiException:
                        pass
                    if fix:
                        ws.workspace.import_from_url(courseware_spec["dbc"], workspace_path)
                    results.append(user)
                if "repo" in courseware_spec:
                    workspace_path = f"/Repos/{user}/{folder_name}"
                    workspace_path2 = f"/Repos/{user}/{folder_name.lower()}"
                    if ws.repos.exists(workspace_path):
                        continue
                    if ws.repos.exists(workspace_path2):
                        continue
                    # Comment out the w.repos.list() if you want all users to have a cluster.
                    if fix and not ws.repos.list():
                        #           w.workspace.mkdirs(f"/Repos/{user}")
                        ws.repos.create(courseware_spec["repo"], workspace_path)
                    results.append(user)
            return results
        return do_courseware_verify

    @staticmethod
    def warehouses_create_starter(ws: Workspace):
        """Creates a starter SQL Endpoint."""
        warehouse_id = ws.sql.warehouses.create(name="Class Warehouse", min_num_clusters=1, max_num_clusters=1,
                                                photon=True, preview_channel=False, spot=True, size="XXSMALL",
                                                timeout_minutes=45)
        ws.sql.warehouses.stop(warehouse_id)
        ws.permissions.sql.warehouses.update_group(warehouse_id, "users", "CAN_USE")
        return True

    @staticmethod
    def warehouses_remove_starter(ws: Workspace):
        """Deletes the Starter Warehouse."""
        warehouses = ws.sql.warehouses.list_by_name()
        for ep in warehouses.values():
            if ep['name'] in ["Starter Endpoint", "Starter Warehouse", "Serverless Starter Warehouse"]:
                ws.sql.warehouses.delete(ep['id'])
                return True
        return False

    @staticmethod
    def warehouses_remove_all(ws: Workspace):
        """Deletes all SQL Warehouses"""
        warehouses = ws.sql.warehouses.list_by_name()
        for ep in warehouses.values():
            ws.sql.warehouses.delete(ep['id'])
        return len(warehouses)

    @staticmethod
    def warehouses_count(ws: Workspace):
        endpoints = ws.sql.warehouses.list()
        return len(endpoints) or -1

    @staticmethod
    def warehouses_create_shared(ws: Workspace):
        from dbacademy.dbrest import DBAcademyRestClient
        from dbacademy.dbhelper.warehouses_helper_class import WarehousesHelper
        endpoints = ws.sql.warehouses.list()
        client = DBAcademyRestClient(endpoint=ws.url[:-len("/api/")], authorization_header=ws.authorization_header)
        if not endpoints:
            WarehousesHelper.create_sql_warehouse(
                client=client,
                name=WarehousesHelper.WAREHOUSES_DEFAULT_NAME,
                for_user=None,
                auto_stop_mins=None,
                min_num_clusters=2,
                max_num_clusters=20,
                enable_serverless_compute=True,
                lab_id="Unknown",
                workspace_description="Unknown",
                workspace_name="Unknown",
                org_id="Unknown")
            return True
        else:
            return False

    @staticmethod
    def users_count(ws: Workspace):
        """Returns a count of all users in the workspace."""
        users = ws.users.list_usernames()
        return len([u for u in users if "odl_user" in u])

    @staticmethod
    def clusters_list_running(ws: Workspace):
        """Lists all running clusters, including their uptime."""

        def uptime(cluster: Dict) -> float:
            from datetime import datetime
            now_utime = datetime.now().timestamp()
            start_utime = cluster.get("driver", {}).get("start_timestamp", 0) / 1000 or now_utime
            hours = (now_utime - start_utime) / 60 / 60
            return round(hours, 1)

        return [{"cluster_name": c["cluster_name"], "up_hours": uptime(c)} for c in ws.clusters.list() if
                c["state"] != "TERMINATED"]

    @staticmethod
    def warehouses_list(ws: Workspace):
        """Lists all running SQL Warehouses."""
        return [{
                   "name": c["name"],
                   "settings": c,
               } for c in ws.sql.warehouses.list()]

    @staticmethod
    def warehouses_list_running(ws: Workspace):
        """Lists all running SQL Warehouses."""
        return [c["name"] for c in ws.sql.warehouses.list() if c["state"] != "STOPPED"]

    @staticmethod
    def clusters_stop_long_lived(ws: Workspace):
        """Stop clusters up more than 12 hours, including running DLT pipelines and jobs."""

        def uptime(cluster: Dict) -> float:
            from datetime import datetime
            now_utime = datetime.now().timestamp()
            start_utime = cluster.get("driver", {}).get("start_timestamp", 0) / 1000 or now_utime
            hours = (now_utime - start_utime) / 60 / 60
            return round(hours, 1)

        running = [c for c in ws.clusters.list() if c["state"] != "TERMINATED" and uptime(c) > 12]
        for c in running:
            ws.clusters.terminate(c["cluster_id"])
        return len(running)

    @staticmethod
    def clusters_stop(ws: Workspace):
        """Stop all clusters, including running DLT pipelines and jobs."""
        running = [c for c in ws.clusters.list() if c["state"] != "TERMINATED"]
        for c in running:
            ws.clusters.terminate(c["cluster_id"])
        return len(running)

    @staticmethod
    def jobs_list(w: Workspace, stop: bool = False):
        """Clears the jobs schedules.  Jobs currently running are not terminated."""
        results = []
        for job in w.jobs.list():
            if job["settings"].get("schedule") is None:
                continue
            if job["settings"]["schedule"].get("pause_status") != "PAUSED":
                job = w.jobs.get(job["job_id"])
                job_name = job["settings"]["name"]
                if not stop:
                    results += [{"job_name": job_name, "paused": False}]
                else:
                    # Don't stop jobs in the following situations
                    if w["deployment_name"] in ["curriculum-dev", "curriculum", "curriculum-operations"]:
                        results += [{"action": "Job skipped", "name": job_name, "exception": ""}]
                        continue
                    if job_name == "_Monitor_Workspace":
                        results += [{"action": "Job skipped", "name": job_name, "exception": ""}]
                        continue
                    job["settings"]["schedule"]["pause_status"] = "PAUSED"
                    job["new_settings"] = job["settings"]
                    del job["settings"]
                    try:
                        w.jobs.update(job)
                        results += [{"action": "Job stopped", "name": job_name, "exception": ""}]
                    except DatabricksApiException as e:
                        w.jobs.delete(job)
                        results += [{"action": "Job deleted", "name": job_name, "exception": str(e)}]
        return results

    @staticmethod
    def jobs_stop(ws: Workspace):
        return Commands.jobs_list(ws, stop=True)

    @staticmethod
    def warehouses_stop(ws: Workspace):
        """Send a stop command to all SQL Warehouses.  They may still automatically restart."""
        warehouses = ws.sql.warehouses.list()
        count = 0
        for warehouse in warehouses:
            if warehouse.get("state") == "STOPPED":
                continue
            ws.sql.warehouses.edit(warehouse)
            ws.sql.warehouses.stop(warehouse["id"])
            count += 1
        return count

    @staticmethod
    def _collapse_acl(acl: List[Dict]) -> List[Dict]:
        """Takes an ACL you read and turns it into an ACL you can write."""
        results = []
        for ac in acl:
            for ac_type, name in ac.items():
                if ac_type not in ["user_name", "group_name"]:
                    continue
                for perm in ac["all_permissions"]:
                    if perm.get("inherited"):
                        continue
                    level = perm["permission_level"]
                    entry = {ac_type: name, "permission_level": level}
                    results.append(entry)
        return results

    @staticmethod
    def warehouses_reset(workspace, running_only=False):
        """Delete and remake all/running SQL Warehouses.  This will prevent scheduled queries from running."""
        warehouses = workspace.sql.warehouses.list()
        count = 0
        for ep in warehouses:
            if running_only and ep.get("state") == "STOPPED":
                continue
            for tag in ep["tags"].get("custom_tags", []):
                if tag["key"] == "monitor" and tag["value"] in ["false", "False"]:
                    # Don't reset warehouses tagged with monitor=false
                    break
            else:
                warehouse_id1 = ep.pop("id")
                acl = workspace.permissions.sql.warehouses.get(warehouse_id1)["access_control_list"]
                acl = Commands._collapse_acl(acl)
                workspace.sql.warehouses.delete(warehouse_id1)
                from time import sleep
                sleep(1)
                warehouse_id2 = workspace.sql.warehouses.create(**ep)
                assert warehouse_id1 != warehouse_id2
                workspace.sql.warehouses.stop(warehouse_id2)
                workspace.permissions.sql.warehouses.replace(warehouse_id2, acl)
                count += 1
        return count

    @staticmethod
    def warehouses_reset_running(ws: Workspace):
        return Commands.warehouses_reset(ws, running_only=True)

    @staticmethod
    def jobs_stop_dlt(ws: Workspace):
        """Switches any continuous DLT pipelines to standard triggered pipelines"""
        import dateutil.parser as dp
        from time import time as now
        results = []
        page_token = ""
        while True:
            response = ws.api("GET", "2.0/pipelines", page_token=page_token)
            for pipe in response.get("statuses", {}):
                if pipe["state"] == "IDLE":
                    continue
                pipe = ws.api("GET", f"2.0/pipelines/{pipe['pipeline_id']}")
                if pipe["spec"]["continuous"]:
                    print("CONTINUOUS", pipe['pipeline_id'])
                    pipe["spec"]["continuous"] = False
                    ws.api("PUT", f"2.0/pipelines/{pipe['pipeline_id']}", pipe["spec"])
                    results.append({"pipeline": pipe["name"], "action": "Cancel continuous"})
            if "next_page_token" in response:
                page_token = response["next_page_token"]
            else:
                break
        for cluster in ws.clusters.list():
            if cluster.get("cluster_source") in ["PIPELINE", "PIPELINE_MAINTENANCE"] and cluster.get(
                    "cluster_name").startswith("dlt-execution-"):
                pipeline_id = cluster["cluster_name"][len("dlt-execution-"):]
                pipeline = ws.api("GET", f"2.0/pipelines/{pipeline_id}")
                if "latest_updates" in pipeline:
                    lastrun_str = ws.api("GET", f"2.0/pipelines/{pipeline_id}").get("latest_updates", [{}])[
                        0].get("creation_time")
                    lastrun = dp.parse(lastrun_str).timestamp() if lastrun_str else None
                else:
                    lastrun = None
                if not lastrun or (now() - lastrun) / 60 > 30:  # If it's been 30 minutes since last DLT run
                    ws.clusters.terminate(cluster["cluster_id"])
                    results.append({"pipeline": pipeline["name"], "action": "Terminate stale cluster"})
        return results

    @staticmethod
    def models_stop(ws: Workspace):
        """Undeploy any ML Models being served"""
        results = []
        model_endpoints = ws.api("GET", "2.0/preview/mlflow/endpoints/list").get("endpoints", [])
        for ep in model_endpoints:
            ws.api("POST", "2.0/preview/mlflow/endpoints/disable", _data=ep)
            results.append(ep)
        return results

    @staticmethod
    def stop_all(ws: Workspace):
        """Everything: Warehouses, Jobs, DLT, clusters, Warehouses."""
        result = {}
        for cmd in [Commands.jobs_stop, Commands.jobs_stop_dlt,
                    Commands.clusters_stop, Commands.warehouses_reset_running]:
            try:
                result[cmd.__name__] = cmd(ws)
            except Exception as e:
                result[cmd.__name__ + "_Error"] = e
        return result

    @staticmethod
    def pools_list(ws: Workspace):
        """List defined cluster pools"""
        return [{"pool_name": p["instance_pool_name"], "pool": p} for p in ws.pools.list()]

    @staticmethod
    def pools_verify(ws: Workspace):
        """Verify cluster pools are correctly deployed matching specs."""
        results = []
        pools = ws.pools.list()
        assert len(pools) == 1
        for pool in pools:
            if pool["instance_pool_name"].startswith("Student"):
                acl = ws.permissions.pools.get(pool["instance_pool_id"])
                perms = acl["access_control_list"]
                for p in perms:
                    if p.get("group_name") == "users":
                        if p["all_permissions"][0]["permission_level"] == "CAN_ATTACH_TO":
                            break
                else:
                    ws.permissions.pools.update_group(pool["instance_pool_id"], "users", "CAN_ATTACH_TO")
                    results.append(pool["instance_pool_name"])
        return results

    @staticmethod
    def policies_verify(ws: Workspace, fix: bool = False):
        """Verify cluster policies are correctly deployed matching specs."""
        results = []
        policies = ws.clusters.policies.list()
        assert len(policies) == 3
        pools = ws.pools.list()
        assert len(pools) == 1
        pool_id = ws.pools.list()[0]["instance_pool_id"]
        for policy in policies:
            import json
            spec = json.loads(policy["definition"])
            if spec.get("cluster_type", {}).get("value") != "dlt" and not spec.get("instance_pool_id", {}).get(
                    "value") == pool_id:
                results.append(policy["name"])
            if policy["name"].startswith("Student"):
                acl = ws.permissions.clusters.policies.get(policy["policy_id"])
                perms = acl["access_control_list"]
                for p in perms:
                    if p.get("group_name") == "users":
                        if p["all_permissions"][0]["permission_level"] == "CAN_USE":
                            break
                else:  # If not break
                    if fix:
                        ws.permissions.clusters.policies.update_group(policy["policy_id"], "users", "CAN_USE")
                        results.append({"policy": policy["name"], "error": "Fixed users access"})
                    else:
                        results.append({"policy": policy["name"], "error": "Missing users access"})
        return results

    @staticmethod
    def policies_fix(ws: Workspace):
        Commands.policies_verify(ws, True)

    @staticmethod
    def clusters_list(ws: Workspace):
        """List all running clusters"""
        return [{"cluster_name": c["cluster_name"], "cluster": c} for c in ws.clusters.list()]

    @staticmethod
    def clusters_no_manage(ws: DatabricksApi):
        count = 0
        for cluster in ws.clusters.list():
            cluster_id = cluster["cluster_id"]
            acl = ws.permissions.clusters.get(cluster_id).get("access_control_list", [])
            owners = [perm["user_name"] for perm in acl if
                      "user_name" in perm and
                      perm["all_permissions"][0]["inherited"] is False
                      ]
            for owner in owners:
                ws.permissions.clusters.update_user(cluster_id, owner, "CAN_RESTART")
                count += 1
        return count

    @staticmethod
    def clusters_check_missing(cluster_spec: Dict, fix: bool = False):
        def do_clusters_check_missing(ws: Workspace):
            """Create user clusters matching the cluster spec above"""
            import re
            pattern = re.compile(r"\D")
            clusters = ws.clusters.list()
            clusters = [c for c in clusters if c["cluster_name"][0:4] not in ("dlt-", "job-")]
            clusters_map = {pattern.subn("", c["cluster_name"])[0]: c for c in clusters}
            usernames = ws.users.list_usernames()
            results = []
            for user in usernames:
                c = dict(cluster_spec)
                if user.startswith("odl_") or user.startswith("class+"):
                    number = pattern.subn("", user)[0]
                    if number in clusters_map:
                        continue
                    #           c=clusters_map[number]
                    #           result=w.permissions.clusters.update_user(c["cluster_id"], user, "CAN_MANAGE")
                    #           results.append({"cluster_name": c["cluster_name"], "error": "Setting ACL"})
                    elif fix:
                        c["cluster_name"] = "my_cluster_" + number
                        c = ws.clusters.create(**c)
                        cluster_id = c["cluster_id"]
                        ws.clusters.terminate(cluster_id)
                        ws.clusters.set_acl(cluster_id, user_permissions={user: "CAN_MANAGE"})
                        results.append({"cluster_name": c["cluster_name"], "error": "Creating cluster"})
                    else:
                        results.append({"cluster_name": c["cluster_name"], "error": "Missing cluster"})
            return results
        return do_clusters_check_missing

    @staticmethod
    def clusters_verify(cluster_spec: Dict, fix: bool = False):
        def do_clusters_verify(ws: Workspace):
            """Check all clusters against the cluster spec above"""
            clusters = ws.clusters.list()
            clusters = [c for c in clusters if
                        c["cluster_name"][0:4] not in ("dlt-", "job-") and c["cluster_name"] != "my_cluster"]
            if not clusters:
                return [{"cluster_name": " ", "error": "No cluster in workspace"}]
            errors = []
            for c in clusters:
                if c.get("cluster_source") in ["PIPELINE", "JOB", "PIPELINE_MAINTENANCE"]:
                    continue
                differences = {k: c.get(k) for k in cluster_spec if k not in c or c[k] != cluster_spec[k]}
                if "num_workers" in cluster_spec and "autoscale" in c:
                    differences["autoscale"] = str(c["autoscale"])
                    del c["autoscale"]
                if "autoscale" in cluster_spec and "num_workers" in c:
                    differences["num_workers"] = str(c["num_workers"])
                    del c["num_workers"]
                if c.get("cluster_source") not in ["UI", "API"]:
                    differences["cluster_source"] = c.get("cluster_source")
                if not differences:
                    continue
                if (differences.get("custom_tags") or {}).get("ResourceClass") == "SingleNode":
                    differences["spark_conf"] = {
                        "spark.databricks.cluster.profile": "singleNode",
                        "spark.master": "local[*, 4]",
                        "spark.databricks.delta.preview.enabled": "true"
                    }
                err = ", ".join(f"{k}={v}" for k, v in differences.items())
                errors.append({"cluster_name": c["cluster_name"], "error": err})
                if fix:
                    c.update(cluster_spec)
                    if "policy_id" in c and "node_type_id" in c:
                        c["node_type_id"] = None
                    elif "node_type_id" in c and "instance_pool_id" in c:
                        c["instance_pool_id"] = None
                    if "policy_id" in c and "driver_node_type_id" in c:
                        c["driver_node_type_id"] = None
                    elif "driver_node_type_id" in c and "driver_instance_pool_id" in c:
                        c["driver_instance_pool_id"] = None
                    # if "runtime_engine" in c:
                    #     del c["runtime_engine"]
                    for k in list(c.get("aws_attributes", {})):
                        if k.startswith("ebs_"):
                            del c["aws_attributes"][k]
                    try:
                        ws.clusters.update(c)
                    except Exception as e:
                        import json
                        errors.append({"cluster_name": c["cluster_name"], "error": str(e) + json.dumps(c)})
                    # ws.clusters.terminate(c["cluster_id"])
            return errors
        return do_clusters_verify

    @staticmethod
    def clusters_start(ws: Workspace):
        """Send a start command to all clusters"""
        count = 0
        for cluster in ws.clusters.list():
            if cluster["state"] == "TERMINATED" and cluster["cluster_source"] != "JOB":
                ws.clusters.start(cluster["cluster_id"])
                count += 1
        return count

    @staticmethod
    def instructors_add(instructors: List[str]):
        def do_add_instructors(ws: Workspace):
            for user in instructors:
                ws.users.create(user)
                ws.groups.add_member("admins", user_name=user)
        return do_add_instructors

    @staticmethod
    def users_allow_cluster_create(ws: DatabricksApi):
        return ws.scim.groups.users_allow_cluster_create(True, group_name="users")

    @staticmethod
    def users_disallow_cluster_create(ws: DatabricksApi):
        changed = False
        for u in ws.users.list():
            entitlements = {e["value"] for e in u.get("entitlements", [])}
            groups = {g["display"] for g in u.get("groups", [])}
            if "allow-cluster-create" in entitlements and "admins" not in groups:
                changed = True
                ws.users.set_cluster_create(u, cluster_create=False, pool_create=False)
        g = ws.scim.groups.get(group_name="users")
        entitlements = {e["value"] for e in g.get("entitlements", [])}
        if "allow-cluster-create" in entitlements:
            changed = True
            ws.scim.groups.users_allow_cluster_create(False, group_name="users")
        return changed

    @staticmethod
    def users_disallow_databricks_sql(ws: DatabricksApi):
        changed = False
        for u in ws.users.list():
            entitlements = {e["value"] for e in u.get("entitlements", [])}
            groups = {g["display"] for g in u.get("groups", [])}
            if "databricks-sql-access" in entitlements and "admins" not in groups:
                changed = True
                ws.users.set_entitlements(u, {"databricks-sql-access": False})
        g = ws.scim.groups.get(group_name="users")
        entitlements = {e["value"] for e in g.get("entitlements", [])}
        if "databricks-sql-access" in entitlements:
            changed = True
            ws.scim.groups.remove_entitlement("databricks-sql-access", group=g)
        return changed

    @staticmethod
    def warehouses_remove_da(ws: Workspace):
        endpoints = ws.sql.warehouses.list()
        for ep in endpoints:
            if ep["name"].startswith("da-"):
                ws.sql.warehouses.delete(ep["id"])

    @staticmethod
    def _cloud_specific_attributes(ws: Workspace):
        # Cloud specific values
        if ".cloud.databricks.com" in ws.url:
            cloud_attributes = {
                "node_type_id": "i3.xlarge",
                "aws_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "spot_bid_price_percent": 100
                },
            }
        elif ".gcp.databricks.com" in ws.url:
            cloud_attributes = {
                "node_type_id": "n1-highmem-4",
                "gcp_attributes": {
                    "use_preemptible_executors": True,
                    "availability": "PREEMPTIBLE_WITH_FALLBACK_GCP",
                },
            }
        elif ".azuredatabricks.net" in ws.url:
            cloud_attributes = {
                "node_type_id": "Standard_DS3_v2",
                "azure_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK_AZURE"
                },
            }
        else:
            raise ValueError("Workspace is in unknown cloud.")
        return cloud_attributes

    @staticmethod
    def universal_setup(ws: DatabricksApi, *, node_type_id: str = None, spark_version: str = None,
                        datasets: List[str] = None, lab_id: str = None, description: str = None,
                        courses: List[str] = None):
        ws: Workspace = cast(Workspace, ws)
        from dbacademy.dbhelper import WorkspaceHelper
        if ws.cloud == "AWS":
            node_type_id = node_type_id or "i3.xlarge"
        elif ws.cloud == "MSA":
            node_type_id = node_type_id or "Standard_DS3_v2"
        elif ws.cloud == "GCP":
            node_type_id = node_type_id or "n1-standard-4"
        else:
            raise Exception(f"The cloud {ws.cloud} is not supported.")
        spark_version = spark_version or "11.3.x-cpu-ml-scala2.12"

        import re
        workspace_hostname = re.match("https://([^/]+)/api/", ws.url)[1]

        if datasets is None:
            datasets = list()
        if courses is None:
            courses = list()

        # Spec for the job to run
        job_spec = {
            "name": WorkspaceHelper.BOOTSTRAP_JOB_NAME,
            # TODO - 6 hours might be a "little" too long?
            "timeout_seconds": 60 * 60 * 6,  # 6 hours
            "max_concurrent_runs": 1,
            "tasks": [{
                "task_key": "Workspace-Setup",
                "notebook_task": {
                    "notebook_path": "Workspace-Setup",
                    "base_parameters": {
                        WorkspaceHelper.PARAM_EVENT_ID: lab_id or "Unknown",
                        WorkspaceHelper.PARAM_EVENT_DESCRIPTION: description or "Unknown",
                        WorkspaceHelper.PARAM_POOLS_NODE_TYPE_ID: node_type_id,
                        WorkspaceHelper.PARAM_DEFAULT_SPARK_VERSION: spark_version,
                        WorkspaceHelper.PARAM_DATASETS: ",".join(datasets),
                        WorkspaceHelper.PARAM_COURSES: ",".join(courses),
                    },
                    "source": "GIT"
                },
                "job_cluster_key": "Workspace-Setup-Cluster",
                "timeout_seconds": 0
            }],
            "job_clusters": [{
                "job_cluster_key": "Workspace-Setup-Cluster",
                "new_cluster": {
                    "spark_version": "11.3.x-scala2.12",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode"
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                        "dbacademy.event_name": "Unknown",
                        "dbacademy.event_description": "Unknown",
                        "dbacademy.workspace": workspace_hostname
                    },
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": True,
                    "data_security_mode": "SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "num_workers": 0
                }
            }],
            "git_source": {
                "git_url": "https://github.com/databricks-academy/workspace-setup.git",
                "git_provider": "gitHub",
                "git_branch": "main"
            },
            "format": "MULTI_TASK"
        }

        # Append cloud specific attributes the job_clusters spec.
        cloud_attributes = Commands._cloud_specific_attributes(ws)
        job_spec["job_clusters"][0]["new_cluster"].update(cloud_attributes)

        job = ws.jobs.get(job_spec["name"], if_not_exists="ignore") or {}
        if job:
            job_id = job["job_id"]
            ws.jobs.runs.delete_all(job_id)
            job_spec["job_id"] = job_id
            ws.jobs.update(job_spec)
        else:
            job_id = ws.jobs.create_multi_task_job(**job_spec)

        runs = ws.jobs.runs.list(job_id=job_id, active_only=True)
        if runs:
            run_id = runs[0]["run_id"]
        else:
            response = ws.api("POST", "/2.1/jobs/run-now", {"job_id": job_id})
            run_id = response["run_id"]

        # Poll for job completion
        from time import sleep
        while True:
            response = ws.api("GET", f"/2.1/jobs/runs/get?run_id={run_id}")
            life_cycle_state = response.get("state").get("life_cycle_state")
            if life_cycle_state not in ["PENDING", "RUNNING", "TERMINATING"]:
                result = response.get("state").get("result_state")
                return result
            sleep(60)  # seconds

    @staticmethod
    def workspace_setup(courseware_spec: Dict, cluster_spec: Dict, event: Dict, all_users=False):
        def do_workspace_setup(ws: Workspace):
            import requests as web
            from dbacademy.dbhelper import WorkspaceHelper

            courseware_url = courseware_spec["repo"]
            response = web.get(courseware_url + "/blob/published/Includes/Workspace-Setup.py")
            if response.status_code != 200:  # If file exists
                return "No Workspace-Setup found."

            import re
            workspace_hostname = re.match("https://([^/]+)/api/", ws.url)[1]

            # Spec for the job to run
            if all_users:
                configure_for = WorkspaceHelper.CONFIGURE_FOR_ALL_USERS
            else:
                configure_for = WorkspaceHelper.CONFIGURE_FOR_MISSING_USERS_ONLY
            job_spec = {
                "name": "Workspace-Setup",
                "timeout_seconds": 60 * 60 * 6,  # 6 hours
                "max_concurrent_runs": 1,
                "tasks": [{
                    "task_key": "Workspace-Setup",
                    "notebook_task": {
                        "notebook_path": "Includes/Workspace-Setup",
                        "base_parameters": {
                            WorkspaceHelper.PARAM_EVENT_ID: event.get("name", "Unknown"),
                            WorkspaceHelper.PARAM_EVENT_DESCRIPTION: event.get("description", "Unknown"),
                            WorkspaceHelper.PARAM_CONFIGURE_FOR: configure_for,
                        },
                        "source": "GIT"
                    },
                    "job_cluster_key": "Workspace-Setup",
                    "timeout_seconds": 0
                }],
                "job_clusters": [{
                    "job_cluster_key": "Workspace-Setup",
                    "new_cluster": {
                        "spark_version": cluster_spec["spark_version"],
                        "spark_conf": {
                            "spark.master": "local[*, 4]",
                            "spark.databricks.cluster.profile": "singleNode"
                        },
                        "custom_tags": {
                            "ResourceClass": "SingleNode",
                            "dbacademy.event_name": event.get("name", "unknown"),
                            "dbacademy.event_description": event.get("description", "unknown"),
                            "dbacademy.workspace": workspace_hostname
                        },
                        "spark_env_vars": {
                            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                        },
                        "enable_elastic_disk": True,
                        "runtime_engine": "STANDARD",
                        "num_workers": 0
                    }
                }],
                "git_source": {
                    "git_url": courseware_url,
                    "git_provider": "gitHub",
                    "git_branch": "published"
                },
                "format": "MULTI_TASK"
            }

            # Append cloud specific attributes the job_clusters spec.
            cloud_attributes = Commands._cloud_specific_attributes(ws)
            job_spec["job_clusters"][0]["new_cluster"].update(cloud_attributes)

            job = ws.jobs.get(job_spec["name"], if_not_exists="ignore") or {}
            if job:
                job_id = job["job_id"]
                ws.jobs.runs.delete_all(job_id)
                job_spec["job_id"] = job_id
                ws.jobs.update(job_spec)
            else:
                job_id = ws.jobs.create_multi_task_job(**job_spec)

            runs = ws.jobs.runs.list(job_id=job_id, active_only=True)
            if runs:
                run_id = runs[0]["run_id"]
            else:
                response = ws.api("POST", "/2.1/jobs/run-now", {"job_id": job_id})
                run_id = response["run_id"]

            # Poll for job completion
            response = Commands._wait_for(ws, run_id)
            if response.get("state").get("life_cycle_state") == "TERMINATED":
                print("The job completed successfully.")
            else:
                pass

            # while True:
            #     response = workspace.api("GET", f"/2.1/jobs/runs/get?run_id={run_id}")
            #     if response["state"]["life_cycle_state"] not in ["PENDING", "RUNNING", "TERMINATING"]:
            #         result = response["state"]["result_state"]
            #         return result
            #     sleep(60)  # seconds
        return do_workspace_setup

    @staticmethod
    def _wait_for(ws: Workspace, run_id: int):
        import time

        wait = 60  # seconds
        response = ws.api("GET", f"/2.1/jobs/runs/get?run_id={run_id}")
        state = response["state"]["life_cycle_state"]
        job_id = response.get("job_id", 0)

        if state != "TERMINATED" and state != "INTERNAL_ERROR" and state != "SKIPPED":
            if state == "PENDING" or state == "RUNNING":
                print(f" - Job #{job_id}-{run_id} is {state}, checking again in {wait} seconds")
                time.sleep(wait)
            else:
                print(f" - Job #{job_id}-{run_id} is {state}, checking again in 5 seconds")
                time.sleep(5)

            return Commands._wait_for(ws, run_id)

        return response

    @staticmethod
    def policies_create(cluster_spec: Dict, event: Dict):
        def do_policies_create(ws: Workspace):
            import re
            workspace_hostname = re.match(r"^https://([^/]+)/.*$", ws.url)[1]
            machine_type = cluster_spec["node_type_id"]
            autotermination = cluster_spec["autotermination_minutes"]
            spark_version = cluster_spec["spark_version"]

            tags = {
                "dbacademy.event_name": event.get("name", "unknown"),
                "dbacademy.event_description": event.get("description", "unknown"),
                "dbacademy.workspace": workspace_hostname,
            }

            instance_pool_name = f"{machine_type} Pool"
            instance_pool_spec = {
                'instance_pool_name': instance_pool_name,
                'min_idle_instances': 0,
                'node_type_id': machine_type,
                'custom_tags': {k.replace("dbacademy", "dbacademy.pool"): v for k, v in tags.items()},
                'idle_instance_autotermination_minutes': 5,
                'preloaded_spark_versions': [spark_version],
            }

            instance_pool = ws.pools.get_by_name(instance_pool_name, if_not_exists="ignore")
            if not instance_pool:
                instance_pool = ws.api("POST", "2.0/instance-pools/create", instance_pool_spec)
                instance_pool_id = instance_pool["instance_pool_id"]
            else:
                instance_pool_id = instance_pool["instance_pool_id"]
            ws.permissions.pools.update_group(instance_pool_id, "users", "CAN_ATTACH_TO")

            tags_policy = {
                f"custom_tags.{tag_name}": {
                    "type": "fixed",
                    "value": tag_value,
                    "hidden": False,
                }
                for tag_name, tag_value in tags.items()
            }

            cluster_policy = {
                "spark_conf.spark.databricks.cluster.profile": {
                    "type": "fixed",
                    "value": "singleNode",
                    "hidden": False,
                },
                "num_workers": {
                    "type": "fixed",
                    "value": 0,
                    "hidden": False,
                },
                "spark_version": {
                    "type": "unlimited",
                    "defaultValue": "auto:latest-lts-ml",
                    "isOptional": True
                },
                "instance_pool_id": {
                    "type": "fixed",
                    "value": instance_pool_id,
                    "hidden": False,
                },
            }
            cluster_policy.update(tags_policy)

            all_purpose_policy: Dict[str, Any] = {
                "cluster_type": {
                    "type": "fixed",
                    "value": "all-purpose"
                },
                "autotermination_minutes": {
                    "type": "range",
                    "minValue": 1,
                    "maxValue": 180,
                    "defaultValue": autotermination,
                },
                "data_security_mode": {
                    "type": "unlimited",
                    "defaultValue": "SINGLE_USER"
                },
            }
            all_purpose_policy.update(cluster_policy)
            all_purpose_policy = ws.clusters.policies.create_or_update("DBAcademy",
                                                                       all_purpose_policy)
            all_purpose_policy_id = all_purpose_policy.get("policy_id")
            ws.permissions.clusters.policies.update(all_purpose_policy_id, "group_name", "users", "CAN_USE")

            jobs_policy: Dict[str, Any] = {
                "cluster_type": {
                    "type": "fixed",
                    "value": "job"
                },
            }
            jobs_policy.update(cluster_policy)
            jobs_policy = ws.clusters.policies.create_or_update("DBAcademy Jobs", jobs_policy)
            jobs_policy_id = jobs_policy.get("policy_id")
            ws.permissions.clusters.policies.update(jobs_policy_id, "group_name", "users", "CAN_USE")
            dlt_policy: Dict[str, Any] = {
                "cluster_type": {
                    "type": "fixed",
                    "value": "dlt"
                },
                "num_workers": {
                    "type": "fixed",
                    "value": 0,
                    "hidden": False,
                },
            }
            for forbidden in ("node_type_id", "driver_node_type_id", "instance_pool_id", "driver_instance_pool_id"):
                if forbidden in dlt_policy:
                    del dlt_policy[forbidden]
            dlt_policy.update(tags_policy)
            dlt_policy = ws.clusters.policies.create_or_update("DBAcademy DLT", dlt_policy)
            dlt_policy_id = dlt_policy.get("policy_id")
            ws.permissions.clusters.policies.update(dlt_policy_id, "group_name", "users", "CAN_USE")
        return do_policies_create

    @staticmethod
    def clusters_set_single_user(ws: Workspace):
        def get_owners(cluster: Dict) -> List[str]:
            cluster_id = cluster["cluster_id"]
            acl = ws.permissions.clusters.get(cluster_id).get("access_control_list", [])
            owners = [perm["user_name"] for perm in acl if
                      "user_name" in perm and
                      perm["user_name"] not in ["databricksadmin@databrickslabs.onmicrosoft.com"] and
                      perm["all_permissions"][0]["inherited"] is False and
                      perm["all_permissions"][0]["permission_level"] in ["CAN_MANAGE", "CAN_RESTART"]
                      ]
            return owners

        def update_cluster(cluster: Dict) -> Optional[Dict]:
            #     if "lab-cluster" not in cluster["cluster_name"]:
            #       return
            if cluster.get("data_security_mode") == "SINGLE_USER" and cluster.get("spark_conf", {}).get(
                    "spark.databricks.dataLineage.enabled") == "true":
                return
            owners = get_owners(cluster)
            if len(owners) != 1:
                return {"Cluster": cluster["cluster_name"], "Error": f"Ambiguous ownership: {owners!r}"}
            cluster["single_user_name"] = owners[0]
            cluster["data_security_mode"] = "SINGLE_USER"
            cluster.get("spark_conf", {})["spark.databricks.dataLineage.enabled"] = "true"
            for forbidden_key in ("ebs_volumes_spec", "ebs_volume_count"):
                if forbidden_key in cluster.get("aws_attributes", {}):
                    del cluster["aws_attributes"][forbidden_key]
            try:
                ws.clusters.update(cluster)
            except Exception as ex:
                return {"Cluster": cluster["cluster_name"], "Error": str(ex)}

        from multiprocessing.pool import ThreadPool
        with ThreadPool(100) as pool:
            results = pool.map(update_cluster, ws.clusters.list())
        return [r for r in results if r is not None]

    @staticmethod
    def users_add_azure_principal(ws: Workspace):
        """Add Doug's Azure Service Principal"""
        client_id = "4d5472a4-eaa9-47bf-8a9f-e8581f865be1"
        try:
            ws.api("POST", "2.0/preview/scim/v2/ServicePrincipals", {
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
                "applicationId": client_id,
            })
        except DatabricksApiException as e:
            if e.http_code != 409:
                raise e
        ws.groups.add_member("admins", user_name=client_id)

    @staticmethod
    def users_add_admins(admins: List[str]):
        def do_add_admins(ws: Workspace):
            for username in admins:
                ws.scim.users.create(username, if_exists="ignore")
                ws.groups.add_member("admins", user_name=username)
        return do_add_admins


def find_workspace(workspaces: List[DatabricksApi], *, name: str = None, url: str = None) -> Workspace:
    workspaces: List[Workspace] = cast(List[Workspace], workspaces)
    if name:
        return next(w for w in workspaces if w["workspace_name"] == name)
    elif url:
        return next(w for w in workspaces if url in w.url)
    else:
        raise Exception("getWorkspace: must provide workspace name or url")


def scan_workspaces(function: Callable[[Workspace], Any], workspaces: List[DatabricksApi], *,
                    url: str = None, name: str = None, ignore_connection_errors: bool = False):
    from requests.exceptions import ConnectionError, HTTPError
    from collections.abc import Mapping, Iterable
    from itertools import chain
    from collections import OrderedDict
    from pyspark.sql import Row
    if name:
        workspaces = (w for w in workspaces if w["workspace_name"] == name)
    elif url:
        workspaces = (w for w in workspaces if url in w.url)

    def check_workspace(ws: Workspace):
        try:
            result = function(ws)
            # Standardize results as an iterator of OrderedDict.
            if not result:
                result = ()
            elif not isinstance(result, Iterable) or isinstance(result, str) or isinstance(result, Mapping):
                result = (result,)
            for r in result:
                if not isinstance(r, Mapping):
                    r = {"result": r}
                yield ws, r, None
        except DatabricksApiException as e:
            yield ws, None, e
        except ConnectionError as e:
            if not ignore_connection_errors:
                yield ws, None, e
        except HTTPError as e:
            yield ws, None, e

    #     except Exception as e:
    #       yield (w, None, e)

    from multiprocessing.pool import ThreadPool
    with ThreadPool(500) as pool:
        map_results = pool.map(lambda w: list(check_workspace(w)), workspaces)

    # Determine the schema for the results and turn it into a pretty dataframe cs
    example = OrderedDict()
    map_results = [r for w in map_results for r in w]
    for r in map_results:
        if r[1]:
            example.update(r[1])
    keys = example.keys()

    results = []
    for r in map_results:
        if r[1]:
            result_field = r[1].items()
        elif keys:
            result_field = ((k, None) for k in keys)
        else:
            result_field = (("result", ""),)
        workspace_field = (
            ("deployment", r[0]["deployment_name"]),
            ("workspace", r[0].url[:-4]),
            ("instructor", r[0].user or "")
        )
        exception_field = (("exception", str(r[2]) if r[2] else ""),)
        row = OrderedDict(chain(workspace_field, result_field, exception_field))
        results.append(Row(**row))
    if not results:
        print("No results.")
        return None
    try:
        from dbacademy.dbgems import display
        display(results)
        return results
    except Exception as ex:
        print("DataFrame:", ex)
        from pprint import pprint
        pprint(results)
        print("-----")
        return results
