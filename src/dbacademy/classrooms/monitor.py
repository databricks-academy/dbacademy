from typing import Dict, Any

from dbacademy.dougrest import DatabricksApi
from dbacademy.rest.common import DatabricksApiException


# noinspection PyPep8Naming
class Commands(object):
    def __init__(self, cluster_spec: Dict, courseware_spec: Dict, event: Dict):
        self.cluster_spec = cluster_spec
        self.courseware_spec = courseware_spec
        self.event = event
        self.all_users = False

    @staticmethod
    def countInstructors(workspace):
        """Returns a count of the number of odl_instructor_* users."""
        users = workspace.users.list_usernames()
        return len([u for u in users if "odl_instructor" in u])

    def verifyCourseware(self, w, fix=False, only_students=False):
        """Compares each user's home folder to the courseware_spec defined above."""
        users = w.users.list_usernames()
        results = []
        correct_file_count = -1

        for user in users:
            if only_students and "odl_user" not in user:
                continue  # Skip instructors
            folder_name = self.courseware_spec['folder']
            if "dbc" in self.courseware_spec:
                workspace_path = f"/Users/{user}/{folder_name}"
                try:
                    file_count = len(list(w.workspace.walk(workspace_path)))
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
                            w.workspace.delete(workspace_path, recursive=True)
                        else:
                            continue
                except DatabricksApiException:
                    pass
                if fix:
                    w.workspace.import_from_url(self.courseware_spec["dbc"], workspace_path)
                results.append(user)
            if "repo" in self.courseware_spec:
                workspace_path = f"/Repos/{user}/{folder_name}"
                workspace_path2 = f"/Repos/{user}/{folder_name.lower()}"
                if w.repos.exists(workspace_path):
                    continue
                if w.repos.exists(workspace_path2):
                    continue
                # Comment out the w.repos.list() if you want all users to have a cluster.
                if fix and not w.repos.list():
                    #           w.workspace.mkdirs(f"/Repos/{user}")
                    w.repos.create(self.courseware_spec["repo"], workspace_path)
                results.append(user)
        return results

    def fixCourseware(self, workspace):
        """Compares each user's home folder to the courseware_spec defined above, deploying if needed."""
        return self.verifyCourseware(workspace, fix=True)

    @staticmethod
    def endpointsSetPreview(workspace):
        #   import re
        #   users=workspace.users.list_usernames()
        #   user=next(u for u in users if "odl_user" in u)
        #   workspace['user']=user
        #   workspace['userNum']=re.match('odl_user_([0-9]+)@databrickslabs\.com', workspace['user'])[1]
        #   c = workspace.sql.endpoints.list_by_name()["my-endpoint-" + workspace['userNum']]
        c = workspace.sql.endpoints.list_by_name()["my-endpoint"]
        if not c.get('channel', {}).get('name') == "CHANNEL_NAME_PREVIEW":
            c['channel'] = {'name': 'CHANNEL_NAME_PREVIEW'}
            workspace.sql.endpoints.edit(c)
        return None

    @staticmethod
    def endpointsCreateStarter(workspace):
        """Creates a starter SQL Endpoint."""
        endpoint_id = workspace.sql.endpoints.create(name="Class Warehouse", min_num_clusters=1, max_num_clusters=1,
                                                     photon=True, preview_channel=False, spot=True, size="XXSMALL",
                                                     timeout_minutes=45)
        workspace.sql.endpoints.stop(endpoint_id)
        workspace.permissions.sql.endpoints.update_group(endpoint_id, "users", "CAN_USE")
        return True

    @staticmethod
    def endpointsRemoveStarter(workspace):
        """Deletes the Starter Warehouse."""
        endpoints = workspace.sql.endpoints.list_by_name()
        for ep in endpoints.values():
            if ep['name'] in ["Starter Endpoint", "Starter Warehouse"]:
                workspace.sql.endpoints.delete(ep['id'])
                return True
        return False

    @staticmethod
    def endpointsRemoveAll(workspace):
        """Deletes all SQL Endpoints"""
        endpoints = workspace.sql.endpoints.list_by_name()
        for ep in endpoints.values():
            workspace.sql.endpoints.delete(ep['id'])
        return len(endpoints)

    @staticmethod
    def countUsers(workspace):
        """Returns a count of all users in the workspace."""
        users = workspace.users.list_usernames()
        return len([u for u in users if "odl_user" in u])

    @staticmethod
    def runningClusters(w):
        """Lists all running clusters, including their uptime."""

        def uptime(cluster):
            from datetime import datetime
            now_utime = datetime.now().timestamp()
            start_utime = cluster.get("driver", {}).get("start_timestamp", 0) / 1000 or now_utime
            hours = (now_utime - start_utime) / 60 / 60
            return round(hours, 1)

        return [{"cluster_name": c["cluster_name"], "up_hours": uptime(c)} for c in w.clusters.list() if
                c["state"] != "TERMINATED"]

    @staticmethod
    def runningEndpoints(w):
        """Lists all running SQL Warehouses."""
        return [c["name"] for c in w.sql.endpoints.list() if c["state"] != "STOPPED"]

    @staticmethod
    def stopClusters(w):
        """Stop all clusters, including running DLT pipelines and jobs."""
        running = [c for c in w.clusters.list() if c["state"] != "TERMINATED"]
        for c in running:
            w.clusters.terminate(c["cluster_id"])
        return len(running)

    @staticmethod
    def listJobs(w, stop=False):
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
    def stopJobs(w):
        return Commands.listJobs(w, stop=True)

    @staticmethod
    def listScheduledQueries(w):
        """List scheduled queries.  This may fail due to permissions issues."""
        return [q for q in w.api("GET", "2.0/preview/sql/queries").get("results", []) if q.get("schedule")]

    @staticmethod
    def stopQueries(w):
        """
        Attempts to unschedule any scheduled SQL Query Refresh Schedules.
        This may fail due to permissions issues.
        """
        count = 0
        for q in w.api("GET", "2.0/preview/sql/queries").get("results", []):
            if q.get("schedule"):
                q = w.api("GET", f"2.0/preview/sql/queries/{q['id']}")
                q["schedule"] = None
                w.api("POST", f"2.0/preview/sql/queries/{q['id']}", _data=q)
                count += 1
        return count

    @staticmethod
    def stopDashboards(workspace):
        """
        Attempts to unschedule any scheduled SQL Dashboard Refresh Schedules.
        This may fail due to permissions issues.
        """
        results = []
        for dashboard in workspace.api("GET", "2.0/preview/sql/dashboards").get("results", []):
            if dashboard.get("refresh_schedules"):
                results.append(dashboard["name"])
                dashboard["refresh_schedules"] = []
                workspace.api("POST", f"2.0/preview/sql/dashboards/{dashboard['id']}", dashboard)
        return results

    @staticmethod
    def stopEndpoints(w):
        """Send a stop command to all SQL endpoints.  They may still automatically restart."""
        endpoints = w.sql.endpoints.list()
        count = 0
        for ep in endpoints:
            if ep.get("state") == "STOPPED":
                continue
            w.sql.endpoints.edit(ep)
            w.sql.endpoints.stop(ep["id"])
            count += 1
        return count

    @staticmethod
    def collapseACL(acl):
        """Takes an ACL you read and turns it into an ACL you can write."""
        results = []
        for ac in acl:
            for ac_type, name in ac.items():
                if ac_type == "all_permissions":
                    continue
                for perm in ac["all_permissions"]:
                    if perm.get("inherited"):
                        continue
                    level = perm["permission_level"]
                    entry = {ac_type: name, "permission_level": level}
                    results.append(entry)
        return results

    @staticmethod
    def resetEndpoints(workspace, running_only=False):
        """Delete and remake all/running SQL endpoints."""
        endpoints = workspace.sql.endpoints.list()
        count = 0
        for ep in endpoints:
            if running_only and ep.get("state") == "STOPPED":
                continue
            endpoint_id1 = ep.pop("id")
            acl = workspace.permissions.sql.endpoints.get(endpoint_id1)["access_control_list"]
            acl = Commands.collapseACL(acl)
            workspace.sql.endpoints.delete(endpoint_id1)
            from time import sleep
            sleep(1)
            endpoint_id2 = workspace.sql.endpoints.create(**ep)
            assert endpoint_id1 != endpoint_id2
            workspace.sql.endpoints.stop(endpoint_id2)
            workspace.permissions.sql.endpoints.replace(endpoint_id2, acl)
            count += 1
        return count

    @staticmethod
    def resetRunningEndpoints(workspace):
        return Commands.resetEndpoints(workspace, running_only=True)

    @staticmethod
    def disableEndpoints(w):
        """Send a stop command to all SQL endpoints.  Disable auto-restart."""
        endpoints = w.sql.endpoints.list()
        count = 0
        for ep in endpoints:
            if ep.get("state") == "STOPPED":
                continue
            ep["auto_resume"] = False
            w.sql.endpoints.edit(ep)
            w.sql.endpoints.stop(ep["id"])
            count += 1
        return count

    @staticmethod
    def stopDLT(workspace):
        """Switches any continuous DLT pipelines to standard triggered pipelines"""

        # TODO dateutil is a "provided" package in Notebooks.
        # noinspection PyPackageRequirements
        import dateutil.parser as dp

        from time import time as now
        results = []
        page_token = ""
        while True:
            response = workspace.api("GET", "2.0/pipelines", page_token=page_token)
            for pipe in response.get("statuses", {}):
                if pipe["state"] == "IDLE":
                    continue
                pipe = workspace.api("GET", f"2.0/pipelines/{pipe['pipeline_id']}")
                if pipe["spec"]["continuous"]:
                    print("CONTINUOUS", pipe['pipeline_id'])
                    pipe["spec"]["continuous"] = False
                    workspace.api("PUT", f"2.0/pipelines/{pipe['pipeline_id']}", pipe["spec"])
                    results.append({"pipeline": pipe["name"], "action": "Cancel continuous"})
            if "next_page_token" in response:
                page_token = response["next_page_token"]
            else:
                break
        for cluster in workspace.clusters.list():
            if cluster.get("cluster_source") in ["PIPELINE", "PIPELINE_MAINTENANCE"] and cluster.get(
                    "cluster_name").startswith("dlt-execution-"):
                pipeline_id = cluster["cluster_name"][len("dlt-execution-"):]
                pipeline = workspace.api("GET", f"2.0/pipelines/{pipeline_id}")
                if "latest_updates" in pipeline:
                    lastrun_str = workspace.api("GET", f"2.0/pipelines/{pipeline_id}").get("latest_updates", [{}])[
                        0].get("creation_time")
                    lastrun = dp.parse(lastrun_str).timestamp() if lastrun_str else None
                else:
                    lastrun = None
                if not lastrun or (now() - lastrun) / 60 > 30:  # If it's been 30 minutes since last DLT run
                    workspace.clusters.terminate(cluster["cluster_id"])
                    results.append({"pipeline": pipeline["name"], "action": "Terminate stale cluster"})
        return results

    @staticmethod
    def stopModels(workspace):
        """Undeploys any ML Models being served"""
        results = []
        model_endpoints = workspace.api("GET", "2.0/preview/mlflow/endpoints/list").get("endpoints", [])
        for ep in model_endpoints:
            workspace.api("POST", "2.0/preview/mlflow/endpoints/disable", _data=ep)
            results.append(ep)
        return results

    @staticmethod
    def stopAll(w):
        """Everything: Queries, Dashboards, Endpoints, Jobs, DLT, clusters, Warehouses."""
        result = {}
        for cmd in [Commands.stopQueries, Commands.stopDashboards, Commands.stopEndpoints, Commands.stopJobs,
                    Commands.stopDLT, Commands.stopClusters, Commands.resetRunningEndpoints]:
            try:
                result[cmd.__name__] = cmd(w)
            except Exception as e:
                result[cmd.__name__ + "_Error"] = e
        return result

    @staticmethod
    def listAlarms(workspace):
        """Search for any SQL Query alarms that might restart Warehouses"""
        # See: https://databricks.slack.com/archives/CTV173T6G/p1652820397463219
        import base64
        from dbacademy.dbgems import dbutils
        results = []
        for ep in workspace.sql.endpoints.list():
            username = ep.get("creator_name", "class+000@databricks.com")
            password = "***REMOVED***" if "class+000" not in username else dbutils.secrets.get("admin", "class000")
            encoded_auth = (username + ":" + password).encode()
            authorization_header = "Basic " + base64.standard_b64encode(encoded_auth).decode()
            old_headers = workspace.session.headers
            try:
                workspace.session.headers = {'Authorization': authorization_header, 'Content-Type': 'text/json'}
                result = workspace.api("GET", "2.0/preview/sql/alerts")
                results.extend(result)
            finally:
                workspace.session.headers = old_headers
        return results

    @staticmethod
    def poolsList(w):
        """List defined cluster pools"""
        return [{"pool_name": p["instance_pool_name"], "pool": p} for p in w.pools.list()]

    @staticmethod
    def poolsVerify(w):
        """Verify cluster pools are correctly deployed matching specs."""
        results = []
        pools = w.pools.list()
        assert len(pools) == 1
        for pool in pools:
            if pool["instance_pool_name"].startswith("Student"):
                acl = w.permissions.pools.get(pool["instance_pool_id"])
                perms = acl["access_control_list"]
                for p in perms:
                    if p.get("group_name") == "users":
                        if p["all_permissions"][0]["permission_level"] == "CAN_ATTACH_TO":
                            break
                else:
                    w.permissions.pools.update_group(pool["instance_pool_id"], "users", "CAN_ATTACH_TO")
                    results.append(pool["instance_pool_name"])
        return results

    @staticmethod
    def policiesVerify(w, fix=False):
        """Verify cluster policies are correctly deployed matching specs."""
        results = []
        policies = w.clusters.policies.list()
        assert len(policies) == 3
        pools = w.pools.list()
        assert len(pools) == 1
        pool_id = w.pools.list()[0]["instance_pool_id"]
        for policy in policies:
            import json
            spec = json.loads(policy["definition"])
            if spec.get("cluster_type", {}).get("value") != "dlt" and not spec.get("instance_pool_id", {}).get(
                    "value") == pool_id:
                results.append(policy["name"])
            if policy["name"].startswith("Student"):
                acl = w.permissions.clusters.policies.get(policy["policy_id"])
                perms = acl["access_control_list"]
                for p in perms:
                    if p.get("group_name") == "users":
                        if p["all_permissions"][0]["permission_level"] == "CAN_USE":
                            break
                else:  # If not break
                    if fix:
                        w.permissions.clusters.policies.update_group(policy["policy_id"], "users", "CAN_USE")
                        results.append({"policy": policy["name"], "error": "Fixed users access"})
                    else:
                        results.append({"policy": policy["name"], "error": "Missing users access"})
        return results

    @staticmethod
    def policiesFix(w):
        Commands.policiesVerify(w, True)

    @staticmethod
    def clustersList(w):
        """List all running clusters"""
        return [{"cluster_name": c["cluster_name"], "cluster": c} for c in w.clusters.list()]

    def clustersCreateMissing(self, w, fix=False):
        """Create user clusters matching the cluster spec above"""
        import re
        pattern = re.compile(r"\D")
        clusters = w.clusters.list()
        clusters = [c for c in clusters if c["cluster_name"][0:4] not in ("dlt-", "job-")]
        clusters_map = {pattern.subn("", c["cluster_name"])[0]: c for c in clusters}
        usernames = w.users.list_usernames()
        results = []
        for user in usernames:
            c = dict(self.cluster_spec)
            if user.startswith("odl_") or user.startswith("class+"):
                number = pattern.subn("", user)[0]
                if number in clusters_map:
                    continue
                #           c=clusters_map[number]
                #           result=w.permissions.clusters.update_user(c["cluster_id"], user, "CAN_MANAGE")
                #           results.append({"cluster_name": c["cluster_name"], "error": "Setting ACL"})
                elif fix:
                    c["cluster_name"] = "my_cluster_" + number
                    c = w.clusters.create(**c)
                    cluster_id = c["cluster_id"]
                    w.clusters.terminate(cluster_id)
                    w.clusters.set_acl(cluster_id, user_permissions={user: "CAN_MANAGE"})
                    results.append({"cluster_name": c["cluster_name"], "error": "Creating cluster"})
                else:
                    results.append({"cluster_name": c["cluster_name"], "error": "Missing cluster"})
        return results

    def clustersVerify(self, w, fix=False):
        """Check all clusters against the cluster spec above"""
        import re
        clusters = w.clusters.list()
        clusters = [c for c in clusters if
                    c["cluster_name"][0:4] not in ("dlt-", "job-") and c["cluster_name"] != "my_cluster"]
        pattern = re.compile(r"\D")

        # TODO What's the purpose of this call if the return value is not used?
        # noinspection PyUnusedLocal
        clusters_map = {pattern.subn("", c["cluster_name"])[0]: c["cluster_id"] for c in clusters}

        if not clusters:
            return [{"cluster_name": " ", "error": "No cluster in workspace"}]

        errors = []
        for c in clusters:
            # TODO this instance of err is not used and is redefined at line 489
            # noinspection PyUnusedLocal
            err = None
            if c.get("cluster_source") in ["PIPELINE", "JOB", "PIPELINE_MAINTENANCE"]:
                continue
            differences = {k: c.get(k) for k in self.cluster_spec if k not in c or c[k] != self.cluster_spec[k]}
            if "num_workers" in self.cluster_spec and "autoscale" in c:
                differences["autoscale"] = str(c["autoscale"])
                del c["autoscale"]
            if "autoscale" in self.cluster_spec and "num_workers" in c:
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
                c.update(self.cluster_spec)
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
                    w.clusters.update(c)
                except Exception as e:
                    import json
                    errors.append({"cluster_name": c["cluster_name"], "error": str(e) + json.dumps(c)})
        #         w.clusters.terminate(c["cluster_id"])
        return errors

    def clustersFix(self, w):
        """Check all clusters against the cluster spec above, correcting where needed."""
        return self.clustersVerify(w, fix=True)

    @staticmethod
    def clustersStart(w):
        """Send a start command to all clusters"""
        count = 0
        for cluster in w.clusters.list():
            if cluster["state"] == "TERMINATED" and cluster["cluster_source"] != "JOB":
                w.clusters.start(cluster["cluster_id"])
                count += 1
        return count

    # TODO Remove unused parameter "w"
    # noinspection PyUnusedLocal
    @staticmethod
    def addInstructors(w, instructors):

        def add_instructors(w2):
            for user in instructors:
                w2.users.create(user)
                w2.groups.add_member("admins", user_name=user)

        return add_instructors

    @staticmethod
    def allow_cluster_create(w: DatabricksApi):
        return w.scim.groups.allow_cluster_create(True, group_name="users")

    @staticmethod
    def disallow_cluster_create(w: DatabricksApi):
        changed = False
        for u in w.users.list():
            entitlements = {e["value"] for e in u.get("entitlements", [])}
            groups = {g["display"] for g in u.get("groups", [])}
            if "allow-cluster-create" in entitlements and "admins" not in groups:
                changed = True
                w.users.set_cluster_create(u, cluster_create=False, pool_create=False)
        g = w.scim.groups.get(group_name="users")
        entitlements = {e["value"] for e in g.get("entitlements", [])}
        if "allow-cluster-create" in entitlements:
            changed = True
            w.scim.groups.allow_cluster_create(False, group_name="users")
        return changed

    @staticmethod
    def disallow_databricks_sql(w: DatabricksApi):
        changed = False
        for u in w.users.list():
            entitlements = {e["value"] for e in u.get("entitlements", [])}
            groups = {g["display"] for g in u.get("groups", [])}
            if "databricks-sql-access" in entitlements and "admins" not in groups:
                changed = True
                w.users.set_entitlements(u, {"databricks-sql-access": False})
        g = w.scim.groups.get(group_name="users")
        entitlements = {e["value"] for e in g.get("entitlements", [])}
        if "databricks-sql-access" in entitlements:
            changed = True
            w.scim.groups.remove_entitlement("databricks-sql-access", group=g)
        return changed

    @staticmethod
    def remove_da_endpoints(ws):
        endpoints = ws.sql.endpoints.list()
        for ep in endpoints:
            if ep["name"].startswith("da-"):
                ws.sql.endpoints.delete(ep["id"])

    # TODO local variables "cloud" are not used
    # noinspection PyUnusedLocal
    @staticmethod
    def _cloud_specific_attributes(workspace):
        # Cloud specific values
        if ".cloud.databricks.com" in workspace.url:
            cloud = "AWS"
            cloud_attributes = {
                "node_type_id": "i3.xlarge",
                "aws_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "spot_bid_price_percent": 100
                },
            }
        elif ".gcp.databricks.com" in workspace.url:
            cloud = "GCP"
            cloud_attributes = {
                "node_type_id": "n1-highmem-4",
                "gcp_attributes": {
                    "use_preemptible_executors": True,
                    "availability": "PREEMPTIBLE_WITH_FALLBACK_GCP",
                },
            }
        elif ".azuredatabricks.net" in workspace.url:
            cloud = "MSA"
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
    def universal_setup(workspace: DatabricksApi):
        from dbacademy.dbhelper import WorkspaceHelper
        if workspace.cloud == "AWS":
            node_type_id = "i3.xlarge"
        elif workspace.cloud == "MSA":
            node_type_id = "Standard_DS3_v2"
        elif workspace.cloud == "GCP":
            node_type_id = "n1-standard-4"
        else:
            raise Exception(f"The cloud {workspace.cloud} is not supported.")
        spark_version = "11.3.x-cpu-ml-scala2.12"

        import re
        workspace_hostname = re.match("https://([^/]+)/api/", workspace.url)[1]

        # Spec for the job to run
        job_spec = {
            "name": "DBAcademy Workspace-Setup",
            "timeout_seconds": 60 * 60 * 6,  # 6 hours
            "max_concurrent_runs": 1,
            "tasks": [{
                "task_key": "Workspace-Setup",
                "notebook_task": {
                    "notebook_path": "Workspace-Setup",
                    "base_parameters": {
                        WorkspaceHelper.PARAM_LAB_ID: "Unknown",
                        WorkspaceHelper.PARAM_DESCRIPTION: "Unknown",
                        WorkspaceHelper.PARAM_CONFIGURE_FOR: WorkspaceHelper.CONFIGURE_FOR_ALL_USERS,
                        WorkspaceHelper.PARAM_NODE_TYPE_ID: node_type_id,
                        WorkspaceHelper.PARAM_SPARK_VERSION: spark_version,
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
                "git_branch": "published"
            },
            "format": "MULTI_TASK"
        }

        # Append cloud specific attributes the job_clusters spec.
        cloud_attributes = Commands._cloud_specific_attributes(workspace)
        job_spec["job_clusters"][0]["new_cluster"].update(cloud_attributes)

        job = workspace.jobs.get(job_spec["name"], if_not_exists="ignore") or {}
        if job:
            job_id = job["job_id"]
            workspace.jobs.runs.delete_all(job_id)
            job_spec["job_id"] = job_id
            workspace.jobs.update(job_spec)
        else:
            job_id = workspace.jobs.create_multi_task_job(**job_spec)

        runs = workspace.jobs.runs.list(job_id=job_id, active_only=True)
        if runs:
            run_id = runs[0]["run_id"]
        else:
            response = workspace.api("POST", "/2.1/jobs/run-now", {"job_id": job_id})
            run_id = response["run_id"]

        # Poll for job completion
        from time import sleep
        while True:
            response = workspace.api("GET", f"/2.1/jobs/runs/get?run_id={run_id}")
            if response["state"]["life_cycle_state"] not in ["PENDING", "RUNNING", "TERMINATING"]:
                result = response["state"]["result_state"]
                return result
            sleep(60)  # seconds

    def workspace_setup(self, workspace: DatabricksApi):
        import requests as web
        from dbacademy.dbhelper import WorkspaceHelper

        courseware_url = self.courseware_spec["repo"]
        response = web.get(courseware_url + "/blob/published/Includes/Workspace-Setup.py")
        if response.status_code != 200:  # If file exists
            return "No Workspace-Setup found."

        import re
        workspace_hostname = re.match("https://([^/]+)/api/", workspace.url)[1]

        # Spec for the job to run
        if self.all_users:
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
                        WorkspaceHelper.PARAM_LAB_ID: self.event["name"],
                        WorkspaceHelper.PARAM_DESCRIPTION: self.event["description"],
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
                    "spark_version": self.cluster_spec["spark_version"],
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode"
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                        "dbacademy.event_name": self.event.get("name", "unknown"),
                        "dbacademy.event_description": self.event.get("description", "unknown"),
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
        cloud_attributes = Commands._cloud_specific_attributes(workspace)
        job_spec["job_clusters"][0]["new_cluster"].update(cloud_attributes)

        job = workspace.jobs.get(job_spec["name"], if_not_exists="ignore") or {}
        if job:
            job_id = job["job_id"]
            workspace.jobs.runs.delete_all(job_id)
            job_spec["job_id"] = job_id
            workspace.jobs.update(job_spec)
        else:
            job_id = workspace.jobs.create_multi_task_job(**job_spec)

        runs = workspace.jobs.runs.list(job_id=job_id, active_only=True)
        if runs:
            run_id = runs[0]["run_id"]
        else:
            response = workspace.api("POST", "/2.1/jobs/run-now", {"job_id": job_id})
            run_id = response["run_id"]

        # Poll for job completion
        from time import sleep
        while True:
            response = workspace.api("GET", f"/2.1/jobs/runs/get?run_id={run_id}")
            if response["state"]["life_cycle_state"] not in ["PENDING", "RUNNING", "TERMINATING"]:
                result = response["state"]["result_state"]
                return result
            sleep(60)  # seconds

    def policies_create(self, workspace):
        import re
        workspace_hostname = re.match(r"^https://([^/]+)/.*$", workspace.url)[1]
        machine_type = self.cluster_spec["node_type_id"]
        autotermination = self.cluster_spec["autotermination_minutes"]
        spark_version = self.cluster_spec["spark_version"]

        tags = {
            "dbacademy.event_name": self.event.get("name", "unknown"),
            "dbacademy.event_description": self.event.get("description", "unknown"),
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

        instance_pool = workspace.pools.get_by_name(instance_pool_name, if_not_exists="ignore")
        if not instance_pool:
            instance_pool = workspace.api("POST", "2.0/instance-pools/create", instance_pool_spec)
            instance_pool_id = instance_pool["instance_pool_id"]
        else:
            instance_pool_id = instance_pool["instance_pool_id"]
        workspace.permissions.pools.update_group(instance_pool_id, "users", "CAN_ATTACH_TO")

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
        all_purpose_policy = workspace.clusters.policies.create_or_update("DBAcademy",
                                                                          all_purpose_policy)
        all_purpose_policy_id = all_purpose_policy.get("policy_id")
        workspace.permissions.clusters.policies.update(all_purpose_policy_id, "group_name", "users", "CAN_USE")

        jobs_policy: Dict[str, Any] = {
            "cluster_type": {
                "type": "fixed",
                "value": "job"
            },
        }
        jobs_policy.update(cluster_policy)
        jobs_policy = workspace.clusters.policies.create_or_update("DBAcademy Jobs", jobs_policy)
        jobs_policy_id = jobs_policy.get("policy_id")
        workspace.permissions.clusters.policies.update(jobs_policy_id, "group_name", "users", "CAN_USE")
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
        dlt_policy = workspace.clusters.policies.create_or_update("DBAcademy DLT", dlt_policy)
        dlt_policy_id = dlt_policy.get("policy_id")
        workspace.permissions.clusters.policies.update(dlt_policy_id, "group_name", "users", "CAN_USE")

    @staticmethod
    def single_user_clusters(ws):
        def get_owners(cluster):
            cluster_id = cluster["cluster_id"]
            acl = ws.permissions.clusters.get(cluster_id).get("access_control_list", [])
            owners = [perm["user_name"] for perm in acl if
                      "user_name" in perm and
                      perm["all_permissions"][0]["inherited"] is False and
                      perm["all_permissions"][0]["permission_level"] == "CAN_MANAGE" and
                      True
                      ]
            return owners

        def update_cluster(cluster):
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
        return results


# noinspection PyPep8Naming
def getWorkspace(workspaces, *, name=None, url=None):
    if name:
        return next(w for w in workspaces if w["workspace_name"] == name)
    elif url:
        return next(w for w in workspaces if url in w.url)
    else:
        raise Exception("getWorkspace: must provide workspace name or url")


# TODO Remove unused locals
# noinspection PyPep8Naming,PyUnusedLocal
def scanWorkspaces(function, workspaces, *, url=None, name=None, ignoreConnectionErrors=False):
    from requests.exceptions import ConnectionError, HTTPError
    from collections.abc import Mapping, Iterable
    from itertools import chain
    from collections import OrderedDict
    from pyspark.sql import Row
    if name:
        workspaces = (w for w in workspaces if w["workspace_name"] == name)
    elif url:
        workspaces = (w for w in workspaces if url in w.url)

    # noinspection PyPep8Naming
    def checkWorkspace(w):
        try:
            result = function(w)
            # Standardize results as an iterator of OrderedDict.
            if not result:
                result = ()
            elif not isinstance(result, Iterable) or isinstance(result, str) or isinstance(result, Mapping):
                result = (result,)
            for r in result:
                if not isinstance(r, Mapping):
                    r = {"result": r}
                yield w, r, None
        except DatabricksApiException as e:
            if not (e.http_code == 401 and ("tfclass" in w.url)):
                yield w, None, e
        except ConnectionError as e:
            if not ignoreConnectionErrors:
                yield w, None, e
        except HTTPError as e:
            yield w, None, e

    #     except Exception as e:
    #       yield (w, None, e)

    from multiprocessing.pool import ThreadPool
    with ThreadPool(500) as pool:
        map_results = pool.map(lambda w: list(checkWorkspace(w)), workspaces)

    # Determine the schema for the results and turn it into a pretty dataframe cs
    has_exceptions = False
    has_results = False
    example = OrderedDict()
    map_results = [r for w in map_results for r in w]
    for r in map_results:
        if r[1]:
            has_results = True
            example.update(r[1])
        else:
            has_exceptions = True
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
