__al__ = ["TestClusters"]

import time, json, unittest
from dbacademy.common import Cloud
from dbacademy.clients.dbrest import from_token
from dbacademy.clients.dbrest.clusters_api.cluster_config import ClusterConfig, Availability
from dbacademy_test.clients.dbrest import DBACADEMY_UNIT_TESTS, UNIT_TEST_SERVICE_PRINCIPLE


class TestClusters(unittest.TestCase):

    def setUp(self) -> None:
        self.__client = from_token(scope=DBACADEMY_UNIT_TESTS)
        self.tearDown()

    def tearDown(self) -> None:
        clusters = self.client.clusters.list()
        for cluster in clusters:
            cluster_id = cluster.get("cluster_id")
            self.client.clusters.destroy_by_id(cluster_id)

    @property
    def client(self):
        return self.__client

    def test_cluster_lifecycles(self):

        cluster_id_1 = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="Life Cycle #1",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10))

        cluster_id_2 = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="Life Cycle #2",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10))

        cluster_id_3 = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="Life Cycle #2",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10))

        self.client.clusters.terminate_by_id(cluster_id_1)
        self.client.clusters.terminate_by_id(cluster_id_2)
        self.client.clusters.terminate_by_id(cluster_id_3)

        clusters = self.client.clusters.list()
        self.assertEqual(3, len(clusters))
        self.assertTrue(clusters[0].get("cluster_name") in ["Life Cycle #1", "Life Cycle #2", "Life Cycle #3"])
        self.assertTrue(clusters[1].get("cluster_name") in ["Life Cycle #1", "Life Cycle #2", "Life Cycle #3"])
        self.assertTrue(clusters[2].get("cluster_name") in ["Life Cycle #1", "Life Cycle #2", "Life Cycle #3"])

        self.client.clusters.destroy_by_id(cluster_id_1)
        self.client.clusters.destroy_by_id(cluster_id_2)
        self.client.clusters.destroy_by_id(cluster_id_3)

    def test_default_cluster_for_single_user(self):

        cluster_id = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="Default Cluster",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10,
            single_user_name=UNIT_TEST_SERVICE_PRINCIPLE))
        cluster = self.client.clusters.get_by_id(cluster_id)

        ignored = ["last_activity_time", "last_restarted_time", "last_state_loss_time", "start_time", "aws_attributes", "state_message"]

        expected = {
            "autotermination_minutes": 10,
            "cluster_id": cluster_id,
            "cluster_name": "Default Cluster",
            "cluster_source": "API",
            "creator_user_name": UNIT_TEST_SERVICE_PRINCIPLE,
            "custom_tags": {
                "ResourceClass": "SingleNode"
            },
            "data_security_mode": "SINGLE_USER",
            "default_tags": {
                "Vendor": "Databricks",
                "Creator": UNIT_TEST_SERVICE_PRINCIPLE,
                "ClusterName": "Default Cluster",
                "ClusterId": cluster_id
            },
            "disk_spec": dict(),
            "driver_instance_source": {"node_type_id": "i3.xlarge"},
            "driver_node_type_id": "i3.xlarge",
            "driver_healthy": False,
            "effective_spark_version": "11.3.x-scala2.12",
            "enable_elastic_disk": False,
            "enable_local_disk_encryption": False,
            "init_scripts_safe_mode": False,
            "instance_source": {"node_type_id": "i3.xlarge"},
            "node_type_id": "i3.xlarge",
            "num_workers": 0,
            "single_user_name": UNIT_TEST_SERVICE_PRINCIPLE,
            "spark_conf": {
                "spark.master": "local[*]",
                "spark.databricks.cluster.profile": "singleNode"
            },
            "spark_version": "11.3.x-scala2.12",
            "state": "PENDING",
            "spec": {
                "cluster_name": "Default Cluster",
                "spark_version": "11.3.x-scala2.12",
                "spark_conf": {
                    "spark.master": "local[*]",
                    "spark.databricks.cluster.profile": "singleNode"
                },
                "aws_attributes": {
                    "availability": "ON_DEMAND"
                },
                "node_type_id": "i3.xlarge",
                "custom_tags": {
                    "ResourceClass": "SingleNode"
                },
                "autotermination_minutes": 10,
                "single_user_name": UNIT_TEST_SERVICE_PRINCIPLE,
                "data_security_mode": "SINGLE_USER",
                "num_workers": 0
            },
        }

        for key in ignored:
            assert key not in expected, f"Duplicate key found: {key}"

        for key in expected:
            actual_value = cluster.get(key)
            expected_value = expected.get(key)
            if expected_value != actual_value:
                self.fail(f"{key}: \"{expected_value}\" != \"{actual_value}\"")

        self.assertTrue(len(cluster.get("aws_attributes")) > 0)

        expected_keys = []
        expected_keys.extend(ignored)
        expected_keys.extend(expected.keys())
        expected_keys = sorted(expected_keys)

        actual_keys = sorted(cluster.keys())

        for key in expected_keys:
            if key not in actual_keys:
                print("-"*80)
                print(json.dumps(cluster, indent=4))
                print("-"*80)
                self.assertTrue(False, f"""Missing "{key}" in actual_keys: {actual_keys}""")

        for key in actual_keys:
            if key not in expected_keys:
                print("-"*80)
                print(json.dumps(cluster, indent=4))
                print("-"*80)
                self.assertTrue(False, f"""Missing "{key}" in expected_keys: {expected_keys}""")

        self.client.clusters.destroy_by_id(cluster_id)

    def test_default_cluster(self):

        cluster_id = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="Default Cluster",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10))
        
        cluster = self.client.clusters.get_by_id(cluster_id)

        ignored = ["last_activity_time", "last_restarted_time", "last_state_loss_time", "start_time", "aws_attributes", "state_message"]

        expected = {
            "autotermination_minutes": 10,
            "cluster_id": cluster_id,
            "cluster_name": "Default Cluster",
            "cluster_source": "API",
            "creator_user_name": UNIT_TEST_SERVICE_PRINCIPLE,
            "custom_tags": {
                "ResourceClass": "SingleNode"
            },
            "default_tags": {
                "Vendor": "Databricks",
                "Creator": UNIT_TEST_SERVICE_PRINCIPLE,
                "ClusterName": "Default Cluster",
                "ClusterId": cluster_id
            },
            "disk_spec": dict(),
            "driver_healthy": False,
            "driver_instance_source": {"node_type_id": "i3.xlarge"},
            "driver_node_type_id": "i3.xlarge",
            "effective_spark_version": "11.3.x-scala2.12",
            "enable_elastic_disk": False,
            "enable_local_disk_encryption": False,
            "init_scripts_safe_mode": False,
            "instance_source": {"node_type_id": "i3.xlarge"},
            "node_type_id": "i3.xlarge",
            "num_workers": 0,
            "spark_conf": {
                "spark.master": "local[*]",
                "spark.databricks.cluster.profile": "singleNode"
            },
            "spark_version": "11.3.x-scala2.12",
            "state": "PENDING",
            "spec": {
                "cluster_name": "Default Cluster",
                "spark_version": "11.3.x-scala2.12",
                "spark_conf": {
                    "spark.master": "local[*]",
                    "spark.databricks.cluster.profile": "singleNode"
                },
                "aws_attributes": {
                    "availability": "ON_DEMAND"
                },
                "node_type_id": "i3.xlarge",
                "custom_tags": {
                    "ResourceClass": "SingleNode"
                },
                "autotermination_minutes": 10,
                "num_workers": 0
            },
        }

        for key in ignored:
            assert key not in expected, f"Duplicate key found: {key}"

        for key in expected:
            actual_value = cluster.get(key)
            expected_value = expected.get(key)
            if expected_value != actual_value:
                self.fail(f"""{key}: "{expected_value}" != "{actual_value}".""")

        self.assertTrue(len(cluster.get("aws_attributes")) > 0)

        expected_keys = []
        expected_keys.extend(ignored)
        expected_keys.extend(expected.keys())
        expected_keys = sorted(expected_keys)

        actual_keys = sorted(cluster.keys())

        for key in expected_keys:
            if key not in actual_keys:
                print("-"*80)
                print(json.dumps(cluster, indent=4))
                print("-"*80)
                self.assertTrue(False, f"""Missing "{key}" in actual_keys: {actual_keys}""")

        for key in actual_keys:
            if key not in expected_keys:
                print("-"*80)
                print(json.dumps(cluster, indent=4))
                print("-"*80)
                self.assertTrue(False, f"""Missing "{key}" in expected_keys: {expected_keys}""")

        self.client.clusters.destroy_by_id(cluster_id)

    def test_auto_terminate(self):

        cluster_id = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="Auto Terminate 10",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10))
        cluster = self.client.clusters.get_by_id(cluster_id)
        self.assertEqual(10, cluster.get("autotermination_minutes"))
        self.client.clusters.destroy_by_id(cluster_id)

        cluster_id = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="Auto Terminate 10",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=23))
        cluster = self.client.clusters.get_by_id(cluster_id)
        self.assertEqual(23, cluster.get("autotermination_minutes"))
        self.client.clusters.destroy_by_id(cluster_id)

    def test_create_multi_node(self):

        cluster_id = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="Single-Node",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10))
        cluster = self.client.clusters.get_by_id(cluster_id)
        self.assertEqual(0, cluster.get("num_workers"))
        self.assertEqual("local[*]", cluster.get("spark_conf").get("spark.master"))
        self.client.clusters.destroy_by_id(cluster_id)

        cluster_id = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="Multi-Node",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=3,
            autotermination_minutes=10))
        cluster = self.client.clusters.get_by_id(cluster_id)
        self.assertEqual(3, cluster.get("num_workers"))
        self.assertEqual(None, cluster.get("spark_conf", {}).get("spark.master"))
        self.client.clusters.destroy_by_id(cluster_id)

    def test_create_single_user(self):

        cluster_id = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="Single User A",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10,
            # single_user_name=unit_test_service_principle
        ))
        cluster = self.client.clusters.get_by_id(cluster_id)
        self.assertIsNone(cluster.get("single_user_name"))
        self.assertIsNone(cluster.get("data_security_mode"))
        self.client.clusters.destroy_by_id(cluster_id)

        cluster_id = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="Single User B",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10,
            single_user_name=UNIT_TEST_SERVICE_PRINCIPLE))
        cluster = self.client.clusters.get_by_id(cluster_id)

        self.assertEqual(UNIT_TEST_SERVICE_PRINCIPLE, cluster.get("single_user_name"))
        self.assertEqual("SINGLE_USER", cluster.get("data_security_mode"))

        self.assertEqual(0, cluster.get("num_workers"))
        self.assertEqual("Single User B", cluster.get("cluster_name"))
        self.assertEqual("11.3.x-scala2.12", cluster.get("spark_version"))

        self.assertEqual(2, len(cluster.get("spark_conf")))
        self.assertEqual("local[*]", cluster.get("spark_conf").get("spark.master"))
        self.assertEqual("singleNode", cluster.get("spark_conf").get("spark.databricks.cluster.profile"))

        self.assertTrue(len(cluster.get("aws_attributes")) > 0)

        self.assertEqual("i3.xlarge", cluster.get("node_type_id"))
        self.assertEqual("i3.xlarge", cluster.get("driver_node_type_id"))

        self.assertEqual(10, cluster.get("autotermination_minutes"))
        self.assertEqual(False, cluster.get("enable_elastic_disk"))
        self.assertEqual("API", cluster.get("cluster_source"))
        self.assertEqual(cluster_id, cluster.get("cluster_id"))

        self.assertEqual(None, cluster.get("ssh_public_keys"))
        self.assertEqual({"ResourceClass": "SingleNode"}, cluster.get("custom_tags"))
        self.assertEqual(None, cluster.get("spark_env_vars"))
        self.assertEqual(None, cluster.get("init_scripts"))

        self.client.clusters.destroy_by_id(cluster_id)

    def test_create_driver_node_type(self):

        cluster_id = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="Driver Node Type A",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            # Expected to be None
            # driver_node_type_id="i3.2xlarge",
            num_workers=0,
            autotermination_minutes=10))
        cluster = self.client.clusters.get_by_id(cluster_id)
        self.assertEqual("i3.xlarge", cluster.get("node_type_id"))
        self.assertEqual("i3.xlarge", cluster.get("driver_node_type_id"))
        self.client.clusters.destroy_by_id(cluster_id)

    def test_create_with_spark_conf(self):

        cluster_id = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="AWS Attributes",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10,
            spark_conf={
                "dbacademy.whatever": "enabled"
            }))
        cluster = self.client.clusters.get_by_id(cluster_id)
        self.assertEqual("enabled", cluster.get("spark_conf").get("dbacademy.whatever"))

    def test_create_with_aws_attributes_availability(self):

        try:
            ClusterConfig(cloud=Cloud.AWS,
                          cluster_name="AWS Attributes SPOT",
                          spark_version="11.3.x-scala2.12",
                          node_type_id="i3.xlarge",
                          num_workers=0,
                          autotermination_minutes=10,
                          extra_params={
                              "aws_attributes": {
                                "availability": "SPOT"
                              }
                          })
        except AssertionError as e:
            self.assertEqual(f"The parameter \"aws_attributes.availability\" should not be specified directly, use \"availability\" instead.", str(e))

    def test_create_with_availability_on_demand_and_instance_profile(self):

        try:
            ClusterConfig(cloud=Cloud.AWS,
                          cluster_name="AWS Attributes Conflicting",
                          spark_version="11.3.x-scala2.12",
                          node_type_id="i3.xlarge",
                          num_workers=0,
                          autotermination_minutes=10,
                          availability=Availability.SPOT,
                          instance_pool_id="0123456789")
        except AssertionError as e:
            self.assertEqual(f"The parameter \"availability\" cannot be specified when \"instance_pool_id\" is specified.", str(e))

    def test_create_with_availability_default(self):

        config = ClusterConfig(cloud=Cloud.AWS,
                               cluster_name="AWS Attributes Default",
                               spark_version="11.3.x-scala2.12",
                               node_type_id="i3.xlarge",
                               num_workers=0,
                               autotermination_minutes=10)
        cluster_id = self.client.clusters.create_from_config(config)
        cluster = self.client.clusters.get_by_id(cluster_id)
        self.assertEqual(Availability.ON_DEMAND.value, cluster.get("aws_attributes").get("availability"))

    def test_create_with_availability_ON_DEMAND(self):

        config = ClusterConfig(cloud=Cloud.AWS,
                               cluster_name="AWS Attributes SPOT",
                               spark_version="11.3.x-scala2.12",
                               node_type_id="i3.xlarge",
                               num_workers=0,
                               autotermination_minutes=10,
                               availability=Availability.ON_DEMAND)

        self.assertEqual(Availability.ON_DEMAND.value, config.params.get("aws_attributes").get("availability"))
        self.assertIsNone(config.params.get("azure_attributes"))
        self.assertIsNone(config.params.get("gcp_attributes"))

        config = ClusterConfig(cloud=Cloud.MSA,
                               cluster_name="MSA Attributes SPOT",
                               spark_version="11.3.x-scala2.12",
                               node_type_id="i3.xlarge",
                               num_workers=0,
                               autotermination_minutes=10,
                               availability=Availability.ON_DEMAND)

        self.assertIsNone(config.params.get("aws_attributes"))
        self.assertEqual("ON_DEMAND_AZURE", config.params.get("azure_attributes").get("availability"))
        self.assertIsNone(config.params.get("gcp_attributes"))

        config = ClusterConfig(cloud=Cloud.GCP,
                               cluster_name="GCP Attributes SPOT",
                               spark_version="11.3.x-scala2.12",
                               node_type_id="i3.xlarge",
                               num_workers=0,
                               autotermination_minutes=10,
                               availability=Availability.ON_DEMAND)

        self.assertIsNone(config.params.get("aws_attributes"))
        self.assertIsNone(config.params.get("azure_attributes"))
        self.assertEqual("ON_DEMAND_GCP", config.params.get("gcp_attributes").get("availability"))

    def test_create_with_availability_SPOT(self):

        config = ClusterConfig(cloud=Cloud.AWS,
                               cluster_name="AWS Attributes SPOT",
                               spark_version="11.3.x-scala2.12",
                               node_type_id="i3.xlarge",
                               num_workers=0,
                               autotermination_minutes=10,
                               availability=Availability.SPOT)

        self.assertEqual(Availability.SPOT.value, config.params.get("aws_attributes").get("availability"))
        self.assertIsNone(config.params.get("azure_attributes"))
        self.assertIsNone(config.params.get("gcp_attributes"))

        config = ClusterConfig(cloud=Cloud.MSA,
                               cluster_name="AWS Attributes SPOT",
                               spark_version="11.3.x-scala2.12",
                               node_type_id="i3.xlarge",
                               num_workers=0,
                               autotermination_minutes=10,
                               availability=Availability.SPOT)

        self.assertIsNone(config.params.get("aws_attributes"))
        self.assertEqual("SPOT_WITH_FALLBACK_AZURE", config.params.get("azure_attributes").get("availability"))
        self.assertIsNone(config.params.get("gcp_attributes"))

        config = ClusterConfig(cloud=Cloud.GCP,
                               cluster_name="AWS Attributes SPOT",
                               spark_version="11.3.x-scala2.12",
                               node_type_id="i3.xlarge",
                               num_workers=0,
                               autotermination_minutes=10,
                               availability=Availability.SPOT)

        self.assertIsNone(config.params.get("aws_attributes"))
        self.assertIsNone(config.params.get("azure_attributes"))
        self.assertEqual("PREEMPTIBLE_WITH_FALLBACK_GCP", config.params.get("gcp_attributes").get("availability"))

    def test_create_with_availability_SPOT_WITH_FALLBACK(self):

        config = ClusterConfig(cloud=Cloud.AWS,
                               cluster_name="AWS Attributes SPOT",
                               spark_version="11.3.x-scala2.12",
                               node_type_id="i3.xlarge",
                               num_workers=0,
                               autotermination_minutes=10,
                               availability=Availability.SPOT_WITH_FALLBACK)

        self.assertEqual(Availability.SPOT_WITH_FALLBACK.value, config.params.get("aws_attributes").get("availability"))
        self.assertIsNone(config.params.get("azure_attributes"))
        self.assertIsNone(config.params.get("gcp_attributes"))

        config = ClusterConfig(cloud=Cloud.MSA,
                               cluster_name="AWS Attributes SPOT",
                               spark_version="11.3.x-scala2.12",
                               node_type_id="i3.xlarge",
                               num_workers=0,
                               autotermination_minutes=10,
                               availability=Availability.SPOT_WITH_FALLBACK)

        self.assertIsNone(config.params.get("aws_attributes"))
        self.assertEqual("SPOT_WITH_FALLBACK_AZURE", config.params.get("azure_attributes").get("availability"))
        self.assertIsNone(config.params.get("gcp_attributes"))

        config = ClusterConfig(cloud=Cloud.GCP,
                               cluster_name="AWS Attributes SPOT",
                               spark_version="11.3.x-scala2.12",
                               node_type_id="i3.xlarge",
                               num_workers=0,
                               autotermination_minutes=10,
                               availability=Availability.SPOT_WITH_FALLBACK)

        self.assertIsNone(config.params.get("aws_attributes"))
        self.assertIsNone(config.params.get("azure_attributes"))
        self.assertEqual("PREEMPTIBLE_WITH_FALLBACK_GCP", config.params.get("gcp_attributes").get("availability"))

    def test_create_with_extra_params(self):

        cluster_id = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name="Extra Params",
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10,
            custom_tags={"dbacademy.smoke_tests": "true"}))
        cluster = self.client.clusters.get_by_id(cluster_id)
        self.assertEqual("true", cluster.get("custom_tags").get("dbacademy.smoke_tests"))

    def test_no_cluster(self):
        from dbacademy.clients.rest.common import DatabricksApiException

        self.assertIsNone(self.client.clusters.get_by_id("0000000000"))
        self.assertIsNone(self.client.clusters.get_by_name("John Doe"))

        self.assertIsNone(self.client.clusters.destroy_by_id("0000000000"))
        self.assertIsNone(self.client.clusters.destroy_by_name("John Doe"))

        try:
            self.assertIsNone(self.client.clusters.terminate_by_id("0000000000"))
            raise Exception("Expected DatabricksApiException")
        except DatabricksApiException as e:
            self.assertEqual(400, e.http_code)

        try:
            self.assertIsNone(self.client.clusters.terminate_by_name("John Doe"))
            raise Exception("Expected DatabricksApiException")
        except DatabricksApiException as e:
            self.assertEqual(400, e.http_code)

    def test_terminate_by_name(self):

        cluster_name = "Destroy By Name"
        self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name=cluster_name,
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10))

        cluster = self.client.clusters.get_by_name(cluster_name)
        self.assertIsNotNone(cluster)
        self.assertEqual("PENDING", cluster.get("state"))

        self.client.clusters.terminate_by_name(cluster_name)

        state = None
        for i in range(30):
            cluster = self.client.clusters.get_by_name(cluster_name)
            self.assertIsNotNone(cluster)

            state = cluster.get("state")
            if cluster.get("state") not in ["TERMINATED", "TERMINATING"]:
                time.sleep(1)  # Give it time to transition

        self.assertTrue(state in ["TERMINATED", "TERMINATING"], f"Found \"{state}\"")

    def test_destroy_by_name(self):

        cluster_name = "Destroy By Name"
        self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name=cluster_name,
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10))
        self.client.clusters.destroy_by_name(cluster_name)

        cluster = self.client.clusters.get_by_name(cluster_name)
        self.assertIsNone(cluster)

    def test_get_by_name(self):

        # Create and then get by name
        cluster_name = "Cluster By Name"
        cluster_id = self.client.clusters.create_from_config(ClusterConfig(
            cloud=Cloud.AWS,
            cluster_name=cluster_name,
            spark_version="11.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=0,
            autotermination_minutes=10))

        cluster_a = self.client.clusters.get_by_id(cluster_id)
        cluster_b = self.client.clusters.get_by_name(cluster_name)
        for c in [cluster_a, cluster_b]:
            if "state_message" in c:
                del c["state_message"]

        self.maxDiff = None
        if cluster_a != cluster_b:
            print("-"*80)
            print(json.dumps(cluster_a, indent=4))
            print("-"*80)
            print(json.dumps(cluster_b, indent=4))
            print("-"*80)
            self.assertEqual(cluster_a, cluster_b)

    # def test_get_deprecated(self):
    #
    #     cluster_id = self.client.clusters.create_from_config(ClusterConfig(
    #         cloud=Cloud.AWS,
    #         cluster_name="Deprecated Get",
    #         spark_version="11.3.x-scala2.12",
    #         node_type_id="i3.xlarge",
    #         num_workers=0,
    #         autotermination_minutes=10))
    #     try:
    #         self.client.clusters.get_by_id(cluster_id)
    #         raise Exception("Expected DeprecationWarning")
    #     except DeprecationWarning as e:
    #         self.assertEqual("dbacademy.clients.databricks.clusters.cluster_client_class.get(self, cluster_id): Use ClustersClient.get_by_id() or ClustersClient.get_by_name() instead", str(e))

    def test_list_node_types(self):
        types = self.client.clusters.list_node_types()

        self.assertIsNotNone(types)
        self.assertTrue(list, type(types))
        self.assertTrue(len(types) > 100, f"Expected at least 100 node types, found {len(types)}")

        note_type_ids = [t.get("node_type_id") for t in types]
        self.assertTrue("i3.xlarge" in note_type_ids)


if __name__ == "__main__":
    unittest.main()
